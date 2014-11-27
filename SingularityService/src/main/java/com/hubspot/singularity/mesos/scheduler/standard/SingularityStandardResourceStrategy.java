package com.hubspot.singularity.mesos.scheduler.standard;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hubspot.mesos.JavaUtils;
import com.hubspot.mesos.MesosUtils;
import com.hubspot.mesos.SingularityResourceRequest;
import com.hubspot.singularity.SingularityTask;
import com.hubspot.singularity.SingularityTaskRequest;
import com.hubspot.singularity.config.MesosConfiguration;
import com.hubspot.singularity.data.TaskManager;
import com.hubspot.singularity.mesos.SchedulerDriverSupplier;
import com.hubspot.singularity.mesos.SingularityMesosTaskBuilder;
import com.hubspot.singularity.mesos.SingularityOfferHolder;
import com.hubspot.singularity.mesos.SingularitySlaveAndRackManager;
import com.hubspot.singularity.mesos.SingularitySlaveAndRackManager.SlaveMatchState;
import com.hubspot.singularity.mesos.scheduler.ResourceStrategy;
import com.hubspot.singularity.scheduler.SingularityScheduler;
import com.hubspot.singularity.scheduler.SingularitySchedulerPriority;
import com.hubspot.singularity.scheduler.SingularitySchedulerStateCache;

@Singleton
public class SingularityStandardResourceStrategy implements ResourceStrategy
{
  private static final Logger LOG = LoggerFactory.getLogger(SingularityStandardResourceStrategy.class);

  private final SchedulerDriverSupplier schedulerDriverSupplier;
  private final SingularityScheduler scheduler;
  private final SingularitySlaveAndRackManager slaveAndRackManager;
  private final SingularitySchedulerPriority schedulerPriority;
  private final SingularityMesosTaskBuilder mesosTaskBuilder;
  private final TaskManager taskManager;

  private final List<SingularityResourceRequest> defaultResources;

  @Inject
  SingularityStandardResourceStrategy(MesosConfiguration mesosConfiguration,
      final SchedulerDriverSupplier schedulerDriverSupplier,
      final SingularityScheduler scheduler,
      final SingularitySlaveAndRackManager slaveAndRackManager,
      final SingularitySchedulerPriority schedulerPriority,
      final TaskManager taskManager,
      final SingularityMesosTaskBuilder mesosTaskBuilder)
  {
    checkNotNull(mesosConfiguration, "mesosConfiguration is null");

    this.schedulerDriverSupplier = checkNotNull(schedulerDriverSupplier, "schedulerDriverSupplier is null");
    this.scheduler = checkNotNull(scheduler, "scheduler is null");
    this.slaveAndRackManager = checkNotNull(slaveAndRackManager, "slaveAndRackManager is null");
    this.schedulerPriority = checkNotNull(schedulerPriority, "schedulerPriority is null");
    this.taskManager = checkNotNull(taskManager, "taskManager is null");
    this.mesosTaskBuilder = checkNotNull(mesosTaskBuilder, "mesosTaskBuilder is null");

    this.defaultResources = mesosConfiguration.getDefaultResources();
  }

  @Override
  public void processOffers(List<Protos.Offer> offers, SingularitySchedulerStateCache stateCache)
  {
    final Optional<SchedulerDriver> maybeDriver = schedulerDriverSupplier.get();

    if (!maybeDriver.isPresent()) {
      LOG.warn("Received offers but driver is not present!");
    }

    final SchedulerDriver driver = maybeDriver.get();

    final long start = System.currentTimeMillis();

    final Set<Protos.OfferID> acceptedOffers = Sets.newHashSetWithExpectedSize(offers.size());

    for (Protos.Offer offer : offers) {
      slaveAndRackManager.checkOffer(offer);
    }

    int numDueTasks = 0;

    try {
      final List<SingularityTaskRequest> taskRequests = scheduler.getDueTasks();
      schedulerPriority.sortTaskRequestsInPriorityOrder(taskRequests);

      for (SingularityTaskRequest taskRequest : taskRequests) {
        LOG.trace("Task {} is due", taskRequest.getPendingTask().getPendingTaskId());
      }

      numDueTasks = taskRequests.size();

      final List<SingularityOfferHolder> offerHolders = Lists.newArrayListWithCapacity(offers.size());

      for (Protos.Offer offer : offers) {
        offerHolders.add(new SingularityOfferHolder(offer, numDueTasks));
      }

      boolean addedTaskInLastLoop = true;

      while (!taskRequests.isEmpty() && addedTaskInLastLoop) {
        addedTaskInLastLoop = false;
        Collections.shuffle(offerHolders);

        for (SingularityOfferHolder offerHolder : offerHolders) {
          Optional<SingularityTask> accepted = match(taskRequests, stateCache, offerHolder);
          if (accepted.isPresent()) {
            offerHolder.addMatchedTask(accepted.get());
            addedTaskInLastLoop = true;
            taskRequests.remove(accepted.get().getTaskRequest());
          }

          if (taskRequests.isEmpty()) {
            break;
          }
        }
      }

      for (SingularityOfferHolder offerHolder : offerHolders) {
        if (!offerHolder.getAcceptedTasks().isEmpty()) {
          offerHolder.launchTasks(driver);

          acceptedOffers.add(offerHolder.getOffer().getId());
        } else {
          driver.declineOffer(offerHolder.getOffer().getId());
        }
      }

    } catch (Throwable t) {
      LOG.error("Received fatal error while accepting offers - will decline all available offers", t);

      for (Protos.Offer offer : offers) {
        if (acceptedOffers.contains(offer.getId())) {
          continue;
        }

        driver.declineOffer(offer.getId());
      }

      throw t;
    }

    LOG.info("Finished handling {} offer(s) ({}), {} accepted, {} declined, {} outstanding tasks", offers.size(), JavaUtils.duration(start), acceptedOffers.size(),
        offers.size() - acceptedOffers.size(), numDueTasks - acceptedOffers.size());
  }

  private Optional<SingularityTask> match(Collection<SingularityTaskRequest> taskRequests, SingularitySchedulerStateCache stateCache, SingularityOfferHolder offerHolder) {

    for (SingularityTaskRequest taskRequest : taskRequests) {
      Optional<Map<String, String>> taskAttributes = taskRequest.getDeploy().getRequestedAttributes();

      List<SingularityResourceRequest> taskResources = taskRequest.getDeploy().getResourceRequestList().or(defaultResources);

      LOG.trace("Attempting to match task {} resources {} with remaining offer resources {}", taskRequest.getPendingTask().getPendingTaskId(), taskResources, offerHolder.getCurrentResources());

      final boolean matchAttributes = MesosUtils.doesOfferMatchAttributes(taskAttributes, offerHolder.getOffer().getAttributesList());
      final boolean matchesResources = MesosUtils.doesOfferMatchResources(taskResources, offerHolder.getCurrentResources());
      final SlaveMatchState slaveMatchState = slaveAndRackManager.doesOfferMatch(offerHolder.getOffer(), taskRequest, stateCache);

      if (matchAttributes && matchesResources && slaveMatchState.isMatchAllowed()) {
        final SingularityTask task = mesosTaskBuilder.buildTask(offerHolder.getOffer(), offerHolder.getCurrentResources(), taskRequest, taskResources);

        LOG.trace("Accepted and built task {}", task);

        LOG.info("Launching task {} slot on slave {} ({})", task.getTaskId(), offerHolder.getOffer().getSlaveId().getValue(), offerHolder.getOffer().getHostname());

        taskManager.createTaskAndDeletePendingTask(task);

        schedulerPriority.notifyTaskLaunched(task.getTaskId());

        stateCache.getActiveTaskIds().add(task.getTaskId());
        stateCache.getScheduledTasks().remove(taskRequest.getPendingTask());

        return Optional.of(task);
      } else {
        LOG.trace("Ignoring offer {} on {} for task {}; matched attributes: {}, matched resources: {}, slave match state: {}",
            offerHolder.getOffer().getId(), offerHolder.getOffer().getHostname(), taskRequest.getPendingTask().getPendingTaskId(),
            matchAttributes, matchesResources, slaveMatchState);
      }
    }

    return Optional.absent();
  }
}
