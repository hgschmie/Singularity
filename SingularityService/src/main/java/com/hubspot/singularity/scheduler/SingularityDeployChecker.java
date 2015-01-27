package com.hubspot.singularity.scheduler;

import static java.lang.String.format;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.hubspot.mesos.JavaUtils;
import com.hubspot.singularity.DeployState;
import com.hubspot.singularity.RequestState;
import com.hubspot.singularity.SingularityDeleteResult;
import com.hubspot.singularity.SingularityDeploy;
import com.hubspot.singularity.SingularityDeployKey;
import com.hubspot.singularity.SingularityDeployMarker;
import com.hubspot.singularity.SingularityDeployResult;
import com.hubspot.singularity.SingularityPendingDeploy;
import com.hubspot.singularity.SingularityPendingRequest;
import com.hubspot.singularity.SingularityPendingRequest.PendingType;
import com.hubspot.singularity.SingularityPendingTaskId;
import com.hubspot.singularity.SingularityRequest;
import com.hubspot.singularity.SingularityRequestDeployState;
import com.hubspot.singularity.SingularityRequestHistory.RequestHistoryType;
import com.hubspot.singularity.SingularityRequestWithState;
import com.hubspot.singularity.SingularityTaskCleanup;
import com.hubspot.singularity.SingularityTaskCleanup.TaskCleanupType;
import com.hubspot.singularity.SingularityTaskId;
import com.hubspot.singularity.config.SingularityConfiguration;
import com.hubspot.singularity.data.DeployManager;
import com.hubspot.singularity.data.RequestManager;
import com.hubspot.singularity.data.TaskManager;
import com.hubspot.singularity.scheduler.SingularityDeployHealthHelper.DeployHealth;

@Singleton
public class SingularityDeployChecker {

  private static final Logger LOG = LoggerFactory.getLogger(SingularityDeployChecker.class);

  private final DeployManager deployManager;
  private final TaskManager taskManager;
  private final SingularityDeployHealthHelper deployHealthHelper;
  private final RequestManager requestManager;
  private final SingularityConfiguration configuration;

  @Inject
  public SingularityDeployChecker(DeployManager deployManager, SingularityDeployHealthHelper deployHealthHelper,RequestManager requestManager, TaskManager taskManager, SingularityConfiguration configuration) {
    this.configuration = configuration;
    this.deployHealthHelper = deployHealthHelper;
    this.requestManager = requestManager;
    this.deployManager = deployManager;
    this.taskManager = taskManager;
  }

  public int checkDeploys() {
    final List<SingularityPendingDeploy> pendingDeploys = deployManager.getPendingDeploys();
    final List<SingularityDeployMarker> cancelDeploys = deployManager.getCancelDeploys();

    if (pendingDeploys.isEmpty() && cancelDeploys.isEmpty()) {
      return 0;
    }

    final Map<SingularityPendingDeploy, SingularityDeployKey> pendingDeployToKey = SingularityDeployKey.fromPendingDeploys(pendingDeploys);
    final Map<SingularityDeployKey, SingularityDeploy> deployKeyToDeploy = deployManager.getDeploysForKeys(pendingDeployToKey.values());

    for (SingularityPendingDeploy pendingDeploy : pendingDeploys) {
      LOG.debug("Checking a deploy {}", pendingDeploy);

      checkDeploy(pendingDeploy, cancelDeploys, pendingDeployToKey, deployKeyToDeploy);
    }

    for (SingularityDeployMarker cancelDeploy : cancelDeploys) {
      SingularityDeleteResult deleteResult = deployManager.deleteCancelDeployRequest(cancelDeploy);

      LOG.debug("Removing cancel deploy request {} - {}", cancelDeploy, deleteResult);
    }

    return pendingDeploys.size();
  }

  private void checkDeploy(final SingularityPendingDeploy pendingDeploy, final List<SingularityDeployMarker> cancelDeploys, final Map<SingularityPendingDeploy, SingularityDeployKey> pendingDeployToKey, final Map<SingularityDeployKey, SingularityDeploy> deployKeyToDeploy) {
    final SingularityDeployKey deployKey = pendingDeployToKey.get(pendingDeploy);
    final Optional<SingularityDeploy> deploy = Optional.fromNullable(deployKeyToDeploy.get(deployKey));

    Optional<SingularityRequestWithState> maybeRequestWithState = requestManager.getRequest(pendingDeploy.getDeployMarker().getRequestId());

    if (!SingularityRequestWithState.isActive(maybeRequestWithState)) {
      LOG.warn("Deploy {} request was {}, removing deploy", SingularityRequestWithState.getRequestState(maybeRequestWithState), pendingDeploy);

      removePendingDeploy(pendingDeploy);
      return;
    }

    final SingularityDeployMarker pendingDeployMarker = pendingDeploy.getDeployMarker();

    final Optional<SingularityDeployMarker> cancelRequest = findCancel(cancelDeploys, pendingDeployMarker);

    final SingularityRequestWithState requestWithState = maybeRequestWithState.get();
    final SingularityRequest request = requestWithState.getRequest();

    final List<SingularityTaskId> requestTasks = taskManager.getTaskIdsForRequest(request.getId());
    final List<SingularityTaskId> activeTasks = taskManager.filterActiveTaskIds(requestTasks);

    final List<SingularityTaskId> inactiveDeployMatchingTasks = SingularityTaskId.matchingAndNotIn(requestTasks, pendingDeployMarker.getRequestId(), pendingDeployMarker.getDeployId(), activeTasks);
    final List<SingularityTaskId> deployMatchingTasks = Lists.newArrayList(Iterables.filter(activeTasks, SingularityTaskId.matchingDeploy(pendingDeployMarker.getDeployId())));
    final List<SingularityTaskId> allOtherMatchingTasks = Lists.newArrayList(Iterables.filter(activeTasks, Predicates.not(SingularityTaskId.matchingDeploy(pendingDeployMarker.getDeployId()))));

    SingularityDeployResult deployResult = getDeployResult(request, cancelRequest, pendingDeploy, deployKey, deploy, deployMatchingTasks, allOtherMatchingTasks, inactiveDeployMatchingTasks);

    LOG.info("Deploy {} had result {} after {}", pendingDeployMarker, deployResult, JavaUtils.durationFromMillis(System.currentTimeMillis() - pendingDeployMarker.getTimestamp()));

    if (deployResult.getDeployState() == DeployState.SUCCEEDED) {
      if (saveNewDeployState(pendingDeployMarker, Optional.of(pendingDeployMarker))) {

        deleteObsoletePendingTasks(pendingDeploy);
        finishDeploy(requestWithState, deploy, pendingDeploy, allOtherMatchingTasks, deployResult);
        return;
      } else {
        LOG.warn("Failing deploy {} because it failed to save deploy state", pendingDeployMarker);
        deployResult = new SingularityDeployResult(DeployState.FAILED_INTERNAL_STATE, Optional.of(format("Deploy had state %s but failed to persist it correctly", deployResult.getDeployState())), deployResult.getTimestamp());
      }
    } else if (!deployResult.getDeployState().isDeployFinished()) {
      return;
    }

    // success case is handled, handle failure cases:
    saveNewDeployState(pendingDeployMarker, Optional.<SingularityDeployMarker> absent());
    finishDeploy(requestWithState, deploy, pendingDeploy, deployMatchingTasks, deployResult);
  }

  private void deleteObsoletePendingTasks(SingularityPendingDeploy pendingDeploy) {
    for (SingularityPendingTaskId pendingTaskId : Iterables.filter(taskManager.getPendingTaskIds(), Predicates.and(SingularityPendingTaskId.matchingRequestId(pendingDeploy.getDeployMarker().getRequestId()), Predicates.not(SingularityPendingTaskId.matchingDeployId(pendingDeploy.getDeployMarker().getDeployId()))))) {
      LOG.debug("Deleting obsolete pending task {}", pendingTaskId.getId());
      taskManager.deletePendingTask(pendingTaskId);
    }
  }

  private Optional<SingularityDeployMarker> findCancel(List<SingularityDeployMarker> cancelDeploys, SingularityDeployMarker activeDeploy) {
    for (SingularityDeployMarker cancelDeploy : cancelDeploys) {
      if (cancelDeploy.getRequestId().equals(activeDeploy.getRequestId()) && cancelDeploy.getDeployId().equals(activeDeploy.getDeployId())) {
        return Optional.of(cancelDeploy);
      }
    }

    return Optional.absent();
  }

  private void cleanupTasks(Iterable<SingularityTaskId> tasksToKill, TaskCleanupType cleanupType, long timestamp) {
    for (SingularityTaskId matchingTask : tasksToKill) {
      taskManager.createCleanupTask(new SingularityTaskCleanup(Optional.<String> absent(), cleanupType, timestamp, matchingTask));
    }
  }

  private boolean saveNewDeployState(SingularityDeployMarker pendingDeployMarker, Optional<SingularityDeployMarker> newActiveDeploy) {
    Optional<SingularityRequestDeployState> deployState = deployManager.getRequestDeployState(pendingDeployMarker.getRequestId());

    if (!deployState.isPresent()) {
      LOG.error("Expected deploy state for deploy marker: {} but didn't find it", pendingDeployMarker);
      return false;
    }

    deployManager.saveNewRequestDeployState(new SingularityRequestDeployState(deployState.get().getRequestId(), newActiveDeploy.or(deployState.get().getActiveDeploy()),
        Optional.<SingularityDeployMarker> absent()));

    return true;
  }

  private void finishDeploy(SingularityRequestWithState requestWithState, Optional<SingularityDeploy> deploy, SingularityPendingDeploy pendingDeploy, Iterable<SingularityTaskId> tasksToKill, SingularityDeployResult deployResult) {
    SingularityRequest request = requestWithState.getRequest();

    if (!request.isOneOff()) {
      cleanupTasks(tasksToKill, deployResult.getDeployState().getCleanupType(), deployResult.getTimestamp());
    }

    if (!request.isDeployable() && !request.isOneOff()) {
      // TODO should this override? What if someone has mucked with the pending queue for this deploy ?
      requestManager.addToPendingQueue(new SingularityPendingRequest(request.getId(), pendingDeploy.getDeployMarker().getDeployId(), deployResult.getTimestamp(), PendingType.NEW_DEPLOY));
    }

    deployManager.saveDeployResult(pendingDeploy.getDeployMarker(), deploy, deployResult);

    if (requestWithState.getState() == RequestState.DEPLOYING_TO_UNPAUSE) {
      if (deployResult.getDeployState() == DeployState.SUCCEEDED) {
        requestManager.activate(request, RequestHistoryType.DEPLOYED_TO_UNPAUSE, deployResult.getTimestamp(), pendingDeploy.getDeployMarker().getUser());
      } else {
        requestManager.pause(request, deployResult.getTimestamp(), pendingDeploy.getDeployMarker().getUser());
      }
    }

    removePendingDeploy(pendingDeploy);
  }

  private void removePendingDeploy(SingularityPendingDeploy pendingDeploy) {
    deployManager.deletePendingDeploy(pendingDeploy.getDeployMarker().getRequestId());
  }

  private long getAllowedMillis(SingularityDeploy deploy) {
    long seconds = deploy.getDeployHealthTimeoutSeconds().or(configuration.getDeployHealthyBySeconds());

    if (deploy.getHealthcheckUri().isPresent() && !deploy.getSkipHealthchecksOnDeploy().or(false)) {
      seconds += deploy.getHealthcheckIntervalSeconds().or(configuration.getHealthcheckIntervalSeconds()) + deploy.getHealthcheckTimeoutSeconds().or(configuration.getHealthcheckTimeoutSeconds());
    }

    return TimeUnit.SECONDS.toMillis(seconds);
  }

  private boolean isDeployOverdue(SingularityPendingDeploy pendingDeploy, Optional<SingularityDeploy> deploy) {
    if (!deploy.isPresent()) {
      LOG.warn("Can't determine if deploy {} is overdue because it was missing", pendingDeploy);

      return false;
    }

    final long startTime = pendingDeploy.getDeployMarker().getTimestamp();

    final long deployDuration = System.currentTimeMillis() - startTime;

    final long allowedTime = getAllowedMillis(deploy.get());

    if (deployDuration > allowedTime) {
      LOG.warn("Deploy {} is overdue (duration: {}), allowed: {}", pendingDeploy, DurationFormatUtils.formatDurationHMS(deployDuration), DurationFormatUtils.formatDurationHMS(allowedTime));

      return true;
    } else {
      LOG.trace("Deploy {} is not yet overdue (duration: {}), allowed: {}", pendingDeploy, DurationFormatUtils.formatDurationHMS(deployDuration), DurationFormatUtils.formatDurationHMS(allowedTime));

      return false;
    }
  }

  private SingularityDeployResult getDeployResult(final SingularityRequest request, final Optional<SingularityDeployMarker> cancelRequest, final SingularityPendingDeploy pendingDeploy, final SingularityDeployKey deployKey,
      final Optional<SingularityDeploy> deploy, final Collection<SingularityTaskId> deployActiveTasks, final Collection<SingularityTaskId> otherActiveTasks, final Collection<SingularityTaskId> inactiveDeployMatchingTasks) {
    if (!request.isDeployable()) {
      LOG.info("Succeeding a deploy {} because the request {} was not deployable", pendingDeploy, request);

      return new SingularityDeployResult(DeployState.SUCCEEDED, "Request not deployable");
    }

    if (!inactiveDeployMatchingTasks.isEmpty()) {
      return new SingularityDeployResult(DeployState.FAILED, format("Task(s) %s for this deploy failed", inactiveDeployMatchingTasks));
    }

    final boolean isCancelRequestPresent = cancelRequest.isPresent();
    final boolean isDeployOverdue = isDeployOverdue(pendingDeploy, deploy);

    if (isCancelRequestPresent || isDeployOverdue) {
      if (isCancelRequestPresent) {
        LOG.info("Canceling a deploy {} due to cancel request {}", pendingDeploy, cancelRequest.get());
        return new SingularityDeployResult(DeployState.CANCELED, format("Canceled due to request by %s at %s", cancelRequest.get().getUser(), cancelRequest.get().getTimestamp()));
      }
    }

    if ((deployActiveTasks.size() < request.getInstancesSafe()) || !deploy.isPresent()) {
      String message = null;

      if (deploy.isPresent()) {
        message = format("Deploy was only able to launch %s out of a required %s tasks in %s: it is likely not enough resources or slaves are available and eligible", deployActiveTasks.size(), request.getInstancesSafe(), JavaUtils.durationFromMillis(getAllowedMillis(deploy.get())));
      }

      return checkOverdue(deploy, isDeployOverdue, message);
    }

    final DeployHealth deployHealth = deployHealthHelper.getDeployHealth(deploy, deployActiveTasks, true);

    switch (deployHealth) {
      case WAITING:
        String message = null;

        if (deploy.isPresent()) {
          message = format("Deploy was able to launch %s tasks, but not all of them became healthy within %s", deployActiveTasks.size(), JavaUtils.durationFromMillis(getAllowedMillis(deploy.get())));
        }

        return checkOverdue(deploy, isDeployOverdue, message);
      case HEALTHY:
        return new SingularityDeployResult(DeployState.SUCCEEDED);
      case UNHEALTHY:
        break;
    }

    return new SingularityDeployResult(DeployState.FAILED, "At least one task for this deploy failed");
  }

  private SingularityDeployResult checkOverdue(Optional<SingularityDeploy> deploy, boolean isOverdue, String message) {
    if (deploy.isPresent() && isOverdue) {
      return new SingularityDeployResult(DeployState.OVERDUE, message);
    } else {
      return new SingularityDeployResult(DeployState.WAITING);
    }
  }

}
