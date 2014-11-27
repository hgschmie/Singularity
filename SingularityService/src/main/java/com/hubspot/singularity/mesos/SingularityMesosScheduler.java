package com.hubspot.singularity.mesos;

import java.util.List;

import javax.inject.Singleton;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.hubspot.mesos.MesosUtils;
import com.hubspot.singularity.ExtendedTaskState;
import com.hubspot.singularity.SingularityCreateResult;
import com.hubspot.singularity.SingularityMainModule;
import com.hubspot.singularity.SingularityPendingDeploy;
import com.hubspot.singularity.SingularityTask;
import com.hubspot.singularity.SingularityTaskHistoryUpdate;
import com.hubspot.singularity.SingularityTaskId;
import com.hubspot.singularity.SingularityTaskStatusHolder;
import com.hubspot.singularity.data.DeployManager;
import com.hubspot.singularity.data.TaskManager;
import com.hubspot.singularity.data.transcoders.IdTranscoder;
import com.hubspot.singularity.mesos.scheduler.ResourceStrategy;
import com.hubspot.singularity.scheduler.SingularityHealthchecker;
import com.hubspot.singularity.scheduler.SingularityNewTaskChecker;
import com.hubspot.singularity.scheduler.SingularityScheduler;
import com.hubspot.singularity.scheduler.SingularitySchedulerStateCache;

@Singleton
public class SingularityMesosScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(SingularityMesosScheduler.class);

  private final TaskManager taskManager;
  private final DeployManager deployManager;
  private final SingularityScheduler scheduler;
  private final SingularityHealthchecker healthchecker;
  private final SingularityNewTaskChecker newTaskChecker;
  private final SingularitySlaveAndRackManager slaveAndRackManager;
  private final SingularityLogSupport logSupport;

  private final Provider<SingularitySchedulerStateCache> stateCacheProvider;
  private final String serverId;

  private final IdTranscoder<SingularityTaskId> taskIdTranscoder;

  private final ResourceStrategy resourceStrategy;

  @Inject
  SingularityMesosScheduler(TaskManager taskManager,
      SingularityScheduler scheduler,
      SingularitySlaveAndRackManager slaveAndRackManager,
      SingularityNewTaskChecker newTaskChecker,
      SingularityLogSupport logSupport,
      Provider<SingularitySchedulerStateCache> stateCacheProvider,
      SingularityHealthchecker healthchecker,
      DeployManager deployManager,
      @Named(SingularityMainModule.SERVER_ID_PROPERTY) String serverId,
      final IdTranscoder<SingularityTaskId> taskIdTranscoder,
      ResourceStrategy resourceStrategy) {
    this.taskManager = taskManager;
    this.deployManager = deployManager;
    this.newTaskChecker = newTaskChecker;
    this.slaveAndRackManager = slaveAndRackManager;
    this.scheduler = scheduler;
    this.logSupport = logSupport;
    this.stateCacheProvider = stateCacheProvider;
    this.healthchecker = healthchecker;
    this.serverId = serverId;
    this.taskIdTranscoder = taskIdTranscoder;
    this.resourceStrategy = resourceStrategy;
  }

  public void resourceOffers(List<Protos.Offer> offers) {
    LOG.info("Received {} offer(s)", offers.size());

    for (Offer offer : offers) {
      MesosUtils.displayOffer(offer);
    }

    final SingularitySchedulerStateCache stateCache = stateCacheProvider.get();

    scheduler.checkForDecomissions(stateCache);
    scheduler.drainPendingQueue(stateCache);

    resourceStrategy.processOffers(offers, stateCache);
  }

  /**
   * 1- we have a previous update, and this is a duplicate of it (ignore) 2- we don't have a
   * previous update, 2 cases: a - this task has already been destroyed (we can ignore it then) b -
   * we've never heard of this task (very unlikely since we first write a status into zk before we
   * launch a task)
   */
  private boolean isDuplicateOrIgnorableStatusUpdate(Optional<SingularityTaskStatusHolder> previousTaskStatusHolder, final SingularityTaskStatusHolder newTaskStatusHolder) {
    if (!previousTaskStatusHolder.isPresent()) {
      return true;
    }

    if (!previousTaskStatusHolder.get().getTaskStatus().isPresent()) { // this is our launch state
      return false;
    }

    return previousTaskStatusHolder.get().getTaskStatus().get().getState() == newTaskStatusHolder.getTaskStatus().get().getState();
  }

  private void saveNewTaskStatusHolder(SingularityTaskId taskIdObj, SingularityTaskStatusHolder newTaskStatusHolder, ExtendedTaskState taskState) {
    if (taskState.isDone()) {
      taskManager.deleteLastActiveTaskStatus(taskIdObj);
    } else {
      taskManager.saveLastActiveTaskStatus(newTaskStatusHolder);
    }
  }

  public void statusUpdate(Protos.TaskStatus status) {
    final String taskId = status.getTaskId().getValue();

    long timestamp = System.currentTimeMillis();

    if (status.hasTimestamp()) {
      timestamp = (long) (status.getTimestamp() * 1000);
    }

    LOG.debug("Task {} is now {} ({}) at {} ", taskId, status.getState(), status.getMessage(), timestamp);

    final SingularityTaskId taskIdObj = taskIdTranscoder.fromString(taskId);

    final SingularityTaskStatusHolder newTaskStatusHolder = new SingularityTaskStatusHolder(taskIdObj, Optional.of(status), System.currentTimeMillis(), serverId, Optional.<String>absent());
    final Optional<SingularityTaskStatusHolder> previousTaskStatusHolder = taskManager.getLastActiveTaskStatus(taskIdObj);
    final ExtendedTaskState taskState = ExtendedTaskState.fromTaskState(status.getState());

    if (isDuplicateOrIgnorableStatusUpdate(previousTaskStatusHolder, newTaskStatusHolder)) {
      LOG.trace("Ignoring status update {} to {}", taskState, taskIdObj);
      saveNewTaskStatusHolder(taskIdObj, newTaskStatusHolder, taskState);
      return;
    }

    final Optional<SingularityTask> maybeActiveTask = taskManager.getActiveTask(taskId);
    Optional<SingularityPendingDeploy> pendingDeploy = null;

    if (maybeActiveTask.isPresent() && status.getState() == TaskState.TASK_RUNNING) {
      pendingDeploy = deployManager.getPendingDeploy(taskIdObj.getRequestId());

      healthchecker.enqueueHealthcheck(maybeActiveTask.get(), pendingDeploy);
    }

    final SingularityTaskHistoryUpdate taskUpdate =
        new SingularityTaskHistoryUpdate(taskIdObj, timestamp, taskState, status.hasMessage() ? Optional.of(status.getMessage()) : Optional.<String>absent());
    final SingularityCreateResult taskHistoryUpdateCreateResult = taskManager.saveTaskHistoryUpdate(taskUpdate);

    logSupport.checkDirectory(taskIdObj);

    if (taskState.isDone()) {
      healthchecker.cancelHealthcheck(taskId);
      newTaskChecker.cancelNewTaskCheck(taskId);

      taskManager.deleteKilledRecord(taskIdObj);

      scheduler.handleCompletedTask(maybeActiveTask, taskIdObj, timestamp, taskState, taskHistoryUpdateCreateResult, stateCacheProvider.get());
    } else if (maybeActiveTask.isPresent()) {
      if (pendingDeploy == null) {
        pendingDeploy = deployManager.getPendingDeploy(taskIdObj.getRequestId());
      }

      // TODO do we need a new task check if we have hit TASK_RUNNING?
      if (!pendingDeploy.isPresent() || !pendingDeploy.get().getDeployMarker().getDeployId().equals(taskIdObj.getDeployId())) {
        newTaskChecker.enqueueNewTaskCheck(maybeActiveTask.get());
      }
    }

    saveNewTaskStatusHolder(taskIdObj, newTaskStatusHolder, taskState);
  }

  public void slaveLost(Protos.SlaveID slaveId) {
    LOG.warn("Lost a slave {}", slaveId);

    slaveAndRackManager.slaveLost(slaveId);
  }
}
