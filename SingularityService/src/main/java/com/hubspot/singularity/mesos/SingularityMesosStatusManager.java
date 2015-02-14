package com.hubspot.singularity.mesos;

import javax.inject.Singleton;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ContainerNetworkSettings;
import org.apache.mesos.Protos.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
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
import com.hubspot.singularity.scheduler.SingularityHealthchecker;
import com.hubspot.singularity.scheduler.SingularityNewTaskChecker;
import com.hubspot.singularity.scheduler.SingularityScheduler;
import com.hubspot.singularity.scheduler.SingularitySchedulerStateCache;

@Singleton
public class SingularityMesosStatusManager extends SingularitySchedulerParticipant {

  private static final Logger LOG = LoggerFactory.getLogger(SingularityMesosStatusManager.class);

  private final TaskManager taskManager;
  private final DeployManager deployManager;
  private final SingularityScheduler scheduler;
  private final SingularityHealthchecker healthchecker;
  private final SingularityNewTaskChecker newTaskChecker;
  private final SingularityLogSupport logSupport;

  private final Provider<SingularitySchedulerStateCache> stateCacheProvider;
  private final String serverId;

  private final IdTranscoder<SingularityTaskId> taskIdTranscoder;

  @Inject
  SingularityMesosStatusManager(TaskManager taskManager, SingularityScheduler scheduler, SingularityNewTaskChecker newTaskChecker,
      SingularityLogSupport logSupport, Provider<SingularitySchedulerStateCache> stateCacheProvider, SingularityHealthchecker healthchecker,
      DeployManager deployManager, @Named(SingularityMainModule.SERVER_ID_PROPERTY) String serverId, final IdTranscoder<SingularityTaskId> taskIdTranscoder) {
    this.taskManager = taskManager;
    this.deployManager = deployManager;
    this.newTaskChecker = newTaskChecker;
    this.scheduler = scheduler;
    this.logSupport = logSupport;
    this.stateCacheProvider = stateCacheProvider;
    this.healthchecker = healthchecker;
    this.serverId = serverId;
    this.taskIdTranscoder = taskIdTranscoder;
  }

  /**
   * 1 - we have a previous update, and this is a duplicate of it (ignore)
   * 2 - we don't have a previous update, 2 cases:
   *   a - this task has already been destroyed (we can ignore it then)
   *   b - we've never heard of this task (very unlikely since we first write a status into zk before we launch a task)
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

  @Override
  public void statusUpdate(Protos.TaskStatus status) throws Exception {
    final String taskId = status.getTaskId().getValue();

    long timestamp = System.currentTimeMillis();

    if (status.hasTimestamp()) {
      timestamp = (long) (status.getTimestamp() * 1000);
    }

    LOG.debug("Task {} is now {} ({}) at {} ", taskId, status.getState(), status.getMessage(), timestamp);

    final SingularityTaskId taskIdObj = taskIdTranscoder.fromString(taskId);

    final Optional<SingularityTaskStatusHolder> previousTaskStatusHolder = taskManager.getLastActiveTaskStatus(taskIdObj);


    // If the status update did not come from the slave (e.g. because of reconciliation), keep the Container network info alive.
    if (status.getSource() != TaskStatus.Source.SOURCE_SLAVE) {
      Protos.TaskStatus.Builder builder = Protos.TaskStatus.newBuilder(status);
      if (previousTaskStatusHolder.isPresent() && previousTaskStatusHolder.get().getTaskStatus().isPresent()) {
        Protos.TaskStatus oldStatus = previousTaskStatusHolder.get().getTaskStatus().get();
        if (oldStatus.hasContainerNetworkSettings()) {
          builder.setContainerNetworkSettings(oldStatus.getContainerNetworkSettings());
          status = builder.build();
        }
      }
    }

    final SingularityTaskStatusHolder newTaskStatusHolder = new SingularityTaskStatusHolder(taskIdObj, Optional.of(status), System.currentTimeMillis(), serverId, Optional.<String> absent());


    final ExtendedTaskState taskState = ExtendedTaskState.fromTaskState(status.getState());

    if (isDuplicateOrIgnorableStatusUpdate(previousTaskStatusHolder, newTaskStatusHolder)) {
      LOG.trace("Ignoring status update {} to {}", taskState, taskIdObj);
      saveNewTaskStatusHolder(taskIdObj, newTaskStatusHolder, taskState);
      return;
    }

    final Optional<SingularityTask> task = taskManager.getTask(taskIdObj);

    final boolean isActiveTask = taskManager.isActiveTask(taskId);

    if (isActiveTask && !taskState.isDone()) {
      final Optional<SingularityPendingDeploy> pendingDeploy = deployManager.getPendingDeploy(taskIdObj.getRequestId());

      if (taskState == ExtendedTaskState.TASK_RUNNING) {
        healthchecker.enqueueHealthcheck(task.get(), pendingDeploy);
      }

      if (!pendingDeploy.isPresent() || !pendingDeploy.get().getDeployMarker().getDeployId().equals(taskIdObj.getDeployId())) {
        newTaskChecker.enqueueNewTaskCheck(task.get());
      }
    }

    final SingularityTaskHistoryUpdate taskUpdate = new SingularityTaskHistoryUpdate(taskIdObj,
        timestamp,
        taskState,
        status.hasMessage() ? Optional.of(status.getMessage()) : Optional.<String> absent(),
        status.hasContainerNetworkSettings() ? Optional.of(status.getContainerNetworkSettings()) : Optional.<ContainerNetworkSettings>absent());
    final SingularityCreateResult taskHistoryUpdateCreateResult = taskManager.saveTaskHistoryUpdate(taskUpdate);

    logSupport.checkDirectory(taskIdObj);

    if (taskState.isDone()) {
      healthchecker.cancelHealthcheck(taskId);
      newTaskChecker.cancelNewTaskCheck(taskId);

      taskManager.deleteKilledRecord(taskIdObj);

      SingularitySchedulerStateCache stateCache = stateCacheProvider.get();

      scheduler.handleCompletedTask(task, taskIdObj, isActiveTask, timestamp, taskState, taskHistoryUpdateCreateResult, stateCache);
    }

    saveNewTaskStatusHolder(taskIdObj, newTaskStatusHolder, taskState);
  }
}
