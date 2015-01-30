package com.hubspot.singularity.data;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.hubspot.singularity.WebExceptions.checkBadRequest;

import java.util.List;
import java.util.UUID;

import javax.inject.Singleton;

import org.apache.mesos.Protos;
import org.quartz.CronExpression;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.hubspot.mesos.SingularityDockerInfo;
import com.hubspot.mesos.SingularityDockerPortMapping;
import com.hubspot.mesos.SingularityPortMappingType;
import com.hubspot.mesos.SingularityResourceRequest;
import com.hubspot.singularity.ScheduleType;
import com.hubspot.singularity.SingularityDeploy;
import com.hubspot.singularity.SingularityDeployBuilder;
import com.hubspot.singularity.SingularityRequest;
import com.hubspot.singularity.config.SingularityConfiguration;
import com.hubspot.singularity.data.history.DeployHistoryHelper;

@Singleton
public class SingularityValidator {

  private static final Joiner JOINER = Joiner.on(" ");

  private final int maxDeployIdSize;
  private final int maxRequestIdSize;
  private final Optional<Integer> maxCpusPerRequest;
  private final Optional<Integer> maxCpusPerInstance;
  private final Optional<Integer> maxInstancesPerRequest;
  private final Optional<Integer> maxMemoryMbPerRequest;
  private final int defaultCpus;
  private final int defaultMemoryMb;
  private final Optional<Integer> maxMemoryMbPerInstance;
  private final boolean allowRequestsWithoutOwners;
  private final boolean createDeployIds;
  private final int deployIdLength;
  private final DeployHistoryHelper deployHistoryHelper;
  private final List<SingularityResourceRequest> defaultResources;

  @Inject
  public SingularityValidator(SingularityConfiguration configuration, DeployHistoryHelper deployHistoryHelper) {
    this.maxDeployIdSize = configuration.getMaxDeployIdSize();
    this.maxRequestIdSize = configuration.getMaxRequestIdSize();
    this.allowRequestsWithoutOwners = configuration.isAllowRequestsWithoutOwners();
    this.createDeployIds = configuration.isCreateDeployIds();
    this.deployIdLength = configuration.getDeployIdLength();
    this.deployHistoryHelper = deployHistoryHelper;

    this.defaultCpus = configuration.getMesosConfiguration().getDefaultCpus();
    this.defaultMemoryMb = configuration.getMesosConfiguration().getDefaultMemory();
    this.defaultResources = configuration.getMesosConfiguration().getDefaultResources();

    this.maxCpusPerInstance = configuration.getMesosConfiguration().getMaxNumCpusPerInstance();
    this.maxCpusPerRequest = configuration.getMesosConfiguration().getMaxNumCpusPerRequest();
    this.maxMemoryMbPerInstance = configuration.getMesosConfiguration().getMaxMemoryMbPerInstance();
    this.maxMemoryMbPerRequest = configuration.getMesosConfiguration().getMaxMemoryMbPerRequest();
    this.maxInstancesPerRequest = configuration.getMesosConfiguration().getMaxNumInstancesPerRequest();
  }

  private void checkForIllegalChanges(SingularityRequest request, SingularityRequest existingRequest) {
    checkBadRequest(request.isScheduled() == existingRequest.isScheduled(), "Request can not change whether it is a scheduled request");
    checkBadRequest(request.isDaemon() == existingRequest.isDaemon(), "Request can not change whether it is a daemon");
  }

  private void checkForIllegalResources(SingularityRequest request, SingularityDeploy deploy) {
    int instances = request.getInstancesSafe();

    List<SingularityResourceRequest> resources = deploy.getResourceRequestList().or(defaultResources);

    final double cpusPerInstance = SingularityResourceRequest.findNumberResourceRequest(resources, SingularityResourceRequest.CPU_RESOURCE_NAME, defaultCpus).doubleValue();
    final double memoryMbPerInstance = SingularityResourceRequest.findNumberResourceRequest(resources, SingularityResourceRequest.MEMORY_RESOURCE_NAME, defaultMemoryMb).doubleValue();

    checkBadRequest(cpusPerInstance > 0, "Request must have more than 0 cpus");
    checkBadRequest(memoryMbPerInstance > 0, "Request must have more than 0 memoryMb");

    if (maxCpusPerInstance.isPresent()) {
      checkBadRequest(cpusPerInstance <= maxCpusPerInstance.get(), "Deploy %s uses too many cpus %s (maxCpusPerInstance %s in mesos configuration)", deploy.getId(), cpusPerInstance, maxCpusPerInstance);
    }
    if (maxCpusPerRequest.isPresent()) {
      checkBadRequest(cpusPerInstance * instances <= maxCpusPerRequest.get(),
          "Deploy %s uses too many cpus %s (%s*%s) (cpusPerRequest %s in mesos configuration)", deploy.getId(), cpusPerInstance * instances, cpusPerInstance, instances, maxCpusPerRequest);
    }

    if (maxMemoryMbPerInstance.isPresent()) {
      checkBadRequest(memoryMbPerInstance <= maxMemoryMbPerInstance.get(),
          "Deploy %s uses too much memoryMb %s (maxMemoryMbPerInstance %s in mesos configuration)", deploy.getId(), memoryMbPerInstance, maxMemoryMbPerInstance);
    }

    if (maxMemoryMbPerRequest.isPresent()) {
      checkBadRequest(memoryMbPerInstance * instances <= maxMemoryMbPerRequest.get(), "Deploy %s uses too much memoryMb %s (%s*%s) (maxMemoryMbPerRequest %s in mesos configuration)", deploy.getId(),
          memoryMbPerInstance * instances, memoryMbPerInstance, instances, maxMemoryMbPerRequest);
    }
  }

  public SingularityRequest checkSingularityRequest(SingularityRequest request, Optional<SingularityRequest> existingRequest, Optional<SingularityDeploy> activeDeploy,
      Optional<SingularityDeploy> pendingDeploy) {
    checkBadRequest(request.getId() != null && !request.getId().contains("/"), "Id can not be null or contain / characters");

    if (!allowRequestsWithoutOwners) {
      checkBadRequest(request.getOwners().isPresent() && !request.getOwners().get().isEmpty(), "Request must have owners defined (this can be turned off in Singularity configuration)");
    }

    checkBadRequest(request.getId().length() < maxRequestIdSize, "Request id must be less than %s characters, it is %s (%s)", maxRequestIdSize, request.getId().length(), request.getId());
    checkBadRequest(!request.getInstances().isPresent() || request.getInstances().get() > 0, "Instances must be greater than 0");

    if (maxInstancesPerRequest.isPresent()) {
      checkBadRequest(request.getInstancesSafe() <= maxInstancesPerRequest.get(),"Instances (%s) be greater than %s (maxInstancesPerRequest in mesos configuration)", request.getInstancesSafe(), maxInstancesPerRequest);
    }

    if (existingRequest.isPresent()) {
      checkForIllegalChanges(request, existingRequest.get());
    }

    if (activeDeploy.isPresent()) {
      checkForIllegalResources(request, activeDeploy.get());
    }

    if (pendingDeploy.isPresent()) {
      checkForIllegalResources(request, pendingDeploy.get());
    }

    String quartzSchedule = null;

    if (request.isScheduled()) {
      final String originalSchedule = request.getQuartzScheduleSafe();

      checkBadRequest(request.getQuartzSchedule().isPresent() || request.getSchedule().isPresent(), "Specify at least one of schedule or quartzSchedule");

      checkBadRequest(!request.getDaemon().isPresent(), "Scheduled request must not set a daemon flag");

      if (request.getQuartzSchedule().isPresent() && !request.getSchedule().isPresent()) {
        checkBadRequest(request.getScheduleType().or(ScheduleType.QUARTZ) == ScheduleType.QUARTZ, "If using quartzSchedule specify scheduleType QUARTZ or leave it blank");
      }

      if (request.getQuartzSchedule().isPresent() || request.getScheduleType().isPresent() && request.getScheduleType().get() == ScheduleType.QUARTZ) {
        quartzSchedule = originalSchedule;
      } else {
        checkBadRequest(request.getScheduleType().or(ScheduleType.CRON) == ScheduleType.CRON, "If not using quartzSchedule specify scheduleType CRON or leave it blank");
        checkBadRequest(!request.getQuartzSchedule().isPresent(), "If using schedule type CRON do not specify quartzSchedule");

        quartzSchedule = getQuartzScheduleFromCronSchedule(originalSchedule);
      }

      checkBadRequest(isValidCronSchedule(quartzSchedule), "Schedule %s (from: %s) was not valid", quartzSchedule, originalSchedule);
    } else {
      checkBadRequest(!request.getScheduleType().isPresent(), "ScheduleType can only be set for scheduled requests");
      checkBadRequest(!request.getNumRetriesOnFailure().isPresent(), "NumRetriesOnFailure can only be set for scheduled requests");
    }

    if (!request.isLongRunning()) {
      checkBadRequest(!request.isRackSensitive(), "non-longRunning (scheduled/oneoff) requests can not be rack sensitive");
    } else {
      checkBadRequest(!request.getKillOldNonLongRunningTasksAfterMillis().isPresent(), "longRunning requests can not define a killOldNonLongRunningTasksAfterMillis value");
    }

    if (request.isScheduled()) {
      checkBadRequest(request.getInstances().or(1) == 1, "Scheduler requests can not be ran on more than one instance");
    } else if (request.isOneOff()) {
      checkBadRequest(!request.getInstances().isPresent(), "one-off requests can not define a # of instances");
    }

    return request.toBuilder().setQuartzSchedule(Optional.fromNullable(quartzSchedule)).build();
  }

  public SingularityDeploy checkDeploy(SingularityRequest request, SingularityDeploy deploy) {
    checkNotNull(request, "request is null");
    checkNotNull(deploy, "deploy is null");

    String deployId = deploy.getId();

    if (deployId == null) {
      checkBadRequest(createDeployIds, "Id must not be null");
      SingularityDeployBuilder builder = deploy.toBuilder();
      builder.setId(createUniqueDeployId());
      deploy = builder.build();
      deployId = deploy.getId();
    }

    checkBadRequest(!deployId.contains("/") && !deployId.contains("-"), "Id must not contain / or - characters");
    checkBadRequest(deployId.length() < maxDeployIdSize, "Deploy id must be less than %s characters, it is %s (%s)", maxDeployIdSize, deployId.length(), deployId);
    checkBadRequest(deploy.getRequestId() != null && deploy.getRequestId().equals(request.getId()), "Deploy id must match request id");

    checkForIllegalResources(request, deploy);

    checkBadRequest(deploy.getCommand().isPresent() && !deploy.getExecutorData().isPresent() ||
        deploy.getExecutorData().isPresent() && deploy.getCustomExecutorCmd().isPresent() && !deploy.getCommand().isPresent() ||
        deploy.getContainerInfo().isPresent(),
        "If not using custom executor, specify a command or containerInfo. If using custom executor, specify executorData and customExecutorCmd and no command.");

    checkBadRequest(!deploy.getContainerInfo().isPresent() || deploy.getContainerInfo().get().getType() != null, "Container type must not be null");

    if (deploy.getContainerInfo().isPresent() && deploy.getContainerInfo().get().getType() == Protos.ContainerInfo.Type.DOCKER) {
      checkDocker(deploy);
    }

    checkBadRequest(deployHistoryHelper.isDeployIdAvailable(request.getId(), deployId), "Can not deploy a deploy that has already been deployed");

    return deploy;
  }

  private String createUniqueDeployId() {
    UUID id = UUID.randomUUID();
    String result = Hashing.sha256().newHasher().putLong(id.getLeastSignificantBits()).putLong(id.getMostSignificantBits()).hash().toString();
    return result.substring(0, deployIdLength);
  }

  private void checkDocker(SingularityDeploy deploy) {
    Optional<List<SingularityResourceRequest>> resources = deploy.getResourceRequestList();

    if (resources.isPresent() && deploy.getContainerInfo().get().getDocker().isPresent()) {
      final SingularityDockerInfo dockerInfo = deploy.getContainerInfo().get().getDocker().get();

      int numPorts = SingularityResourceRequest.findNumberResourceRequest(resources.get(), SingularityResourceRequest.PORT_COUNT_RESOURCE_NAME, 0).intValue();

      if (!dockerInfo.getPortMappings().isEmpty()) {
        checkBadRequest(dockerInfo.getNetwork().or(Protos.ContainerInfo.DockerInfo.Network.HOST) == Protos.ContainerInfo.DockerInfo.Network.BRIDGE,
            "Docker networking type must be BRIDGE if port mappings are set");
      }

      for (SingularityDockerPortMapping portMapping : dockerInfo.getPortMappings()) {
        if (portMapping.getContainerPortType() == SingularityPortMappingType.FROM_OFFER) {
          checkBadRequest(portMapping.getContainerPort() >= 0 && portMapping.getContainerPort() < numPorts,
              "Index of port resource for containerPort must be between 0 and %d (inclusive)", numPorts - 1);
        }

        if (portMapping.getHostPortType() == SingularityPortMappingType.FROM_OFFER) {
          checkBadRequest(portMapping.getHostPort() >= 0 && portMapping.getHostPort() < numPorts,
              "Index of port resource for hostPort must be between 0 and %d (inclusive)", numPorts - 1);
        }
      }
    }
  }

  private boolean isValidCronSchedule(String schedule) {
    return CronExpression.isValidExpression(schedule);
  }

  /**
   *
   * Transforms unix cron into quartz compatible cron;
   *
   * - adds seconds if not included - switches either day of month or day of week to ?
   *
   * Field Name   Allowed Values          Allowed Special Characters
   * Seconds      0-59                    - * /
   * Minutes      0-59                    - * /
   * Hours        0-23                    - * /
   * Day-of-month 1-31                    - * ? / L W
   * Month        1-12 or JAN-DEC         - * /
   * Day-of-Week  1-7 or SUN-SAT          - * ? / L #
   * Year         (Optional), 1970-2199   - * /
   */
  private String getQuartzScheduleFromCronSchedule(String schedule) {
    if (schedule == null) {
      return null;
    }

    String[] split = schedule.split(" ");

    checkBadRequest(split.length >= 4, "Schedule %s is invalid because it contained only %s splits (looking for at least 4)", schedule, split.length);

    List<String> newSchedule = Lists.newArrayListWithCapacity(6);

    boolean hasSeconds = split.length > 5;

    if (!hasSeconds) {
      newSchedule.add("0");
    } else {
      newSchedule.add(split[0]);
    }

    int indexMod = hasSeconds ? 1 : 0;

    newSchedule.add(split[indexMod + 0]);
    newSchedule.add(split[indexMod + 1]);

    String dayOfMonth = split[indexMod + 2];
    String dayOfWeek = split[indexMod + 4];

    if (dayOfWeek.equals("*")) {
      dayOfWeek = "?";
    } else if (!dayOfWeek.equals("?")) {
      dayOfMonth = "?";
    }

    // standard cron is 0-6, quartz is 1-7
    // therefore, we should add 1 to any values between 0-6. 7 in a standard cron is sunday,
    // which is sat in quartz. so if we get a value of 7, we should change it to 1.
    if (isValidInteger(dayOfWeek)) {
      int dayOfWeekValue = Integer.parseInt(dayOfWeek);

      checkBadRequest(dayOfWeekValue >= 0 && dayOfWeekValue <= 7, "Schedule %s is invalid, day of week (%s) is not 0-7", schedule, dayOfWeekValue);

      if (dayOfWeekValue == 7) {
        dayOfWeekValue = 1;
      } else {
        dayOfWeekValue++;
      }

      dayOfWeek = Integer.toString(dayOfWeekValue);
    }

    newSchedule.add(dayOfMonth);
    newSchedule.add(split[indexMod + 3]);
    newSchedule.add(dayOfWeek);

    return JOINER.join(newSchedule);
  }

  private boolean isValidInteger(String strValue) {
    try {
      Integer.parseInt(strValue);
      return true;
    } catch (NumberFormatException nfe) {
      return false;
    }
  }

}
