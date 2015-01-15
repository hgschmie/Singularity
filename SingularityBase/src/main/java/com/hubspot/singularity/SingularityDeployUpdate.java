package com.hubspot.singularity;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

public class SingularityDeployUpdate {

  public enum DeployEventType {
    STARTING, FINISHED;
  }

  private final SingularityDeployMarker deployMarker;
  private final Optional<SingularityDeploy> deploy;
  private final DeployEventType eventType;
  private final Optional<SingularityDeployResult> deployResult;

  @JsonCreator
  public SingularityDeployUpdate(@JsonProperty("deployMarker") SingularityDeployMarker deployMarker, @JsonProperty("deploy") Optional<SingularityDeploy> deploy, @JsonProperty("eventType") DeployEventType eventType, @JsonProperty("deployResult") Optional<SingularityDeployResult> deployResult) {
    this.deployMarker = checkNotNull(deployMarker, "deployMarker is null");
    this.deploy = checkNotNull(deploy, "deploy is null");
    this.eventType = checkNotNull(eventType, "eventType is null");
    this.deployResult = checkNotNull(deployResult, "deployResult is null");
  }

  public SingularityDeployMarker getDeployMarker() {
    return deployMarker;
  }

  public Optional<SingularityDeploy> getDeploy() {
    return deploy;
  }

  public DeployEventType getEventType() {
    return eventType;
  }

  public Optional<SingularityDeployResult> getDeployResult() {
    return deployResult;
  }

  @Override
  public String toString() {
    return "SingularityDeployWebhook [deployMarker=" + deployMarker + ", deploy=" + deploy + ", eventType=" + eventType + ", deployResult=" + deployResult + "]";
  }

}
