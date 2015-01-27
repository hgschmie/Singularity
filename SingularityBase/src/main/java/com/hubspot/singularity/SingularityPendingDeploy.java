package com.hubspot.singularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SingularityPendingDeploy {

  private final SingularityDeployMarker deployMarker;
  private final DeployState currentDeployState;

  @JsonCreator
  public SingularityPendingDeploy(@JsonProperty("deployMarker") SingularityDeployMarker deployMarker,
      @JsonProperty("currentDeployState") DeployState currentDeployState) {
    this.deployMarker = deployMarker;
    this.currentDeployState = currentDeployState;
  }

  public SingularityDeployMarker getDeployMarker() {
    return deployMarker;
  }

  public DeployState getCurrentDeployState() {
    return currentDeployState;
  }

  @Override
  public String toString() {
    return "SingularityPendingDeploy [deployMarker=" + deployMarker + ", currentDeployState=" + currentDeployState + "]";
  }

}
