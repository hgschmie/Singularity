package com.hubspot.singularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

public class SingularityDeployResult {

  private final DeployState deployState;
  private final Optional<String> message;
  private final long timestamp;

  public SingularityDeployResult(DeployState deployState) {
    this(deployState, Optional.<String> absent(), System.currentTimeMillis());
  }

  public SingularityDeployResult(DeployState deployState, String message) {
    this(deployState, Optional.of(message), System.currentTimeMillis());
  }

  @JsonCreator
  public SingularityDeployResult(@JsonProperty("deployState") DeployState deployState, @JsonProperty("message") Optional<String> message, @JsonProperty("timestamp") long timestamp) {
    this.deployState = deployState;
    this.message = message;
    this.timestamp = timestamp;
  }

  public Optional<String> getMessage() {
    return message;
  }

  public DeployState getDeployState() {
    return deployState;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "SingularityDeployResult [deployState=" + deployState + ", message=" + message + ", timestamp=" + timestamp + "]";
  }

}
