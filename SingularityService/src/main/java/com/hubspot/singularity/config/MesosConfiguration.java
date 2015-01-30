package com.hubspot.singularity.config;

import javax.validation.constraints.NotNull;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.hubspot.mesos.SingularityResourceRequest;

public class MesosConfiguration {

  private boolean useNativeCode = true;

  @NotNull
  private String master;

  @NotNull
  private String frameworkName;

  @NotNull
  private String frameworkId;

  private double frameworkFailoverTimeout = 0.0;

  private int defaultCpus = 1;

  private int defaultMemory = 64;

  private boolean checkpoint = false;

  @NotNull
  private String rackIdAttributeKey = "rackid";

  @NotNull
  private String defaultRackId = "DEFAULT";

  private int slaveHttpPort = 5051;

  private Integer maxNumInstancesPerRequest = null; // 25;
  private Integer maxNumCpusPerInstance = null; // 50
  private Integer maxNumCpusPerRequest = null; // 900
  private Integer maxMemoryMbPerInstance = null; // 24_000
  private Integer maxMemoryMbPerRequest = null;  // 450_000

  public boolean isUseNativeCode() {
    return useNativeCode;
  }

  public void setUseNativeCode(boolean useNativeCode) {
    this.useNativeCode = useNativeCode;
  }

  public Optional<Integer> getMaxNumInstancesPerRequest() {
    return Optional.fromNullable(maxNumInstancesPerRequest);
  }

  public void setMaxNumInstancesPerRequest(Integer maxNumInstancesPerRequest) {
    this.maxNumInstancesPerRequest = maxNumInstancesPerRequest;
  }

  public Optional<Integer> getMaxNumCpusPerInstance() {
    return Optional.fromNullable(maxNumCpusPerInstance);
  }

  public void setMaxNumCpusPerInstance(Integer maxNumCpusPerInstance) {
    this.maxNumCpusPerInstance = maxNumCpusPerInstance;
  }

  public Optional<Integer> getMaxNumCpusPerRequest() {
    return Optional.fromNullable(maxNumCpusPerRequest);
  }

  public void setMaxNumCpusPerRequest(Integer maxNumCpusPerRequest) {
    this.maxNumCpusPerRequest = maxNumCpusPerRequest;
  }

  public Optional<Integer> getMaxMemoryMbPerInstance() {
    return Optional.fromNullable(maxMemoryMbPerInstance);
  }

  public void setMaxMemoryMbPerInstance(Integer maxMemoryMbPerInstance) {
    this.maxMemoryMbPerInstance = maxMemoryMbPerInstance;
  }

  public Optional<Integer> getMaxMemoryMbPerRequest() {
    return Optional.fromNullable(maxMemoryMbPerRequest);
  }

  public void setMaxMemoryMbPerRequest(Integer maxMemoryMbPerRequest) {
    this.maxMemoryMbPerRequest = maxMemoryMbPerRequest;
  }

  public String getRackIdAttributeKey() {
    return rackIdAttributeKey;
  }

  public String getDefaultRackId() {
    return defaultRackId;
  }

  public void setDefaultRackId(String defaultRackId) {
    this.defaultRackId = defaultRackId;
  }

  public void setRackIdAttributeKey(String rackIdAttributeKey) {
    this.rackIdAttributeKey = rackIdAttributeKey;
  }

  public String getMaster() {
    return master;
  }

  public String getFrameworkId() {
    return frameworkId;
  }

  public void setFrameworkId(String frameworkId) {
    this.frameworkId = frameworkId;
  }

  public String getFrameworkName() {
    return frameworkName;
  }

  public void setFrameworkName(String frameworkName) {
    this.frameworkName = frameworkName;
  }

  public double getFrameworkFailoverTimeout() {
    return frameworkFailoverTimeout;
  }

  public void setFrameworkFailoverTimeout(double frameworkFailoverTimeout) {
    this.frameworkFailoverTimeout = frameworkFailoverTimeout;
  }

  public void setMaster(String master) {
    this.master = master;
  }

  public boolean getCheckpoint() {
    return checkpoint;
  }

  public void setCheckpoint(boolean checkpoint) {
    this.checkpoint = checkpoint;
  }

  public int getDefaultCpus() {
    return defaultCpus;
  }

  public void setDefaultCpus(int defaultCpus) {
    this.defaultCpus = defaultCpus;
  }

  public int getDefaultMemory() {
    return defaultMemory;
  }

  public void setDefaultMemory(int defaultMemory) {
    this.defaultMemory = defaultMemory;
  }

  public int getSlaveHttpPort() {
    return slaveHttpPort;
  }

  public void setSlaveHttpPort(int slaveHttpPort) {
    this.slaveHttpPort = slaveHttpPort;
  }

  /**
   * Returns the default configured resources as a {@link SingularityResourceRequest} list.
   */
  public ImmutableList<SingularityResourceRequest> getDefaultResources() {
    return ImmutableList.of(new SingularityResourceRequest(SingularityResourceRequest.CPU_RESOURCE_NAME, defaultCpus),
      new SingularityResourceRequest(SingularityResourceRequest.MEMORY_RESOURCE_NAME, defaultMemory));
  }
}
