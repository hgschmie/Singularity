package com.hubspot.singularity.config;

import java.util.Map;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;


@JsonIgnoreProperties(ignoreUnknown = true)
public class BaragonConfiguration {

  @JsonProperty("loadBalancerQueryParams")
  private Map<String, String> loadBalancerQueryParams;

  private long loadBalancerRequestTimeoutMillis = 2000L;

  private String loadBalancerUri;

  public Optional<Map<String, String>> getLoadBalancerQueryParams() {
    return Optional.fromNullable(loadBalancerQueryParams);
  }

  public long getLoadBalancerRequestTimeoutMillis() {
    return loadBalancerRequestTimeoutMillis;
  }

  @Nonnull
  public String getLoadBalancerUri() {
    return loadBalancerUri;
  }

  public void setLoadBalancerQueryParams(Map<String, String> loadBalancerQueryParams) {
    this.loadBalancerQueryParams = loadBalancerQueryParams;
  }

  public void setLoadBalancerRequestTimeoutMillis(long loadBalancerRequestTimeoutMillis) {
    this.loadBalancerRequestTimeoutMillis = loadBalancerRequestTimeoutMillis;
  }

  public void setLoadBalancerUri(String loadBalancerUri) {
    this.loadBalancerUri = loadBalancerUri;
  }
}
