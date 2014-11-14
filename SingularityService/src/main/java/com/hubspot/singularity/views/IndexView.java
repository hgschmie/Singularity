package com.hubspot.singularity.views;

import static com.google.common.base.Preconditions.checkNotNull;

import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.views.View;

import com.hubspot.singularity.SingularityService;
import com.hubspot.singularity.config.SingularityConfiguration;

public class IndexView extends View {

  private final String serverRoot;
  private final String appRoot;
  private final String staticRoot;
  private final String apiRoot;
  private final String navColor;

  private final Integer defaultMemory;
  private final Integer defaultCpus;

  private final Boolean hideNewDeployButton;
  private final Boolean hideNewRequestButton;

  private final String title;

  private final Integer slaveHttpPort;
  private final Integer slaveHttpsPort;

  public IndexView(String uiPrefix, SingularityConfiguration configuration) {
    super("index.mustache");

    checkNotNull(uiPrefix, "uiPrefix is null");

    String serverRoot = configuration.getUiConfiguration().getBaseUrl().or(((SimpleServerFactory) configuration.getServerFactory()).getApplicationContextPath());
    if (serverRoot.endsWith("/")) {
      serverRoot = serverRoot.substring(0, serverRoot.length() - 1);
    }


    this.serverRoot = serverRoot;
    this.appRoot = String.format("%s%s", serverRoot, uiPrefix);
    this.staticRoot = String.format("%s/static", serverRoot);
    this.apiRoot = String.format("%s%s", serverRoot, SingularityService.API_BASE_PATH);

    this.title = configuration.getUiConfiguration().getTitle();

    this.slaveHttpPort = configuration.getMesosConfiguration().getSlaveHttpPort();
    this.slaveHttpsPort = configuration.getMesosConfiguration().getSlaveHttpsPort().orNull();

    this.defaultCpus = configuration.getMesosConfiguration().getDefaultCpus();
    this.defaultMemory = configuration.getMesosConfiguration().getDefaultMemory();

    this.hideNewDeployButton = configuration.getUiConfiguration().isHideNewDeployButton();
    this.hideNewRequestButton = configuration.getUiConfiguration().isHideNewRequestButton();

    navColor = configuration.getUiConfiguration().getNavColor();
  }

  public String getAppRoot() {
    return appRoot;
  }

  public String getStaticRoot() {
    return staticRoot;
  }

  public String getServerRoot() {
    return serverRoot;
  }

  public String getApiRoot() {
    return apiRoot;
  }

  public String getTitle() {
    return title;
  }

  public String getNavColor() {
    return navColor;
  }

  public Integer getSlaveHttpPort() {
    return slaveHttpPort;
  }

  public Integer getSlaveHttpsPort() {
    return slaveHttpsPort;
  }

  public Integer getDefaultMemory() {
    return defaultMemory;
  }

  public Integer getDefaultCpus() {
    return defaultCpus;
  }

  public Boolean getHideNewDeployButton() {
    return hideNewDeployButton;
  }

  public Boolean getHideNewRequestButton() {
    return hideNewRequestButton;
  }

  @Override
  public String toString() {
    return "IndexView [appRoot=" + appRoot + ", staticRoot=" + staticRoot + ", apiRoot=" + apiRoot + ", navColor=" + navColor + ", defaultMemory=" + defaultMemory + ", defaultCpus=" + defaultCpus
        + ", hideNewDeployButton=" + hideNewDeployButton + ", hideNewRequestButton=" + hideNewRequestButton + ", title=" + title + ", slaveHttpPort=" + slaveHttpPort + ", slaveHttpsPort="
        + slaveHttpsPort + "]";
  }

}
