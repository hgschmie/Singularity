package com.hubspot.singularity.config;

import javax.validation.constraints.Pattern;

import org.hibernate.validator.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

public class UIConfiguration {

  @NotEmpty
  @JsonProperty
  private String title = "Singularity";

  @JsonProperty
  @Pattern( regexp = "^|#[0-9a-fA-F]{6}$" )
  private String navColor = "";

  @JsonProperty
  private String baseUrl;

  private boolean hideNewDeployButton = false;
  private boolean hideNewRequestButton = false;

  /**
   * How to handle the root URL:
   * REDIRECT_TO_UI  -- Redirect / to /ui/
   * INDEX_CATCHALL  -- Serve the index HTML (same as /ui/ endpoint) for any URL not handled by another Resource class
   * DISABLED        -- Don't bind to / or /.*
   */
  @JsonProperty
  private ROOT_URL_MODE rootUrlMode = ROOT_URL_MODE.REDIRECT_TO_UI;

  public boolean isHideNewDeployButton() {
    return hideNewDeployButton;
  }

  public void setHideNewDeployButton(boolean hideNewDeployButton) {
    this.hideNewDeployButton = hideNewDeployButton;
  }

  public boolean isHideNewRequestButton() {
    return hideNewRequestButton;
  }

  public void setHideNewRequestButton(boolean hideNewRequestButton) {
    this.hideNewRequestButton = hideNewRequestButton;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public Optional<String> getBaseUrl() {
    return Optional.fromNullable(Strings.emptyToNull(baseUrl));
  }

  public void setBaseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  public String getNavColor() {
    return navColor;
  }

  public void setNavColor(String navColor) {
    this.navColor = navColor;
  }

  public ROOT_URL_MODE getRootUrlMode() {
    return rootUrlMode;
  }

  public void setRootUrlMode(ROOT_URL_MODE rootUrlMode) {
    this.rootUrlMode = rootUrlMode;
  }

  public static enum ROOT_URL_MODE {
    REDIRECT_TO_UI,
    INDEX_CATCHALL,
    DISABLED
  }
}
