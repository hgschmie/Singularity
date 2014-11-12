package com.hubspot.singularity.resources;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.hubspot.singularity.config.SingularityConfiguration;
import com.hubspot.singularity.views.IndexView;

/**
 * Serves as the base for the UI, returns the mustache view for the actual GUI.
 */
@Singleton
@Path(UiResource.UI_PREFIX + "{uiPath:.*}")
public class UiResource {

  static final String UI_PREFIX = "/ui/";

  private final SingularityConfiguration configuration;

  @Inject
  public UiResource(SingularityConfiguration configuration) {
    this.configuration = configuration;
  }

  @GET
  @Timed
  @ExceptionMetered
  @Produces(MediaType.TEXT_HTML)
  public IndexView getIndex() {
    return new IndexView(UI_PREFIX, configuration);
  }
}
