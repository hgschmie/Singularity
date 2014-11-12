package com.hubspot.singularity.resources;

import java.net.URI;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;

@Path("/")
@Produces(MediaType.TEXT_HTML)
public class IndexResource {
  private static final Logger LOG = LoggerFactory.getLogger(IndexResource.class);

  @Inject
  public IndexResource() {
  }

  @GET
  @Timed
  @ExceptionMetered
  @Path("/")
  public Response getIndex(@Context UriInfo info) {
    LOG.info("Requesting " + info.getPath());
    return Response.status(Status.MOVED_PERMANENTLY).location(URI.create(UiResource.UI_PREFIX)).build();
  }
}
