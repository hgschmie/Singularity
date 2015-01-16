package com.hubspot.singularity.resources;

import static com.hubspot.singularity.WebExceptions.checkForbidden;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.hubspot.singularity.SingularityAbort;
import com.hubspot.singularity.SingularityAbort.AbortReason;
import com.hubspot.singularity.SingularityLeaderController;
import com.hubspot.singularity.SingularityService;
import com.hubspot.singularity.config.SingularityConfiguration;
import com.hubspot.singularity.mesos.SingularityDriver;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Path(TestResource.PATH)
@Api(description="Misc testing endpoints.", value=TestResource.PATH)
public class TestResource {
  public static final String PATH = SingularityService.API_BASE_PATH + "/test";

  private final SingularityAbort abort;
  private final SingularityLeaderController managed;
  private final SingularityConfiguration configuration;
  private final SingularityDriver driver;

  @Inject
  public TestResource(SingularityConfiguration configuration, SingularityLeaderController managed, SingularityAbort abort, final SingularityDriver driver) {
    this.configuration = configuration;
    this.managed = managed;
    this.abort = abort;
    this.driver = driver;
  }

  @POST
  @Timed
  @ExceptionMetered
  @Path("/scheduler/statusUpdate/{taskId}/{taskState}")
  @ApiOperation("Force an update for a specific task.")
  public void statusUpdate(@PathParam("taskId") String taskId, @PathParam("taskState") String taskState) {
    checkForbidden(configuration.isAllowTestResourceCalls(), "Test resource calls are disabled (set isAllowTestResourceCalls to true in configuration)");

    driver.getScheduler().statusUpdate(null, TaskStatus.newBuilder()
        .setTaskId(TaskID.newBuilder().setValue(taskId))
        .setState(TaskState.valueOf(taskState))
        .build());
  }

  @POST
  @Timed
  @ExceptionMetered
  @Path("/leader")
  @ApiOperation("Make this instance of Singularity believe it's elected leader.")
  public void setLeader() {
    checkForbidden(configuration.isAllowTestResourceCalls(), "Test resource calls are disabled (set isAllowTestResourceCalls to true in configuration)");

    managed.isLeader();
  }

  @POST
  @Timed
  @ExceptionMetered
  @Path("/notleader")
  @ApiOperation("Make this instance of Singularity believe it's lost leadership.")
  public void setNotLeader() {
    checkForbidden(configuration.isAllowTestResourceCalls(), "Test resource calls are disabled (set isAllowTestResourceCalls to true in configuration)");

    managed.notLeader();
  }

  @POST
  @Timed
  @ExceptionMetered
  @Path("/stop")
  @ApiOperation("Stop the Mesos scheduler driver.")
  public void stop() throws Exception {
    checkForbidden(configuration.isAllowTestResourceCalls(), "Test resource calls are disabled (set isAllowTestResourceCalls to true in configuration)");

    managed.stop();
  }

  @POST
  @Timed
  @ExceptionMetered
  @Path("/abort")
  @ApiOperation("Abort the Mesos scheduler driver.")
  public void abort() {
    checkForbidden(configuration.isAllowTestResourceCalls(), "Test resource calls are disabled (set isAllowTestResourceCalls to true in configuration)");

    abort.abort(AbortReason.TEST_ABORT);
  }

  @POST
  @Timed
  @ExceptionMetered
  @Path("/start")
  @ApiOperation("Start the Mesos scheduler driver.")
  public void start() throws Exception {
    checkForbidden(configuration.isAllowTestResourceCalls(), "Test resource calls are disabled (set isAllowTestResourceCalls to true in configuration)");

    managed.start();
  }
}
