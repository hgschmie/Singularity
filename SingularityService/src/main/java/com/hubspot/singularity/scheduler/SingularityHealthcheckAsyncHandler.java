package com.hubspot.singularity.scheduler;

import static java.lang.String.format;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.hubspot.singularity.SingularityAbort;
import com.hubspot.singularity.SingularityAbort.AbortReason;
import com.hubspot.singularity.SingularityTask;
import com.hubspot.singularity.SingularityTaskHealthcheckResult;
import com.hubspot.singularity.config.SingularityConfiguration;
import com.hubspot.singularity.data.TaskManager;
import com.squareup.okhttp.Callback;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;

public class SingularityHealthcheckAsyncHandler implements Callback {

  private static final Logger LOG = LoggerFactory.getLogger(SingularityHealthchecker.class);

  private final long startTime;
  private final SingularityHealthchecker healthchecker;
  private final SingularityNewTaskChecker newTaskChecker;
  private final SingularityTask task;
  private final TaskManager taskManager;
  private final SingularityAbort abort;
  private final int maxHealthcheckResponseBodyBytes;

  public SingularityHealthcheckAsyncHandler(SingularityConfiguration configuration, SingularityHealthchecker healthchecker, SingularityNewTaskChecker newTaskChecker, TaskManager taskManager, SingularityAbort abort, SingularityTask task) {
    this.taskManager = taskManager;
    this.newTaskChecker = newTaskChecker;
    this.healthchecker = healthchecker;
    this.abort = abort;
    this.task = task;
    this.maxHealthcheckResponseBodyBytes = configuration.getMaxHealthcheckResponseBodyBytes();

    startTime = System.currentTimeMillis();
  }

  @Override
  public void onResponse(Response response) throws IOException {
    Optional<String> responseBody = Optional.absent();

    try (ResponseBody body = response.body();
        Reader reader = new InputStreamReader(ByteStreams.limit(body.byteStream(), maxHealthcheckResponseBodyBytes), StandardCharsets.UTF_8)) {
      String result = CharStreams.toString(reader);
      responseBody = Optional.of(result);
      LOG.trace(format("Webhook response message is: '%s'", result));
    }

    saveResult(Optional.of(response.code()), responseBody, Optional.<String> absent());
  }

  @Override
  public void onFailure(Request request, IOException e) {
    LOG.trace(format("Exception while making health check for task %s", task.getTaskId()), e);

    saveResult(Optional.<Integer> absent(), Optional.<String> absent(), Optional.of(format("Healthcheck failed due to exception: %s", e.getMessage())));
  }

  public void saveResult(Optional<Integer> statusCode, Optional<String> responseBody, Optional<String> errorMessage) {
    SingularityTaskHealthcheckResult result = new SingularityTaskHealthcheckResult(statusCode, Optional.of(System.currentTimeMillis() - startTime), startTime, responseBody,
        errorMessage, task.getTaskId());

    LOG.trace("Saving healthcheck result {}", result);

    try {
      taskManager.saveHealthcheckResult(result);

      if (result.isFailed()) {
        if (!taskManager.isActiveTask(task.getTaskId().getId())) {
          LOG.trace("Task {} is not active, not re-enqueueing healthcheck", task.getTaskId());
          return;
        }

        healthchecker.enqueueHealthcheck(task);
      } else {
        newTaskChecker.runNewTaskCheckImmediately(task);
      }
    } catch (Throwable t) {
      LOG.error("Caught throwable while saving health check result {}, will re-enqueue", result, t);

      reEnqueueOrAbort(task);
    }
  }

  private void reEnqueueOrAbort(SingularityTask task) {
    try {
      healthchecker.enqueueHealthcheck(task);
    } catch (Throwable t) {
      LOG.error("Caught throwable while re-enqueuing health check for {}, aborting", task.getTaskId(), t);

      abort.abort(AbortReason.UNRECOVERABLE_ERROR);
    }
  }

}
