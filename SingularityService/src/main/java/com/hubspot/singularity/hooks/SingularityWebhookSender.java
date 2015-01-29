package com.hubspot.singularity.hooks;

import static java.lang.String.format;

import static com.google.common.net.MediaType.JSON_UTF_8;

import java.io.IOException;
import java.util.List;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.hubspot.mesos.JavaUtils;
import com.hubspot.singularity.SingularityDeployUpdate;
import com.hubspot.singularity.SingularityRequestHistory;
import com.hubspot.singularity.SingularityTask;
import com.hubspot.singularity.SingularityTaskHistoryUpdate;
import com.hubspot.singularity.SingularityTaskWebhook;
import com.hubspot.singularity.SingularityWebhook;
import com.hubspot.singularity.config.SingularityConfiguration;
import com.hubspot.singularity.data.WebhookManager;
import com.hubspot.singularity.data.history.TaskHistoryHelper;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;

@Singleton
public class SingularityWebhookSender {

  private static final Logger LOG = LoggerFactory.getLogger(SingularityWebhookSender.class);

  private static final MediaType JSON_MEDIATYPE = MediaType.parse(JSON_UTF_8.toString());

  private final SingularityConfiguration configuration;
  private final OkHttpClient httpClient;
  private final WebhookManager webhookManager;
  private final TaskHistoryHelper taskHistoryHelper;
  private final ObjectMapper objectMapper;

  @Inject
  public SingularityWebhookSender(SingularityConfiguration configuration, OkHttpClient httpClient, ObjectMapper objectMapper, TaskHistoryHelper taskHistoryHelper, WebhookManager webhookManager) {
    this.configuration = configuration;
    this.httpClient = httpClient;
    this.webhookManager = webhookManager;
    this.taskHistoryHelper = taskHistoryHelper;
    this.objectMapper = objectMapper;
  }

  public void checkWebhooks() {
    final long start = System.currentTimeMillis();

    final List<SingularityWebhook> webhooks = webhookManager.getActiveWebhooks();
    if (webhooks.isEmpty()) {
      return;
    }

    int taskUpdates = 0;
    int requestUpdates = 0;
    int deployUpdates = 0;

    for (SingularityWebhook webhook : webhooks) {

      switch (webhook.getType()) {
        case TASK:
          taskUpdates += checkTaskUpdates(webhook);
          break;
        case REQUEST:
          requestUpdates += checkRequestUpdates(webhook);
          break;
        case DEPLOY:
          deployUpdates += checkDeployUpdates(webhook);
          break;
        default:
          break;
      }
    }

    LOG.info(format("Sent %d task, %d request, and %d deploy updates for %d webhooks in %s", taskUpdates, requestUpdates, deployUpdates, webhooks.size(), JavaUtils.duration(start)));
  }

  private int checkRequestUpdates(SingularityWebhook webhook) {
    final List<SingularityRequestHistory> requestUpdates = webhookManager.getQueuedRequestHistoryForHook(webhook.getId());

    int numRequestUpdates = 0;

    for (SingularityRequestHistory requestUpdate : requestUpdates) {
      executeWebhook(webhook, requestUpdate, new SingularityRequestWebhookAsyncHandler(webhookManager, webhook, requestUpdate, numRequestUpdates++ > configuration.getMaxQueuedUpdatesPerWebhook()));
    }

    return requestUpdates.size();
  }

  private int checkDeployUpdates(SingularityWebhook webhook) {
    final List<SingularityDeployUpdate> deployUpdates = webhookManager.getQueuedDeployUpdatesForHook(webhook.getId());

    int numDeployUpdates = 0;

    for (SingularityDeployUpdate deployUpdate : deployUpdates) {
      executeWebhook(webhook, deployUpdate, new SingularityDeployWebhookAsyncHandler(webhookManager, webhook, deployUpdate, numDeployUpdates++ > configuration.getMaxQueuedUpdatesPerWebhook()));
    }

    return deployUpdates.size();
  }

  private int checkTaskUpdates(SingularityWebhook webhook) {
    final List<SingularityTaskHistoryUpdate> taskUpdates = webhookManager.getQueuedTaskUpdatesForHook(webhook.getId());

    int numTaskUpdates = 0;

    for (SingularityTaskHistoryUpdate taskUpdate : taskUpdates) {
      Optional<SingularityTask> task = taskHistoryHelper.getTask(taskUpdate.getTaskId());

      // TODO compress
      if (!task.isPresent()) {
        LOG.warn(format("Couldn't find task for taskUpdate %s", taskUpdate));
        webhookManager.deleteTaskUpdate(webhook, taskUpdate);
      }

      executeWebhook(webhook, new SingularityTaskWebhook(task.get(), taskUpdate), new SingularityTaskWebhookAsyncHandler(webhookManager, webhook, taskUpdate, numTaskUpdates++ > configuration.getMaxQueuedUpdatesPerWebhook()));
    }

    return taskUpdates.size();
  }

  // TODO handle retries, errors.
  private <T> void executeWebhook(SingularityWebhook webhook, Object payload, AbstractSingularityWebhookAsyncHandler<T> handler) {
    LOG.trace(format("Sending '%s' to %s", payload, webhook.getUri()));

    try {
      RequestBody requestBody = RequestBody.create(JSON_MEDIATYPE, objectMapper.writeValueAsBytes(payload));
      Request req = new Request.Builder().post(requestBody).url(webhook.getUri()).build();

      httpClient.newCall(req).enqueue(handler);
    }
    catch (IOException e) {
      LOG.trace(format("Could not send request to %s", webhook.getUri()), e);
      if (handler.shouldDeleteUpdateDueToQueueAboveCapacity()) {
        handler.deleteWebhookUpdate();
      }
    }
  }
}
