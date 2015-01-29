package com.hubspot.singularity.hooks;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hubspot.mesos.JavaUtils;
import com.hubspot.singularity.SingularityWebhook;
import com.squareup.okhttp.Callback;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;

public abstract class AbstractSingularityWebhookAsyncHandler<T> implements Callback  {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSingularityWebhookAsyncHandler.class);

  protected final SingularityWebhook webhook;
  protected final T update;
  private final long start;
  private final boolean shouldDeleteUpdateOnFailure;

  public AbstractSingularityWebhookAsyncHandler(SingularityWebhook webhook, T update, boolean shouldDeleteUpdateOnFailure) {
    this.webhook = webhook;
    this.update = update;
    this.shouldDeleteUpdateOnFailure = shouldDeleteUpdateOnFailure;

    this.start = System.currentTimeMillis();
  }

  @Override
  public void onFailure(Request req, IOException e) {
    LOG.trace("Webhook {} for {} failed after {}", webhook.getUri(), update, JavaUtils.duration(start), e);

    if (shouldDeleteUpdateOnFailure) {
      deleteWebhookUpdate();
    }
  }

  public boolean shouldDeleteUpdateDueToQueueAboveCapacity() {
    return shouldDeleteUpdateOnFailure;
  }

  @Override
  public void onResponse (Response response) throws IOException {
    LOG.trace("Webhook {} for {} completed with {} after {}", webhook.getUri(), update, response.code(), JavaUtils.duration(start));

    try (ResponseBody body = response.body()) {
      String result = body.string();
      LOG.trace("Webhook response message is: '{}'", result);
    }

    if (response.isSuccessful() || shouldDeleteUpdateOnFailure) {
      deleteWebhookUpdate();
    }
  }

  public abstract void deleteWebhookUpdate();
}
