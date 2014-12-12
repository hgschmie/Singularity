package com.hubspot.singularity.hooks;

import io.dropwizard.lifecycle.Managed;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.hubspot.mesos.JavaUtils;
import com.hubspot.singularity.SingularityMainModule;
import com.hubspot.singularity.config.SingularityConfiguration;
import com.hubspot.singularity.sentry.SingularityExceptionNotifier;

@Singleton
public class SingularityWebhookPoller implements Managed {

  private static final Logger LOG = LoggerFactory.getLogger(SingularityWebhookPoller.class);

  private final SingularityWebhookSender webhookSender;
  private final SingularityExceptionNotifier exceptionNotifier;
  private final SingularityConfiguration configuration;
  private final ScheduledExecutorService executorService;

  @Inject
  public SingularityWebhookPoller(@Named(SingularityMainModule.CORE_THREADPOOL_NAME) ScheduledExecutorService executorService,
      SingularityWebhookSender webhookSender, SingularityExceptionNotifier exceptionNotifier, SingularityConfiguration configuration) {
    this.webhookSender = webhookSender;
    this.configuration = configuration;
    this.exceptionNotifier = exceptionNotifier;

    this.executorService = executorService;
  }

  @Override
  public void stop() {
  }

  @Override
  public void start() {
    LOG.info("Starting a webhookPoller that executes webhooks every {}", JavaUtils.durationFromMillis(configuration.getCheckWebhooksEveryMillis()));

    executorService.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        try {
          webhookSender.checkWebhooks();
        } catch (Throwable t) {
          LOG.error("Caught an unexpected exception while checking webhooks", t);
          exceptionNotifier.notify(t);
        }
      }
    }, configuration.getCheckWebhooksEveryMillis(), configuration.getCheckWebhooksEveryMillis(), TimeUnit.MILLISECONDS);
  }
}
