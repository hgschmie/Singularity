package com.hubspot.singularity.executor.config;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.hubspot.singularity.executor.handlebars.BashEscapedHelper;
import com.hubspot.singularity.executor.handlebars.IfPresentHelper;
import com.squareup.okhttp.OkHttpClient;

public class SingularityExecutorModule extends AbstractModule {

  public static final String RUNNER_TEMPLATE = "runner.sh";
  public static final String ENVIRONMENT_TEMPLATE = "deploy.env";
  public static final String LOGROTATE_TEMPLATE = "logrotate.conf";
  public static final String LOCAL_DOWNLOAD_HTTP_CLIENT = "SingularityExecutorModule.local.download.http.client";

  @Override
  protected void configure() {}

  @Provides
  @Singleton
  @Named(LOCAL_DOWNLOAD_HTTP_CLIENT)
  public OkHttpClient providesHttpClient(SingularityExecutorConfiguration configuration) {
      OkHttpClient client = new OkHttpClient();
      client.setConnectTimeout(configuration.getLocalDownloadServiceTimeoutMillis(), TimeUnit.MILLISECONDS);
      return client;
  }

  @Provides
  @Singleton
  @Named(RUNNER_TEMPLATE)
  public Template providesRunnerTemplate(Handlebars handlebars) throws IOException {
    return handlebars.compile(RUNNER_TEMPLATE);
  }

  @Provides
  @Singleton
  @Named(ENVIRONMENT_TEMPLATE)
  public Template providesEnvironmentTemplate(Handlebars handlebars) throws IOException {
    return handlebars.compile(ENVIRONMENT_TEMPLATE);
  }

  @Provides
  @Singleton
  @Named(LOGROTATE_TEMPLATE)
  public Template providesLogrotateTemplate(Handlebars handlebars) throws IOException {
    return handlebars.compile(LOGROTATE_TEMPLATE);
  }

  @Provides
  @Singleton
  public Handlebars providesHandlebars() {
    final Handlebars handlebars = new Handlebars();

    handlebars.registerHelper(BashEscapedHelper.NAME, new BashEscapedHelper());
    handlebars.registerHelper(IfPresentHelper.NAME, new IfPresentHelper());

    return handlebars;
  }

}
