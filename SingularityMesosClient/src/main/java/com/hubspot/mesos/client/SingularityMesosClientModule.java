package com.hubspot.mesos.client;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Protocol;

public class SingularityMesosClientModule extends AbstractModule {

  public static final String SINGULARITY_MESOS_CLIENT_NAME = "singularity.mesos.client";

  @Override
  protected void configure() {
  }

  @Provides
  @Singleton
  @Named(SINGULARITY_MESOS_CLIENT_NAME)
  public OkHttpClient getOkHttpClient() {
    OkHttpClient client = new OkHttpClient()
      .setProtocols(ImmutableList.of(Protocol.HTTP_1_1));
    client.setConnectTimeout(30, SECONDS);
    client.setReadTimeout(10, SECONDS);
    client.setWriteTimeout(10, SECONDS);
    return client;
  }
}
