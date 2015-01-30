package com.hubspot.singularity.data;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.UriBuilder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hubspot.mesos.json.MesosFileChunkObject;
import com.hubspot.mesos.json.MesosFileObject;
import com.hubspot.singularity.config.SingularityConfiguration;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;

@Singleton
public class SandboxManager {

  private final OkHttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final SingularityConfiguration configuration;

  private static final TypeReference<Collection<MesosFileObject>> MESOS_FILE_OBJECTS = new TypeReference<Collection<MesosFileObject>>() {};

  public static final String FILES_BROWSE = "/files/browse.json";
  public static final String FILES_READ = "/files/read.json";

  @Inject
  public SandboxManager(final OkHttpClient httpClient, SingularityConfiguration configuration, ObjectMapper objectMapper) {
    this.httpClient = httpClient.clone();
    this.objectMapper = objectMapper;
    this.configuration = configuration;

    this.httpClient.setConnectTimeout(this.configuration.getSandboxHttpTimeoutMillis(), TimeUnit.MILLISECONDS);
    this.httpClient.setReadTimeout(this.configuration.getSandboxHttpTimeoutMillis(), TimeUnit.MILLISECONDS);
    this.httpClient.setWriteTimeout(this.configuration.getSandboxHttpTimeoutMillis(), TimeUnit.MILLISECONDS);
  }

  public Collection<MesosFileObject> browse(HostAndPort slaveHostAndPort, String fullPath) throws IOException {
    URL url = UriBuilder.fromPath(FILES_BROWSE)
        .scheme("http")
        .host(slaveHostAndPort.getHostText()).port(slaveHostAndPort.getPort())
        .queryParam("path", fullPath)
        .build().toURL();

    Response response = httpClient.newCall(new Request.Builder().get().url(url).build()).execute();

    if (response.code() == 404) {
      return Collections.emptyList();
    }

    if (!response.isSuccessful()) {
      throw new SandboxException("Got response code %s from Mesos slave %s", response.code(), slaveHostAndPort);
    }

    try (ResponseBody body = response.body()) {
      return objectMapper.readValue(body.byteStream(), MESOS_FILE_OBJECTS);
    }
  }

  public Optional<MesosFileChunkObject> read(HostAndPort slaveHostAndPort, String fullPath, Optional<Long> offset, Optional<Long> length) throws IOException {

    UriBuilder urlBuilder = UriBuilder.fromPath(FILES_READ)
        .scheme("http")
        .host(slaveHostAndPort.getHostText()).port(slaveHostAndPort.getPort())
        .queryParam("path", fullPath);

    if (offset.isPresent()) {
      urlBuilder.queryParam("offset", offset.get());
    }

    if (length.isPresent()) {
      urlBuilder.queryParam("length", length.get());
    }

    URL url = urlBuilder.build().toURL();

    Response response = httpClient.newCall(new Request.Builder().get().url(url).build()).execute();

    if (response.code() == 404) {
      return Optional.absent();
    }

    if (!response.isSuccessful()) {
      throw new SandboxException("Got response code %s from Mesos slave %s", response.code(), slaveHostAndPort);
    }
    try (ResponseBody body = response.body()) {
      return Optional.of(objectMapper.readValue(body.byteStream(), MesosFileChunkObject.class));
    }
  }
}
