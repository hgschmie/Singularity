package com.hubspot.mesos.client;

import static java.lang.String.format;
import static com.hubspot.mesos.client.SingularityMesosClientModule.SINGULARITY_MESOS_CLIENT_NAME;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.hubspot.mesos.JavaUtils;
import com.hubspot.mesos.json.MesosMasterStateObject;
import com.hubspot.mesos.json.MesosSlaveStateObject;
import com.hubspot.mesos.json.MesosTaskMonitorObject;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;

@Singleton
public class MesosClient {

  private static final Logger LOG = LoggerFactory.getLogger(MesosClient.class);

  private static final String MASTER_STATE_FORMAT = "http://%s/master/state.json";
  private static final String MESOS_SLAVE_JSON_URL = "http://%s:5051/slave(1)/state.json";
  private static final String MESOS_SLAVE_STATISTICS_URL = "http://%s:5051/monitor/statistics.json";

  private static final TypeReference<List<MesosTaskMonitorObject>> TASK_MONITOR_TYPE_REFERENCE = new TypeReference<List<MesosTaskMonitorObject>>() {
  };

  private final OkHttpClient okHttpClient;
  private final ObjectMapper objectMapper;

  @Inject
  public MesosClient(@Named(SINGULARITY_MESOS_CLIENT_NAME) final OkHttpClient okHttpClient, final ObjectMapper objectMapper) {
    this.okHttpClient = okHttpClient;
    this.objectMapper = objectMapper;
  }

  public String getMasterUri(String hostnameAndPort) {
    return format(MASTER_STATE_FORMAT, hostnameAndPort);
  }

  public static class MesosClientException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public MesosClientException(String message) {
      super(message);
    }

    public MesosClientException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private Response getFromMesos(String uri) {
    Response response = null;

    final long start = System.currentTimeMillis();

    LOG.debug("Fetching {} from mesos", uri);

    try {
      response = okHttpClient.newCall(new Request.Builder().url(uri).build()).execute();

      LOG.debug("Response {} - {} after {}", response.code(), uri, JavaUtils.duration(start));
    } catch (Exception e) {
      throw new MesosClientException(format("Exception fetching %s after %s", uri, JavaUtils.duration(start)), e);
    }

    if (!response.isSuccessful()) {
      throw new MesosClientException(format("Invalid response code from %s : %s", uri, response.code()));
    }

    return response;
  }

  private <T> T getFromMesos(String uri, Class<T> clazz) {
    Response response = getFromMesos(uri);

    try (final ResponseBody body = response.body()) {
      return objectMapper.readValue(body.byteStream(), clazz);
    } catch (IOException e) {
      throw new MesosClientException(format("Couldn't deserialize %s from %s", clazz.getSimpleName(), uri), e);
    }
  }

  public MesosMasterStateObject getMasterState(String uri) {
    return getFromMesos(uri, MesosMasterStateObject.class);
  }

  public String getSlaveUri(String hostname) {
    return format(MESOS_SLAVE_JSON_URL, hostname);
  }

  public MesosSlaveStateObject getSlaveState(String uri) {
    return getFromMesos(uri, MesosSlaveStateObject.class);
  }

  public List<MesosTaskMonitorObject> getSlaveResourceUsage(String hostname) {
    final String uri = format(MESOS_SLAVE_STATISTICS_URL, hostname);

    Response response = getFromMesos(uri);

    try (final ResponseBody body = response.body()) {
      return objectMapper.readValue(body.byteStream(), TASK_MONITOR_TYPE_REFERENCE);
    } catch (IOException e) {
      throw new MesosClientException(format("Couldn't deserialize task monitor from %s", uri), e);
    }
  }
}
