package com.hubspot.singularity.hooks;

import static com.google.common.base.Preconditions.checkState;
import static com.hubspot.baragon.models.BaragonRequestState.FAILED;
import static com.hubspot.baragon.models.BaragonRequestState.UNKNOWN;
import static com.hubspot.singularity.SingularityLoadBalancerUpdate.LoadBalancerMethod.CANCEL;
import static com.hubspot.singularity.SingularityLoadBalancerUpdate.LoadBalancerMethod.CHECK_STATE;
import static com.hubspot.singularity.SingularityLoadBalancerUpdate.LoadBalancerMethod.ENQUEUE;
import static java.lang.String.format;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.hubspot.baragon.models.BaragonRequest;
import com.hubspot.baragon.models.BaragonRequestState;
import com.hubspot.baragon.models.BaragonResponse;
import com.hubspot.baragon.models.BaragonService;
import com.hubspot.baragon.models.UpstreamInfo;
import com.hubspot.mesos.JavaUtils;
import com.hubspot.singularity.LoadBalancerRequestType.LoadBalancerRequestId;
import com.hubspot.singularity.SingularityDeploy;
import com.hubspot.singularity.SingularityLoadBalancerUpdate;
import com.hubspot.singularity.SingularityLoadBalancerUpdate.LoadBalancerMethod;
import com.hubspot.singularity.SingularityRequest;
import com.hubspot.singularity.SingularityTask;
import com.hubspot.singularity.config.BaragonConfiguration;
import com.hubspot.singularity.config.SingularityConfiguration;
import com.hubspot.singularity.data.transcoders.SingularityTranscoderException;
import com.hubspot.singularity.data.transcoders.Transcoder;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Request;
import com.ning.http.client.Response;

public class BaragonLoadBalancerClientImpl implements LoadBalancerClient {

  private static final Logger LOG = LoggerFactory.getLogger(LoadBalancerClient.class);

  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final String HEADER_CONTENT_TYPE = "Content-Type";

  private final Optional<BaragonConfiguration> baragonConfiguration;

  private final AsyncHttpClient httpClient;
  private final Transcoder<BaragonResponse> baragornResponseTranscoder;
  private final Transcoder<BaragonRequest> baragornRequestTranscoder;

  private static final String OPERATION_URI = "%s/%s";

  @Inject
  public BaragonLoadBalancerClientImpl(SingularityConfiguration configuration,
      Transcoder<BaragonRequest> baragornRequestTranscoder,
      Transcoder<BaragonResponse> baragornResponseTranscoder,
      AsyncHttpClient httpClient) {
    this.httpClient = httpClient;
    this.baragornRequestTranscoder = baragornRequestTranscoder;
    this.baragornResponseTranscoder = baragornResponseTranscoder;
    this.baragonConfiguration = configuration.getBaragonConfiguration();
  }

  @Override
  public boolean isActive() {
    return baragonConfiguration.isPresent();
  }

  @Override
  public SingularityLoadBalancerUpdate getState(LoadBalancerRequestId loadBalancerRequestId) {
    if (!baragonConfiguration.isPresent()) {
      return unknownBaragonState(loadBalancerRequestId, CHECK_STATE);
    }

    final String uri = getLoadBalancerUri(loadBalancerRequestId);

    final BoundRequestBuilder requestBuilder = httpClient.prepareGet(uri);
    addAllQueryParams(requestBuilder);

    return sendRequest(loadBalancerRequestId, CHECK_STATE, requestBuilder.build(), UNKNOWN);
  }

  @Override
  public SingularityLoadBalancerUpdate enqueue(LoadBalancerRequestId loadBalancerRequestId, SingularityRequest request, SingularityDeploy deploy, List<SingularityTask> add,
      List<SingularityTask> remove) {

    if (!baragonConfiguration.isPresent()) {
      return unknownBaragonState(loadBalancerRequestId, ENQUEUE);
    }

    final List<String> serviceOwners = request.getOwners().or(Collections.<String> emptyList());
    final List<String> loadBalancerGroups = deploy.getLoadBalancerGroups().or(Collections.<String> emptyList());
    final BaragonService lbService = new BaragonService(request.getId(), serviceOwners, deploy.getServiceBasePath().get(), loadBalancerGroups, deploy.getLoadBalancerOptions().orNull());

    final List<UpstreamInfo> addUpstreams = tasksToUpstreams(add, loadBalancerRequestId.toString());
    final List<UpstreamInfo> removeUpstreams = tasksToUpstreams(remove, loadBalancerRequestId.toString());

    final BaragonRequest loadBalancerRequest = new BaragonRequest(loadBalancerRequestId.toString(), lbService, addUpstreams, removeUpstreams);

    final String uri = getLoadBalancerUri(loadBalancerRequestId);

    try {
      LOG.trace("Deploy {} is preparing to send {}", deploy.getId(), loadBalancerRequest);

      final BoundRequestBuilder requestBuilder = httpClient.preparePost(uri)
          .addHeader(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON)
          .setBody(baragornRequestTranscoder.toBytes(loadBalancerRequest));

      addAllQueryParams(requestBuilder);

      return sendRequest(loadBalancerRequestId, ENQUEUE, requestBuilder.build(), FAILED);
    } catch (SingularityTranscoderException e) {
      return new SingularityLoadBalancerUpdate(UNKNOWN, loadBalancerRequestId, Optional.of(e.getMessage()), System.currentTimeMillis(), ENQUEUE, Optional.of(uri));
    }
  }

  @Override
  public SingularityLoadBalancerUpdate cancel(LoadBalancerRequestId loadBalancerRequestId) {
    if (!baragonConfiguration.isPresent()) {
      return unknownBaragonState(loadBalancerRequestId, CANCEL);
    }

    final String uri = getLoadBalancerUri(loadBalancerRequestId);

    final BoundRequestBuilder requestBuilder = httpClient.prepareDelete(uri);

    addAllQueryParams(requestBuilder);

    return sendRequest(loadBalancerRequestId, CANCEL, requestBuilder.build(), UNKNOWN);
  }


  private String getLoadBalancerUri(LoadBalancerRequestId loadBalancerRequestId) {
    checkState(baragonConfiguration.isPresent(), "can not call getLoadBalancerUri without configuration present!");
    return String.format(OPERATION_URI, baragonConfiguration.get().getLoadBalancerUri(), loadBalancerRequestId);
  }

  private void addAllQueryParams(BoundRequestBuilder boundRequestBuilder) {
    checkState(baragonConfiguration.isPresent(), "can not call addAllQueryParams without configuration present!");

    Optional<Map<String, String>> queryParams = baragonConfiguration.get().getLoadBalancerQueryParams();

    for (Map.Entry<String, String> entry : queryParams.get().entrySet()) {
      boundRequestBuilder.addQueryParameter(entry.getKey(), entry.getValue());
    }
  }

  private SingularityLoadBalancerUpdate unknownBaragonState(LoadBalancerRequestId loadBalancerRequestId, LoadBalancerMethod loadBalancerMethod) {
    return new SingularityLoadBalancerUpdate(UNKNOWN, loadBalancerRequestId, Optional.of("baragon not configured"), System.currentTimeMillis(), loadBalancerMethod, Optional.<String>absent());
  }

  private List<UpstreamInfo> tasksToUpstreams(List<SingularityTask> tasks, String requestId) {
    final List<UpstreamInfo> upstreams = Lists.newArrayListWithCapacity(tasks.size());

    for (SingularityTask task : tasks) {
      final Optional<Long> maybeFirstPort = task.getFirstPort();

      if (maybeFirstPort.isPresent()) {
        String upstream = String.format("%s:%d", task.getOffer().getHostname(), maybeFirstPort.get());
        String rackId = task.getTaskId().getRackId();

        upstreams.add(new UpstreamInfo(upstream, Optional.of(requestId), Optional.fromNullable(rackId)));
      } else {
        LOG.warn("Task {} is missing port but is being passed to LB  ({})", task.getTaskId(), task);
      }
    }

    return upstreams;
  }

  private SingularityLoadBalancerUpdate sendRequest(LoadBalancerRequestId loadBalancerRequestId, LoadBalancerMethod method, Request request, BaragonRequestState failureState) {
    checkState(baragonConfiguration.isPresent(), "can not call sendRequest without configuration present!");

    final long loadBalancerTimeoutMillis = baragonConfiguration.get().getLoadBalancerRequestTimeoutMillis();

    final long start = System.currentTimeMillis();

    BaragonRequestState state = failureState;
    Optional<String> message = Optional.absent();

    try {
      LOG.trace("Sending LB {} request for {} to {}", request.getMethod(), loadBalancerRequestId, request.getUrl());

      ListenableFuture<Response> future = httpClient.executeRequest(request);

      Response response = future.get(loadBalancerTimeoutMillis, TimeUnit.MILLISECONDS);

      LOG.trace("LB {} request {} returned with code {}", request.getMethod(), loadBalancerRequestId, response.getStatusCode());

      if (!JavaUtils.isHttpSuccess(response.getStatusCode())) {
        message = Optional.of(format("Response status code %s", response.getStatusCode()));
      }
      else {
        BaragonResponse lbResponse = baragornResponseTranscoder.fromBytes(response.getResponseBodyAsBytes());
        state = lbResponse.getLoadBalancerState();
        message = lbResponse.getMessage();
      }
    } catch (TimeoutException te) {
      LOG.trace("LB {} request {} timed out after waiting {}", request.getMethod(), loadBalancerRequestId, JavaUtils.durationFromMillis(loadBalancerTimeoutMillis));
      message = Optional.of(String.format("Timed out after %s", JavaUtils.durationFromMillis(loadBalancerTimeoutMillis)));
    } catch (Throwable t) {
      LOG.error("LB {} request {} to {} threw error", request.getMethod(), loadBalancerRequestId, request.getUrl(), t);
      message = Optional.of(String.format("Exception %s - %s", t.getClass().getSimpleName(), t.getMessage()));
    }

    LOG.debug("LB {} request {} had state={}, message={} after {}", request.getMethod(), loadBalancerRequestId, state, message, JavaUtils.duration(start));
    return new SingularityLoadBalancerUpdate(state, loadBalancerRequestId, message, start, method, Optional.of(request.getUrl()));
  }

}
