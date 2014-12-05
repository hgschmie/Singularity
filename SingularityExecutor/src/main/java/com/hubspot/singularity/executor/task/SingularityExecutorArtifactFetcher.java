package com.hubspot.singularity.executor.task;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.hubspot.deploy.EmbeddedArtifact;
import com.hubspot.deploy.ExecutorData;
import com.hubspot.deploy.RemoteArtifact;
import com.hubspot.deploy.S3Artifact;
import com.hubspot.mesos.JavaUtils;
import com.hubspot.singularity.executor.config.SingularityExecutorConfiguration;
import com.hubspot.singularity.executor.config.SingularityExecutorModule;
import com.hubspot.singularity.s3.base.ArtifactDownloadRequest;
import com.hubspot.singularity.s3.base.ArtifactManager;
import com.hubspot.singularity.s3.base.config.SingularityS3Configuration;
import com.squareup.okhttp.Callback;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

public class SingularityExecutorArtifactFetcher {

  private static final String LOCAL_DOWNLOAD_STRING_FORMAT = "http://localhost:%s%s";

  private static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");

  private final OkHttpClient localDownloadHttpClient;
  private final String localDownloadUri;
  private final SingularityExecutorConfiguration executorConfiguration;
  private final SingularityS3Configuration s3Configuration;
  private final ObjectMapper objectMapper;

  @Inject
  public SingularityExecutorArtifactFetcher(@Named(SingularityExecutorModule.LOCAL_DOWNLOAD_HTTP_CLIENT) OkHttpClient localDownloadHttpClient, SingularityS3Configuration s3Configuration,
      SingularityExecutorConfiguration executorConfiguration, ObjectMapper objectMapper) {
    this.localDownloadHttpClient = localDownloadHttpClient;
    this.executorConfiguration = executorConfiguration;
    this.s3Configuration = s3Configuration;
    this.objectMapper = objectMapper;

    this.localDownloadUri = String.format(LOCAL_DOWNLOAD_STRING_FORMAT, s3Configuration.getLocalDownloadHttpPort(), s3Configuration.getLocalDownloadPath());
  }

  public SingularityExecutorTaskArtifactFetcher buildTaskFetcher(ExecutorData executorData, SingularityExecutorTask task) {
    ArtifactManager artifactManager = new ArtifactManager(s3Configuration, task.getLog());

    return new SingularityExecutorTaskArtifactFetcher(artifactManager, executorData, task);
  }

  public class SingularityExecutorTaskArtifactFetcher {

    private final ArtifactManager artifactManager;
    private final ExecutorData executorData;
    private final SingularityExecutorTask task;

    public SingularityExecutorTaskArtifactFetcher(ArtifactManager artifactManager, ExecutorData executorData, SingularityExecutorTask task) {
      this.artifactManager = artifactManager;
      this.executorData = executorData;
      this.task = task;
    }

    public void cancel() {
      artifactManager.markKilled();
      artifactManager.destroyProcessIfActive();
    }

    public void fetchFiles() {
      extractFiles(task, artifactManager, executorData);

      boolean fetchS3ArtifactsLocally = true;

      if (executorConfiguration.isUseLocalDownloadService() && !executorData.getS3Artifacts().isEmpty()) {
        final long start = System.currentTimeMillis();

        task.getLog().info("Fetching {} (S3) artifacts from local download service", executorData.getS3Artifacts().size());

        try {
          downloadFilesFromLocalDownloadService(executorData.getS3Artifacts(), task);

          fetchS3ArtifactsLocally = false;

          task.getLog().info("Fetched {} (S3) artifacts from local download service in {}", executorData.getS3Artifacts().size(), JavaUtils.duration(start));
        } catch (Throwable t) {
          task.getLog().error("Failed downloading S3 artifacts from local download service - falling back to in-task fetch", t);
        }
      }

      if (fetchS3ArtifactsLocally) {
        for (RemoteArtifact s3Artifact : executorData.getS3Artifacts()) {
          downloadRemoteArtifact(s3Artifact, artifactManager, task);
        }
      }

      for (RemoteArtifact externalArtifact : executorData.getExternalArtifacts()) {
        downloadRemoteArtifact(externalArtifact, artifactManager, task);
      }
    }

    private void extractFiles(SingularityExecutorTask task, ArtifactManager artifactManager, ExecutorData executorData) {
      for (EmbeddedArtifact artifact : executorData.getEmbeddedArtifacts()) {
        artifactManager.extract(artifact, task.getTaskDefinition().getTaskDirectoryPath());
      }
    }

    private void downloadFilesFromLocalDownloadService(List<S3Artifact> s3Artifacts, SingularityExecutorTask task) {
      final ImmutableList.Builder<ListenableFuture<Response>> futures = ImmutableList.builder();

      for (S3Artifact s3Artifact : s3Artifacts) {
        ArtifactDownloadRequest artifactDownloadRequest = new ArtifactDownloadRequest(task.getTaskDefinition().getTaskDirectory(), s3Artifact);

        task.getLog().debug("Requesting {} from {}", artifactDownloadRequest, localDownloadUri);

        try {
          Request downloadRequest = new Request.Builder()
          .url(localDownloadUri)
          .post(RequestBody.create(MEDIA_TYPE_JSON, objectMapper.writeValueAsBytes(artifactDownloadRequest)))
          .build();


          final SettableFuture<Response> future = SettableFuture.create();
          futures.add(future);

          final Callback callback = new Callback() {
            @Override
            public void onResponse(Response response) throws IOException {
              future.set(response);
            }

            @Override
            public void onFailure(Request request, IOException e) {
              future.setException(e);
            }
          };

          localDownloadHttpClient.newCall(downloadRequest).enqueue(callback);
        } catch (JsonProcessingException e) {
          throw Throwables.propagate(e);
        }
      }

      for (ListenableFuture<Response> future : futures.build()) {
        Response response;
        try {
          response = future.get();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }

        task.getLog().debug("Future got status code {}", response.code());

        if (!response.isSuccessful()) {
          throw new IllegalStateException("Got status code:" + response.code());
        }
      }
    }

    private void downloadRemoteArtifact(RemoteArtifact remoteArtifact, ArtifactManager artifactManager, SingularityExecutorTask task) {
      Path fetched = artifactManager.fetch(remoteArtifact);

      if (fetched.getFileName().toString().endsWith(".tar.gz")) {
        artifactManager.untar(fetched, task.getTaskDefinition().getTaskDirectoryPath());
      } else {
        artifactManager.copy(fetched, task.getTaskDefinition().getTaskAppDirectoryPath());
      }
    }

  }

}
