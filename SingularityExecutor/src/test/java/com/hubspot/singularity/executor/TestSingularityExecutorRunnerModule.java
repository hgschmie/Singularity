package com.hubspot.singularity.executor;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.nio.file.Files;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.hubspot.singularity.executor.config.SingularityExecutorConfigurationLoader;
import com.hubspot.singularity.runner.base.config.SingularityConfigurationLoader;
import com.hubspot.singularity.runner.base.config.SingularityRunnerBaseConfigurationLoader;
import com.hubspot.singularity.s3.base.config.SingularityS3ConfigurationLoader;

public class TestSingularityExecutorRunnerModule {

  private File directory;

  @Before
  public void setUp() throws Exception {
    directory = Files.createTempDirectory("singularity-test").toFile();
    directory.deleteOnExit();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testSpinup() throws Exception {

    final SingularityConfigurationLoader testLoader = new SingularityConfigurationLoader(Optional.<String> absent(), Optional.<String> absent()) {

      @Override
      protected void bindDefaults(Properties properties) {
        properties.put(SingularityRunnerBaseConfigurationLoader.ROOT_LOG_DIRECTORY, directory.getAbsolutePath());
        properties.put(SingularityRunnerBaseConfigurationLoader.LOG_METADATA_DIRECTORY, directory.getAbsolutePath());
        properties.put(SingularityRunnerBaseConfigurationLoader.S3_METADATA_DIRECTORY, directory.getAbsolutePath());
        properties.put(SingularityRunnerBaseConfigurationLoader.ROOT_LOG_FILENAME, "singularity.log");

        properties.put(SingularityExecutorConfigurationLoader.DEFAULT_USER, "unknown");
        properties.put(SingularityExecutorConfigurationLoader.GLOBAL_TASK_DEFINITION_DIRECTORY, directory.getAbsolutePath());
        properties.put(SingularityExecutorConfigurationLoader.S3_UPLOADER_PATTERN, ".*");
        properties.put(SingularityExecutorConfigurationLoader.S3_UPLOADER_BUCKET, "singularity_log_bucket");
        properties.put(SingularityExecutorConfigurationLoader.LOGROTATE_CONFIG_DIRECTORY, directory.getAbsolutePath());

        properties.put(SingularityS3ConfigurationLoader.ARTIFACT_CACHE_DIRECTORY, directory.getAbsolutePath());
      }
    };

    SingularityConfigurationLoader[] configurationLoaders = new SingularityConfigurationLoader[] {
        new SingularityS3ConfigurationLoader(Optional.<String>absent()),
        new SingularityExecutorConfigurationLoader(Optional.<String>absent()),
        testLoader
      };

    final Injector injector = Guice.createInjector(Stage.PRODUCTION, new SingularityExecutorRunnerModule(configurationLoaders));
    final SingularityExecutorRunner executorRunner = injector.getInstance(SingularityExecutorRunner.class);
    assertNotNull(executorRunner);
  }
}
