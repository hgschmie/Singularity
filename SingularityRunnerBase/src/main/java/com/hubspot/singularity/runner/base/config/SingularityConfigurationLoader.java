package com.hubspot.singularity.runner.base.config;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

public abstract class SingularityConfigurationLoader {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final Optional<String> propertyFile;
  private final Optional<String> defaultLogFileName;

  public SingularityConfigurationLoader(Optional<String> propertyFile, Optional<String> defaultLogFileName) {
    this.propertyFile = propertyFile;
    this.defaultLogFileName = defaultLogFileName;
  }

  protected SingularityConfigurationLoader(String propertyFile, Optional<String> defaultLogFileName) {
    this(Optional.of(propertyFile), defaultLogFileName);
  }

  public void bindPropertiesFile(Properties properties) {
    if (propertyFile.isPresent()) {
      try (BufferedReader br = Files.newBufferedReader(Paths.get(propertyFile.get()), Charset.defaultCharset())) {
        properties.load(br);
      } catch (NoSuchFileException nsfe) {
        logger.debug("Could not load property file '{}', falling back to defaults!", propertyFile);
      } catch (Throwable t) {
        throw Throwables.propagate(t);
      }
    }
  }

  public void bindAllDefaults(Properties properties) {
    if (defaultLogFileName.isPresent()) {
      properties.put(SingularityRunnerBaseConfigurationLoader.ROOT_LOG_FILENAME, defaultLogFileName.get());
    }

    bindDefaults(properties);
  }

  protected abstract void bindDefaults(Properties properties);

}
