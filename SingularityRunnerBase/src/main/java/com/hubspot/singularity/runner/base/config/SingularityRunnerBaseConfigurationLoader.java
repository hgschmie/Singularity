package com.hubspot.singularity.runner.base.config;

import java.util.Properties;

import com.google.common.base.Optional;
import com.hubspot.mesos.JavaUtils;

public class SingularityRunnerBaseConfigurationLoader extends SingularityConfigurationLoader {

  public static final String LOGGING_PATTERN = "logging.pattern";
  public static final String ROOT_LOG_DIRECTORY = "root.log.directory";
  public static final String ROOT_LOG_FILENAME = "root.log.filename";

  public static final String ROOT_LOG_LEVEL = "root.log.level";
  public static final String HUBSPOT_LOG_LEVEL = "hubspot.log.level";
  public static final String OBFUSCATE_KEYS = "obfuscate.keys.comma.separated";

  public static final String LOG_METADATA_DIRECTORY = "logwatcher.metadata.directory";
  public static final String LOG_METADATA_SUFFIX = "logwatcher.metadata.suffix";

  public static final String S3_METADATA_SUFFIX = "s3uploader.metadata.suffix";
  public static final String S3_METADATA_DIRECTORY = "s3uploader.metadata.directory";

  public static final String DEFAULT_PROPERTY_FILE = "/etc/singularity.base.properties";

  public SingularityRunnerBaseConfigurationLoader() {
    super(DEFAULT_PROPERTY_FILE, Optional.<String>absent());
  }

  @Override
  protected void bindDefaults(Properties properties) {
    properties.put(LOGGING_PATTERN, JavaUtils.LOGBACK_LOGGING_PATTERN);
    properties.put(LOG_METADATA_SUFFIX, ".tail.json");
    properties.put(ROOT_LOG_LEVEL, "INFO");
    properties.put(HUBSPOT_LOG_LEVEL, "INFO");
    properties.put(OBFUSCATE_KEYS, "key,pass,secret");
    properties.put(S3_METADATA_SUFFIX, ".s3.json");
    properties.put(S3_METADATA_DIRECTORY, "");
    properties.put(LOG_METADATA_DIRECTORY, "");
  }
}
