package com.hubspot.singularity.logwatcher.config;

import java.util.Properties;

import com.google.common.base.Optional;
import com.hubspot.singularity.runner.base.config.SingularityConfigurationLoader;

public class SingularityLogWatcherConfigurationLoader extends SingularityConfigurationLoader {

  public static final String BYTE_BUFFER_CAPACITY = "logwatcher.bytebuffer.capacity";
  public static final String POLL_MILLIS = "logwatcher.poll.millis";
  public static final String FLUENTD_HOSTS = "logwatcher.fluentd.comma.separated.hosts.and.ports";

  public static final String STORE_DIRECTORY = "logwatcher.store.directory";
  public static final String STORE_SUFFIX = "logwatcher.store.suffix";

  public static final String RETRY_DELAY_SECONDS = "logwatcher.retry.delay.seconds";

  public static final String FLUENTD_TAG_PREFIX = "logwatcher.fluentd.tag.prefix";

  public static final String DEFAULT_PROPERTY_FILE = "/etc/singularity.logwatcher.properties";
  public static final String DEFAULT_LOG_FILE = "singularity-logwatcher.log";

  public SingularityLogWatcherConfigurationLoader() {
    super(DEFAULT_PROPERTY_FILE, Optional.of(DEFAULT_LOG_FILE));
  }

  @Override
  protected void bindDefaults(Properties properties) {
    properties.put(BYTE_BUFFER_CAPACITY, "8192");
    properties.put(POLL_MILLIS, "1000");
    properties.put(FLUENTD_HOSTS, "localhost:24224");

    properties.put(RETRY_DELAY_SECONDS, "60");

    properties.put(STORE_SUFFIX, ".store");
    properties.put(FLUENTD_TAG_PREFIX, "forward");
  }

}
