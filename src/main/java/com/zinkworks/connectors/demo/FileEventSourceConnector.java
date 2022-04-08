package com.zinkworks.connectors.demo;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

@Slf4j
public class FileEventSourceConnector extends SourceConnector {

  static final String WATCH_DIR = "watch.dir";
  static final String WATCH_EVENT = "watch.event";
  static final String TOPIC_NAME = "topic";
  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          WATCH_DIR, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
          "The directory to watch to an event")
      .define(
          WATCH_EVENT, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
          "A Event in disk to watch for")
      .define(
          TOPIC_NAME, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
          "The name of the topic to write to");

  private String watchDir;
  private String watchEvent;
  private String topicName;

  /**
   * Start this Connector. This method will only be called on a clean Connector, i.e. it has either
   * just been instantiated and initialized or {@link #stop()} has been invoked.
   *
   * @param props configuration settings
   */
  @Override
  public void start(Map<String, String> props) {
    log.info("FileEventSourceConnector -> start -> invoked [{}]", props);
    watchDir = props.get(WATCH_DIR);
    watchEvent = props.get(WATCH_EVENT);
    topicName = props.get(TOPIC_NAME);
  }

  /**
   * Returns the Task implementation for this Connector.
   */
  @Override
  public Class<? extends Task> taskClass() {
    return FileEventSourceTask.class;
  }

  /**
   * Returns a set of configurations for Tasks based on the current configuration, producing at most
   * count configurations.
   *
   * @param maxTasks maximum number of configurations to generate
   * @return configurations for Tasks
   */
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    Map<String, String> config = Map.of(
        WATCH_EVENT, watchEvent,
        WATCH_DIR, watchDir,
        TOPIC_NAME, topicName);

    return List.of(config);
  }

  /**
   * Stop this connector.
   */
  @Override
  public void stop() {
    log.info("FileEventSourceConnector -> stop -> invoked");
  }

  /**
   * Define the configuration for the connector.
   *
   * @return The ConfigDef for this connector; may not be null.
   */
  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  /**
   * Get the version of this component.
   *
   * @return the version, formatted as a String. The version may not be (@code null} or empty.
   */
  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }
}
