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

  static final String DIR_TO_WATCH = "dir.to.watch";
  static final String EVENT_TO_WATCH = "event.to.watch";
  static final String OUTPUT_TOPIC_NAME = "output.topic.name";
  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          DIR_TO_WATCH, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
          "The directory to watch to an event")
      .define(
          EVENT_TO_WATCH, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
          "A Event in disk to watch for")
      .define(
          OUTPUT_TOPIC_NAME, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
          "The name of the topic to write to");

  private String dirToWatch;
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
    dirToWatch = props.get(DIR_TO_WATCH);
    watchEvent = props.get(EVENT_TO_WATCH);
    topicName = props.get(OUTPUT_TOPIC_NAME);
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
        EVENT_TO_WATCH, watchEvent,
        DIR_TO_WATCH, dirToWatch,
        OUTPUT_TOPIC_NAME, topicName);

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
