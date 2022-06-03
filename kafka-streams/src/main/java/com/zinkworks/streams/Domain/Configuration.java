package com.zinkworks.streams.Domain;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.lang.String.join;
import static java.util.Optional.of;

public class Configuration {
    private final List<Topic> topics = new ArrayList<>();
    private final Properties kafkaProps = new Properties();

    private final TaskConfig taskConfig;

    public Configuration() {
        Config appConfig = ConfigFactory.parseResources("application.conf").resolve();
        configTopics(appConfig);
        configKafka(appConfig);
        this.taskConfig = new TaskConfig(
                appConfig.getInt("task-config.\"schema.registry.retries.num\""),
                appConfig.getDuration("task-config.\"topic.creation.timeout\""),
                appConfig.getDuration("task-config.\"schema.registry.retries.interval\"")
        );
    }

    private void configKafka(Config config) {
        Config kConfig = config.getConfig("kafka-config");
        kafkaProps.putAll(
                kConfig.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Entry::getKey, e -> kConfig.getAnyRef(e.getKey()))));
    }

    private void configTopics(Config config) {
        BiFunction<String, String, String> joiner =
                (t, u) -> join(".", "application", t, u);

        String topicName = "input-topic";
        topics.add(new Topic(
                config.getString(joiner.apply(topicName, "name")),
                of(config.getInt(joiner.apply(topicName, "partitions"))),
                of((short) config.getInt(joiner.apply(topicName, "replication-factor")))));

        topicName = "output-topic";
        topics.add(new Topic(
                config.getString(joiner.apply(topicName, "name")),
                of(config.getInt(joiner.apply(topicName, "partitions"))),
                of((short) config.getInt(joiner.apply(topicName, "replication-factor")))));
    }

    public List<Topic> getTopics() {
        return this.topics;
    }

    public Properties getKafkaProps() {
        return this.kafkaProps;
    }

    public TaskConfig getTaskConfig() {
        return this.taskConfig;
    }

    @RequiredArgsConstructor
    @Getter
    public static class TaskConfig {
        private final int schemaRegistryRetries;
        private final Duration topicCreationTimeout;
        private final Duration schemaRegistryRetriesInterval;
    }
}
