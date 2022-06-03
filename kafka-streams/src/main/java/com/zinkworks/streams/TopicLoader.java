package com.zinkworks.streams;

import com.zinkworks.streams.Domain.Configuration;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

@Slf4j
@RequiredArgsConstructor
public class TopicLoader {

    private final Configuration config;

    public void createTopics() {
        try (Admin admin = Admin.create(config.getKafkaProps())) {
            StringJoiner topicNames = new StringJoiner(",");
            List<NewTopic> topics = config.getTopics()
                    .stream()
                    .map(t -> {
                        topicNames.add(t.getName());
                        return new NewTopic(t.getName(), t.getPartitions(), t.getReplicationFactor());
                    })
                    .collect(toList());
            log.info("Starting the topics creation for: {}", topicNames);

            CreateTopicsResult createTopicsResult = admin.createTopics(topics);
            createTopicsResult.values().forEach(
                    (topicName, kFuture) -> kFuture.whenComplete((vd, throwable) -> {
                        if (throwable != null) {
                            log.warn("Topic creation didn't complete.", throwable);
                        } else {
                            log.info("Topic {} successfully created", topicName);
                        }
                    })
            );

            Duration wait = config.getTaskConfig().getTopicCreationTimeout();
            try {
                createTopicsResult.all().get(wait.getSeconds(), SECONDS);
                log.info("Topic creation stage completed.");
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    log.info("Topic creation stage completed. (Topics already created)");
                } else {
                    log.error("The topic creation failed to complete", e);
                    throw new RuntimeException(e);
                }
            } catch (Exception e) {
                log.error("The following exception occurred during the topic creation", e);
                throw new RuntimeException(e);
            }
        }
    }

    private static Callback callback() {
        return (metadata, exception) -> {
            if(exception != null) {
                System.out.printf("Producing records encountered error %s %n", exception);
            } else {
                System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
            }

        };
    }

    private List<ElectronicOrder> getOrders(Instant instant) {
        ElectronicOrder electronicOrder1 = ElectronicOrder.newBuilder()
                .setElectronicId("PROJ-233")
                .setOrderId("instore-1")
                .setUserId("10261998")
                .setPrice(1000.00)
                .setTime(instant.toEpochMilli()).build();

        instant = instant.plusSeconds(35L);
        ElectronicOrder electronicOrder2 = ElectronicOrder.newBuilder()
                .setElectronicId("PROJ-233")
                .setOrderId("instore-1")
                .setUserId("1033737373")
                .setPrice(2000.00)
                .setTime(instant.toEpochMilli()).build();

        instant = instant.plusSeconds(35L);
        ElectronicOrder electronicOrder3 = ElectronicOrder.newBuilder()
                .setElectronicId("PROJ-233")
                .setOrderId("instore-1")
                .setUserId("1026333")
                .setPrice(3000.00)
                .setTime(instant.toEpochMilli()).build();

        instant = instant.plusSeconds(35L);
        ElectronicOrder electronicOrder4 = ElectronicOrder.newBuilder()
                .setElectronicId("PROJ-233")
                .setOrderId("instore-1")
                .setUserId("1038884844")
                .setPrice(4000.00)
                .setTime(instant.toEpochMilli()).build();

        instant = instant.plusSeconds(35L);
        ElectronicOrder electronicOrder5 = ElectronicOrder.newBuilder()
                .setElectronicId("HDTV-2333")
                .setOrderId("instore-1")
                .setUserId("10261998")
                .setPrice(2000.00)
                .setTime(instant.toEpochMilli()).build();

        instant = instant.plusSeconds(35L);
        ElectronicOrder electronicOrder6 = ElectronicOrder.newBuilder()
                .setElectronicId("HDTV-2333")
                .setOrderId("instore-1")
                .setUserId("1033737373")
                .setPrice(1999.23)
                .setTime(instant.toEpochMilli()).build();

        instant = instant.plusSeconds(35L);
        ElectronicOrder electronicOrder7 = ElectronicOrder.newBuilder()
                .setElectronicId("HDTV-2333")
                .setOrderId("instore-1")
                .setUserId("1026333")
                .setPrice(4500.00)
                .setTime(instant.toEpochMilli()).build();

        instant = instant.plusSeconds(35L);
        ElectronicOrder electronicOrder8 = ElectronicOrder.newBuilder()
                .setElectronicId("HDTV-2333")
                .setOrderId("instore-1")
                .setUserId("1038884844")
                .setPrice(1333.58)
                .setTime(instant.toEpochMilli()).build();

        return new ArrayList<ElectronicOrder>() {{
            add(electronicOrder1);
            add(electronicOrder2);
            add(electronicOrder3);
            add(electronicOrder4);
            add(electronicOrder5);
            add(electronicOrder6);
            add(electronicOrder7);
            add(electronicOrder8);
        }};
    }

    public void loadTopic() {
        Properties producerProps = config.getKafkaProps();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        try (Producer<String, ElectronicOrder> producer = new KafkaProducer<>(producerProps)) {
            getOrders(Instant.now()).forEach((electronicOrder -> {
                ProducerRecord<String, ElectronicOrder> producerRecord = new ProducerRecord<>("input-topic",
                        electronicOrder.getElectronicId().length()%2,
                        electronicOrder.getTime(),
                        electronicOrder.getElectronicId(),
                        electronicOrder);

                System.out.println("Sending record ->" + producerRecord);
                producer.send(producerRecord, callback());
            }));
        }
    }

    public static void main(String[] args) {
        (new TopicLoader(new Configuration())).loadTopic();
    }
}
