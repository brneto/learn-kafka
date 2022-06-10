package com.zinkworks.streams;

import com.zinkworks.streams.domain.Configuration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.UUID;
import java.util.stream.StreamSupport;

import static java.time.Duration.ofMillis;
import static java.util.stream.Collectors.toList;

public class TopicReader {

    private final Configuration appConfig = new Configuration();

    public void read(String groupId, String topicName) {
        final Thread mainThread = Thread.currentThread();
        Properties consumerProps = appConfig.getKafkaProps();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class);
        //consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        try (Consumer<String, Double> consumer = new KafkaConsumer<>(consumerProps)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Starting exit...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }));

            consumer.assign(
                    consumer.partitionsFor(topicName)
                            .stream()
                            .map(p -> new TopicPartition(p.topic(), p.partition()))
                            .collect(toList()));
            consumer.seekToBeginning(consumer.assignment());

            int totalRetries = 1;
            for (int i = 0; i <= totalRetries; i++){
                ConsumerRecords<String, Double> consumerRecords = consumer.poll(ofMillis(100));
                System.out.println(System.currentTimeMillis() + " -- waiting for data " + "[retry: " + i + "]...");

                StreamSupport.stream(consumerRecords.spliterator(), false)
                        .forEach(r -> System.out.printf(
                                "topic -> %s, partition -> %s, offset -> %s, electronicId -> %s, total -> %s\n",
                                r.topic(), r.partition(), r.offset(), r.key(), r.value()));

                consumer.assignment()
                        .forEach(tp -> System.out.println("Partition " + tp.toString() +  " Committing offset at position: " + consumer.position(tp)));

                consumer.commitSync();
            }
        } finally {
            System.out.println("Closed consumer");
        }
    }

    public static void main(String[] args) {
        TopicReader topicReader = new TopicReader();
        topicReader.read(UUID.randomUUID().toString(), "output-topic");
    }
}
