package com.zinkworks.streams;

import com.zinkworks.streams.Domain.Configuration;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.StreamSupport;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

@RequiredArgsConstructor
public class ProcessorApi {

    private final static String storeName = "total-price-store";

    private final static StoreBuilder<KeyValueStore<String, Double>> totalPriceStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(storeName),
            Serdes.String(),
            Serdes.Double());

    private final static class TotalPriceOrderProcessor implements Processor<String, ElectronicOrder, String, Double> {
        private ProcessorContext<String, Double> context;
        private KeyValueStore<String, Double> store;

        @Override
        public void init(ProcessorContext<String, Double> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
            this.context.schedule(Duration.ofSeconds(30), PunctuationType.STREAM_TIME, this::forwardAll);
            System.out.println("Processor initiated.");
        }

        @Override
        public void process(Record<String, ElectronicOrder> record) {
            final String key = record.key();
            final ElectronicOrder value = record.value();

            Double currentTotal = ofNullable(store.get(key)).orElse(0.0);
            System.out.println("current value -> " + currentTotal);

            Double newTotal = value.getPrice() + currentTotal;
            System.out.println("Price: " + value.getPrice() + " newTotal: " + newTotal);

            store.put(key, newTotal);
            System.out.println("Processed incoming record - key " + key + " value " + record.value());
        }

        private void forwardAll(final long timestamp) {
            try (KeyValueIterator<String, Double> iterator = store.all()) {
                Iterable<KeyValue<String, Double>> iterable = () -> iterator;

                StreamSupport.stream(iterable.spliterator(), false)
                        .forEach(e -> {
                            Record<String, Double> totalPriceRecord = new Record<>(e.key, e.value, timestamp);
                            context.forward(totalPriceRecord);

                            System.out.println("Punctuation forwarded. Full store (thread id: " +
                                    Thread.currentThread().getId() + "): " + e.key);

                            System.out.println("Punctuation forwarded record - key " +
                                    totalPriceRecord.key() + " value " + totalPriceRecord.value());
                        });
            }
        }
    }

    private static Map<String, ?> toConfigMap(Properties props) {
        return props.entrySet()
                .stream()
                .collect(toMap(e -> (String) e.getKey(), Entry::getValue));
    }

    private final Configuration config;

    @SuppressWarnings("resource")
    public void start() {
        final Properties streamsProps = config.getKafkaProps();

        final SpecificAvroSerde<ElectronicOrder> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(toConfigMap(streamsProps), false);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Double> doubleSerde = Serdes.Double();

        Topology topology = new Topology();
        topology
                .addSource(
                        "source-node",
                        stringSerde.deserializer(),
                        specificAvroSerde.deserializer(),
                        "input-topic")
                .addProcessor(
                        "aggregate-price",
                        TotalPriceOrderProcessor::new,
                        "source-node")
                .addStateStore(
                        totalPriceStoreBuilder,
                        "aggregate-price")
                .addSink(
                        "sink-node",
                        "output-topic",
                        stringSerde.serializer(),
                        doubleSerde.serializer(),
                        "aggregate-price");

//        StreamsBuilder builder = new StreamsBuilder();
//        KTable<String, ElectronicOrder> kTable = builder.table("input-topic",
//                Materialized.<String, ElectronicOrder, KeyValueStore<Bytes, byte[]>>as("ktable-store")
//                        .withKeySerde(stringSerde)
//                        .withValueSerde(specificAvroSerde));
//
//        kTable.mapValues(value -> value.getOrderId().substring(value.getOrderId().indexOf("-") + 1))
//                .toStream()
//                .to("output-topic", Produced.with(stringSerde, stringSerde));
//
//        new KafkaStreams(builder.build(), streamsProps);

        streamsProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        (new KafkaStreams(topology, streamsProps)).start();
    }

    public static void main(String[] args) {
        Configuration config = new Configuration();
        (new TopicLoader(config)).createTopics();
        (new ProcessorApi(config)).start();
    }
}
