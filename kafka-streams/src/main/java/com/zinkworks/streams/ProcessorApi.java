package com.zinkworks.streams;

import com.zinkworks.streams.domain.Configuration;
import com.zinkworks.streams.processor.GlobalStoreUpdater;
import com.zinkworks.streams.processor.TotalPriceOrderProcessor;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toMap;

@RequiredArgsConstructor
public class ProcessorApi {

    private final static String
            REF_TOPIC = "ref-topic",
            SOURCE_TOPIC = "input-topic",
            SINK_TOPIC = "output-topic";

    private final static StoreBuilder<KeyValueStore<String, Double>> totalPriceStoreBuilder =
            Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore("total-price-store"),
                    Serdes.String(),
                    Serdes.Double()
            ).withLoggingDisabled();

    private final static Supplier<SpecificAvroSerde<ElectronicOrder>> electronicOrderSerdeSupplier = () -> {
        SpecificAvroSerde<ElectronicOrder> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(
                toConfigMap(new Configuration().getKafkaProps()),
                false);

        return specificAvroSerde;
    };

// tag::globalStoreBuilder[]
    private final static StoreBuilder<KeyValueStore<String, ElectronicOrder>> globalTotalPriceStoreBuilder =
            Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore("global-total-price-store"),
                    Serdes.String(),
                    electronicOrderSerdeSupplier.get()
            ).withLoggingDisabled(); // <1>
// end::globalStoreBuilder[]

    private final Configuration config;

    private static Map<String, ?> toConfigMap(Properties props) {
        return props.entrySet()
                .stream()
                .collect(toMap(e -> (String) e.getKey(), Entry::getValue));
    }

    public void start() {
        final Properties streamsProps = config.getKafkaProps();

        final SpecificAvroSerde<ElectronicOrder> specificAvroSerde = electronicOrderSerdeSupplier.get();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Double> doubleSerde = Serdes.Double();
        final Map<String, ElectronicOrder> dataHolder = new HashMap<>();

        Topology topology = new Topology()
                .addSource(
                        "source-node",
                        stringSerde.deserializer(),
                        specificAvroSerde.deserializer(),
                        SOURCE_TOPIC)
// tag::globalStore[]
                .addGlobalStore(
                        globalTotalPriceStoreBuilder,
                        "source-store-node", // <1>
                        stringSerde.deserializer(),
                        specificAvroSerde.deserializer(),
                        REF_TOPIC,
                        GlobalStoreUpdater.NAME,
                        () -> new GlobalStoreUpdater<>(
                                globalTotalPriceStoreBuilder.name(),
                                dataHolder))
// end::globalStore[]
//                .addSource(
//                        "source-store-node",
//                        stringSerde.deserializer(),
//                        specificAvroSerde.deserializer(),
//                        REF_TOPIC)
//                .addProcessor(
//                        InMemoryTopicAccumulator.NAME,
//                        () -> new InMemoryTopicAccumulator(REF_TOPIC, dataHolder),
//                        "source-store-node")
                .addProcessor(
                        TotalPriceOrderProcessor.NAME,
                        () -> new TotalPriceOrderProcessor(
                                dataHolder,
                                globalTotalPriceStoreBuilder.name(),
                                totalPriceStoreBuilder.name()),
                        "source-node")//, InMemoryTopicAccumulator.NAME)
                .addStateStore(
                        totalPriceStoreBuilder,
                        TotalPriceOrderProcessor.NAME)
                .addSink(
                        "sink-node",
                        SINK_TOPIC,
                        stringSerde.serializer(),
                        doubleSerde.serializer(),
                        TotalPriceOrderProcessor.NAME);
// tag::kstream[]
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
// end::kstream[]

        streamsProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        KafkaStreams kStreams = new KafkaStreams(topology, streamsProps);
        kStreams.cleanUp();
        kStreams.start();
    }

    public static void main(String[] args) {
        Configuration config = new Configuration();
        (new TopicLoader(config)).createTopics();
        (new ProcessorApi(config)).start();
    }
}
