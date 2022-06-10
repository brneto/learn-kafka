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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

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

// tag::globalStoreBuilder[]
    private final static StoreBuilder<KeyValueStore<String, Double>> globalTotalPriceStoreBuilder =
            Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore("total-price-global-store"),
                    Serdes.String(),
                    Serdes.Double()
            ).withLoggingDisabled(); // <1>
// end::globalStoreBuilder[]

    private final Configuration config;

    private Map<String, ?> toConfigMap(Properties props) {
        return props.entrySet()
                .stream()
                .collect(toMap(e -> (String) e.getKey(), Entry::getValue));
    }

    public void start() {
        final Properties streamsProps = config.getKafkaProps();

        final SpecificAvroSerde<ElectronicOrder> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(toConfigMap(streamsProps), false);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Double> doubleSerde = Serdes.Double();

        Topology topology = new Topology()
                .addSource(
                        "source-node",
                        stringSerde.deserializer(),
                        specificAvroSerde.deserializer(),
                        SOURCE_TOPIC)
// tag::globalStore[]
                .addGlobalStore(
                        globalTotalPriceStoreBuilder,
                        "store-node", // <1>
                        stringSerde.deserializer(),
                        specificAvroSerde.deserializer(),
                        REF_TOPIC,
                        GlobalStoreUpdater.NAME,
                        () -> new GlobalStoreUpdater<>(globalTotalPriceStoreBuilder.name()))
// end::globalStore[]
                .addProcessor(
                        "aggregate-price",
                        () -> new TotalPriceOrderProcessor(
                                globalTotalPriceStoreBuilder.name(),
                                totalPriceStoreBuilder.name()),
                        "source-node")
                .addStateStore(
                        totalPriceStoreBuilder,
                        "aggregate-price")
                .addSink(
                        "sink-node",
                        SINK_TOPIC,
                        stringSerde.serializer(),
                        doubleSerde.serializer(),
                        "aggregate-price");
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
