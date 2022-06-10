package com.zinkworks.streams.processor;

import io.confluent.developer.avro.ElectronicOrder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TotalPriceOrderProcessor implements Processor<String, ElectronicOrder, String, Double> {

    private final String[] storesName;
    private ProcessorContext<String, Double> context;
    private Map<String, KeyValueStore<String, Double>> storeMap;

    public TotalPriceOrderProcessor(String... storesName) {
        this.storesName = storesName;
    }

    @Override
    public void init(ProcessorContext<String, Double> context) {
        this.context = context;
        this.storeMap = Arrays.stream(storesName).collect(toMap(identity(), context::getStateStore));

        // tag::schedulePunctuation[]
        this.context.schedule(Duration.ofSeconds(30), PunctuationType.STREAM_TIME, this::forwardAll);
        // end::schedulePunctuation[]

        System.out.println("Processor initiated.");
    }

    private void useAllLocalStores(Consumer<? super KeyValueStore<String, Double>> action) {
        storeMap.keySet()
                .stream()
                .filter(k -> k.contains("global"))
                .map(storeMap::get)
                .forEach(action);
    }

    @Override
    public void process(Record<String, ElectronicOrder> record) {
        useAllLocalStores(store -> {
            final String key = record.key();
            final ElectronicOrder value = record.value();

            Double currentTotal = ofNullable(store.get(key)).orElse(0.0);
            System.out.println("current value -> " + currentTotal);

            Double newTotal = value.getPrice() + currentTotal;
            System.out.println("Price: " + value.getPrice() + " newTotal: " + newTotal);

            store.put(key, newTotal);
            System.out.println("Processed incoming record - key " + key + " value " + record.value());
        });
    }

    private void forwardAll(final long timestamp) {
        useAllLocalStores(store -> {
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

                            context.commit();
                        });
            }
        });
    }

    @Override
    public void close() {
        // No-op
    }
}
