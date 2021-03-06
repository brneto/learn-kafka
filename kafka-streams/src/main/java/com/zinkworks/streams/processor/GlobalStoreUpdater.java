package com.zinkworks.streams.processor;

import io.confluent.developer.avro.ElectronicOrder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Map;

@RequiredArgsConstructor
public class GlobalStoreUpdater<K, V> implements Processor<String, ElectronicOrder, Void, Void> {

    public static final String NAME = "global-store-updater";

    private final String storeName;

    @NonNull
    private final Map<String, ElectronicOrder> accumulator;

    private ProcessorContext<Void, Void> context;
    private KeyValueStore<String, ElectronicOrder> store;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.context = context;
        this.store = context.getStateStore(storeName);
        System.out.printf(NAME + "[threadId: %s] -> Processor initiated.\n", Thread.currentThread().getId());
    }

    @Override
    public void process(Record<String, ElectronicOrder> record) {
        final String key = record.key();
        final ElectronicOrder value = record.value();

        store.put(key, value);
        accumulator.put(key, value);
        System.out.printf(
                NAME + "[threadId: %s] -> Processed incoming record - partition: %d, key: %s, value: %s\n",
                Thread.currentThread().getId(), context.recordMetadata().get().partition(), key, value);
    }

    @Override
    public void close() {
        // No-op
    }
}
