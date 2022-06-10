package com.zinkworks.streams.processor;

import io.confluent.developer.avro.ElectronicOrder;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
public class GlobalStoreUpdater<K, V> implements Processor<String, ElectronicOrder, Void, Void> {

    private final String storeName;

    private KeyValueStore<String, ElectronicOrder> store;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<String, ElectronicOrder> record) {
        store.put(record.key(), record.value());
    }

    @Override
    public void close() {
        // No-op
    }
}
