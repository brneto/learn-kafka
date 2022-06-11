package com.zinkworks.streams.processor;

import io.confluent.developer.avro.ElectronicOrder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Map;

@RequiredArgsConstructor
public class InMemoryTopicAccumulator implements Processor<String, ElectronicOrder, Void, Void>  {

    public static final String NAME = "accumulate-topic-in-memory";

    @NonNull
    private final String topic;

    @NonNull
    private final Map<String, ElectronicOrder> accumulator;

    private ProcessorContext<Void, Void> context;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
       this.context = context;
        System.out.printf(NAME + "[threadId: %s] -> Processor initiated.\n", Thread.currentThread().getId());
    }

    @Override
    public void process(Record<String, ElectronicOrder> record) {
        context.recordMetadata().ifPresent(m -> {
            if (!topic.isEmpty() && topic.equals(m.topic())) {
                final String key = record.key();
                final ElectronicOrder value = record.value();

                accumulator.put(key, value);
                System.out.printf(
                        NAME + "[threadId: %s] -> Processed incoming record - partition: %d, key: %s, value: %s\n",
                        Thread.currentThread().getId(), m.partition(), key, value);
            }
        });
    }

    @Override
    public void close() {
        // No-op
    }
}
