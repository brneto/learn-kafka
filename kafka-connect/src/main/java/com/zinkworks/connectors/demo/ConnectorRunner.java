/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package com.zinkworks.connectors.demo;

import static com.zinkworks.connectors.demo.FileEventSourceConnector.TOPIC_NAME;
import static com.zinkworks.connectors.demo.FileEventSourceConnector.WATCH_DIR;
import static com.zinkworks.connectors.demo.FileEventSourceConnector.WATCH_EVENT;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;

@Slf4j
public class ConnectorRunner {
    public String getGreeting() {
        return "Hello World!";
    }

    public static void main(String[] args) throws InterruptedException {
        log.info("Running main...");
        log.info(new ConnectorRunner().getGreeting());

        var task = new FileEventSourceTask();
        task.start(Map.of(
            WATCH_DIR, ".",
            WATCH_EVENT, "create",
            TOPIC_NAME, "whatever"));

        for (int i = 0; i < 5; i++) {
            List<SourceRecord> recordList = task.poll();
            if (recordList == null) {
                Thread.sleep(5000);
                continue;
            }
            recordList.forEach(r -> log.info(r.toString()));
        }

    }
}
