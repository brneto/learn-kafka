package com.zinkworks.connectors.demo;

import static com.zinkworks.connectors.demo.FileEventSourceConnector.TOPIC_NAME;
import static com.zinkworks.connectors.demo.FileEventSourceConnector.WATCH_DIR;
import static com.zinkworks.connectors.demo.FileEventSourceConnector.WATCH_EVENT;
import static java.lang.String.format;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Selector;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

@Slf4j
public class FileEventSourceTask extends SourceTask {

  private final FileEventSourceConnector sourceConnector = new FileEventSourceConnector();
  private String watchDir;
  private String topicName;
  private Map<String, String> sourcePartition;
  private WatchService watcher;

  /**
   * Get the version of this task. Usually this should be the same as the corresponding {@link
   * FileEventSourceConnector} class's version.
   *
   * @return the version, formatted as a String
   */
  @Override
  public String version() {
    return sourceConnector.version();
  }

  /**
   * Start the Task. This should handle any configuration parsing and one-time setup of the task.
   *
   * @param props initial configuration
   */
  @Override
  public void start(Map<String, String> props) {
    log.info("FileEventSourceTask -> start -> invoked [{}]", props);
    final String watchEvent = props.get(WATCH_EVENT);
    watchDir = props.get(WATCH_DIR);
    topicName = props.get(TOPIC_NAME);
    sourcePartition = Map.of("watchDir", watchDir);

    try {
      watcher = FileSystems.getDefault().newWatchService();
      Path.of(watchDir).register(watcher, valueOf(watchEvent));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private WatchEvent.Kind<Path> valueOf(String eventKind) throws IOException {
    switch (eventKind) {
      case "create": return ENTRY_CREATE;
      case "modify": return ENTRY_MODIFY;
      case "delete": return ENTRY_DELETE;
      default: throw new IOException(format("Unknown event kind [%s]", eventKind));
    }
  }

  /**
   * <p>
   * Poll this source task for new records. If no data is currently available, this method should
   * block but return control to the caller regularly (by returning {@code null}) in order for the
   * task to transition to the {@code PAUSED} state if requested to do so.
   * </p>
   * <p>
   * The task will be {@link #stop() stopped} on a separate thread, and when that happens this
   * method is expected to unblock, quickly finish up any remaining processing, and return.
   * </p>
   *
   * @return a list of source records
   */
  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.info("FileEventSourceTask -> poll -> invoked");
    final AtomicInteger idx = new AtomicInteger();
    final List<SourceRecord> result = new ArrayList<>();

    final WatchKey watchKey = watcher.take();
    watchKey
        .pollEvents()
        .forEach(event -> {
          WatchEvent.Kind<?> kind = event.kind();
          if (kind != OVERFLOW) {
            Path path = (Path) event.context();

            Map<String, Integer> sourceOffset = Map.of("index", idx.incrementAndGet());
            String key = Instant.now().toString();
            String value = kind.name() + ":" + path.toFile().getName();
            result.add(
                new SourceRecord(
                    sourcePartition, sourceOffset, topicName,
                    Schema.STRING_SCHEMA, key,
                    Schema.STRING_SCHEMA, value));
          }
        });

    if (!watchKey.reset()) {
      var e = new InterruptedException(format("Directory '%s' is inaccessible", watchDir));
      log.error("The watch key is no longer valid", e);
      throw e;
    }

    log.info("FileEventSourceTask -> poll -> completed [result count = {}]", result.size());
    return result;
  }

  /**
   * Signal this SourceTask to stop. In SourceTasks, this method only needs to signal to the task
   * that it should stop trying to poll for new data and interrupt any outstanding poll() requests.
   * It is not required that the task has fully stopped. Note that this method necessarily may be
   * invoked from a different thread than {@link #poll()} and {@link #commit()}.
   * <p>
   * For example, if a task uses a {@link Selector} to receive data over the network, this method
   * could set a flag that will force {@link #poll()} to exit immediately and invoke {@link
   * Selector#wakeup() wakeup()} to interrupt any ongoing requests.
   */
  @Override
  public void stop() {
    log.info("FileEventSourceTask -> stop -> invoked");
  }
}
