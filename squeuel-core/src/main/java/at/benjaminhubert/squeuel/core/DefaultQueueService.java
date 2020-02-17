package at.benjaminhubert.squeuel.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultQueueService implements QueueService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultQueueService.class);

    private final StorageProvider storageProvider;
    private MetricsRecorder metricsRecorder = new DefaultMetricsRecorder();

    public DefaultQueueService(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    public void enqueue(String queue, String partition, String data) {
        if (isEmpty(queue)) throw new IllegalArgumentException("Queue name is required but was " + queue);
        if (isEmpty(partition)) throw new IllegalArgumentException("Partition group is required but was " + queue);
        if (isEmpty(data)) throw new IllegalArgumentException("Data is required but was " + data);

        storageProvider.saveEvent(queue, partition, data);
        metricsRecorder.recordEnqueue(queue, data.length());
    }

    public void handleNext(String queue, Integer batchSize, TemporalAmount maxLockTime, EventHandler eventHandler) {
        if (isEmpty(queue)) throw new IllegalArgumentException("Queue name is required but was " + queue);
        if (batchSize == null || batchSize <= 0) throw new IllegalArgumentException("Positive batch size is required but was " + batchSize);
        if (maxLockTime == null) throw  new IllegalArgumentException("Max lock time is required but was " + maxLockTime);
        if (eventHandler == null) throw new IllegalArgumentException("Event handler is required but was " + eventHandler);

        List<Event> events = storageProvider.findNextAvailableEvents(queue, batchSize);
        metricsRecorder.recordHandleNext(queue, events.size());

        Map<String, List<Event>> eventsByPartition = events.stream().collect(Collectors.groupingBy(Event::getPartition));
        eventsByPartition.forEach((key, value) -> handleEventsOfPartition(queue, value, maxLockTime, eventHandler));
    }

    private void handleEventsOfPartition(String queue, List<Event> events, TemporalAmount maxLockTime, EventHandler eventHandler) {
        Event firstEvent = events.get(0);
        String partition = firstEvent.getPartition();
        LocalDateTime lockUntilUtc = LocalDateTime.now(Clock.systemUTC()).plus(maxLockTime);
        try {
            if (storageProvider.lockPartition(firstEvent.getId(), lockUntilUtc)) {
                metricsRecorder.recordPartitionLockAquired(queue, events.size());
                events.forEach((event) -> {
                    handleEvent(queue, eventHandler, event);
                    storageProvider.markAsProcessed(event.getId());
                });
                storageProvider.unlockPartition(firstEvent.getId());
                metricsRecorder.recordPartitionLockReleased(queue);
            } else {
                metricsRecorder.recordPartitionLockRejected(queue);
            }
        } catch (Exception e) {
            metricsRecorder.recordEventFailed(queue);
            LOG.error("Failed to process an event in partition {}. Will retry after {}", partition, lockUntilUtc);
        }
    }

    private void handleEvent(String queue, EventHandler eventHandler, Event event) {
        long start = System.nanoTime();
        eventHandler.handle(event);
        long durationNanos = System.nanoTime() - start;
        metricsRecorder.recordEventHandled(queue, durationNanos);
    }

    @Override
    public void cleanup(String queue, LocalDateTime olderThanUtc) {
        if (isEmpty(queue)) throw new IllegalArgumentException("Queue name is required but was " + queue);
        if (olderThanUtc == null) throw new IllegalArgumentException("A date for finding old events is required but was " + olderThanUtc);

        storageProvider.removeProcessedEvents(queue, olderThanUtc);
        metricsRecorder.recordCleanup(queue);
    }

    @Override
    public Map<String, QueueStats> listQueues() {
        return storageProvider.listQueues();
    }

    private boolean isEmpty(String argument) {
        return argument == null || argument.trim().isEmpty();
    }

    @Override
    public void replaceMetricsRecorder(MetricsRecorder metricsRecorder) {
        if (metricsRecorder == null) throw new IllegalArgumentException("Metrics recorder must not be null");
        this.metricsRecorder = metricsRecorder;
    }

}
