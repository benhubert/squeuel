package at.benjaminhubert.squeuel.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DefaultQueueService implements QueueService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultQueueService.class);

    private final StorageProvider storageProvider;

    public DefaultQueueService(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    public void enqueue(String queue, String partition, String data) {
        if (isEmpty(queue)) throw new IllegalArgumentException("Queue name is required but was " + queue);
        if (isEmpty(partition)) throw new IllegalArgumentException("Partition group is required but was " + queue);
        if (isEmpty(data)) throw new IllegalArgumentException("Data is required but was " + data);

        storageProvider.saveEvent(queue, partition, data);
    }

    public void handleNext(String queue, Integer batchSize, TemporalAmount maxLockTime, EventHandler eventHandler) {
        if (isEmpty(queue)) throw new IllegalArgumentException("Queue name is required but was " + queue);
        if (batchSize == null || batchSize <= 0) throw new IllegalArgumentException("Positive batch size is required but was " + batchSize);
        if (maxLockTime == null) throw  new IllegalArgumentException("Max lock time is required but was " + maxLockTime);
        if (eventHandler == null) throw new IllegalArgumentException("Event handler is required but was " + eventHandler);

        List<Event> events = storageProvider.findNextAvailableEvents(queue, batchSize);

        Map<String, List<Event>> eventsByPartition = events.stream().collect(Collectors.groupingBy(Event::getPartition));
        eventsByPartition.entrySet()
		        .parallelStream()
		        .forEach(partition -> handleEventsOfPartition(partition.getValue(), maxLockTime, eventHandler));
    }

    private void handleEventsOfPartition(List<Event> events, TemporalAmount maxLockTime, EventHandler eventHandler) {
        Event firstEvent = events.get(0);
        String partition = firstEvent.getPartition();
        LocalDateTime lockUntilUtc = LocalDateTime.now(Clock.systemUTC()).plus(maxLockTime);
        try {
            if (storageProvider.lockPartition(firstEvent.getId(), lockUntilUtc)) {
                events.forEach((event) -> {
                    eventHandler.handle(event);
                    storageProvider.markAsProcessed(event.getId());
                });
                storageProvider.unlockPartition(firstEvent.getId());
            }
        } catch (Exception e) {
            LOG.error("Failed to process an event in partition {}. Will retry after {}", partition, lockUntilUtc);
        }
    }

    @Override
    public void cleanup(String queue, LocalDateTime olderThanUtc) {
        if (isEmpty(queue)) throw new IllegalArgumentException("Queue name is required but was " + queue);
        if (olderThanUtc == null) throw new IllegalArgumentException("A date for finding old events is required but was " + olderThanUtc);

        storageProvider.removeProcessedEvents(queue, olderThanUtc);
    }

    private boolean isEmpty(String argument) {
        return argument == null || argument.trim().isEmpty();
    }

}
