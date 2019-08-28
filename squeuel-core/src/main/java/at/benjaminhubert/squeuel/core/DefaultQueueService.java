package at.benjaminhubert.squeuel.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Optional;

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

        List<Long> eventIds = storageProvider.findNextAvailableEvents(queue, batchSize);
        eventIds.forEach(id -> {
            LocalDateTime lockUntilUtc = LocalDateTime.now(Clock.systemUTC()).plus(maxLockTime);
            try {
                if (storageProvider.lockEvent(id, lockUntilUtc)) {
                    Optional<Event> event = storageProvider.fetchEvent(id);
                    if (!event.isEmpty()) {
                        eventHandler.handle(event.get());
                        storageProvider.markAsProcessed(id);
                    } else {
                        LOG.error("Seems that the event with the ID {} got removed while I had a lock for it. This should not happen.", id);
                    }
                }
            } catch (Exception e) {
                LOG.error("Failed to process event. Will retry after {}", lockUntilUtc);
            }
        });
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
