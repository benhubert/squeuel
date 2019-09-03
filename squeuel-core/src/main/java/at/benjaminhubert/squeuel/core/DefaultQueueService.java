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

        List<Event> events = storageProvider.findNextAvailableEvents(queue, batchSize);
        events.forEach(event -> {
            long starttime = System.currentTimeMillis();
            LOG.info("TIME       start " + (System.currentTimeMillis() - starttime));
            LocalDateTime lockUntilUtc = LocalDateTime.now(Clock.systemUTC()).plus(maxLockTime);
            try {
                LOG.info("TIME before lock " + (System.currentTimeMillis() - starttime));
                if (storageProvider.lockEvent(event.getId(), lockUntilUtc)) {
                    LOG.info("TIME  after lock " + (System.currentTimeMillis() - starttime));
                    eventHandler.handle(event);
                    LOG.info("TIME after handl " + (System.currentTimeMillis() - starttime));
                    storageProvider.markAsProcessed(event.getId());
                    LOG.info("TIME marked proc " + (System.currentTimeMillis() - starttime));
                } else {
                    LOG.info("TIME lock missed " + (System.currentTimeMillis() - starttime));
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
