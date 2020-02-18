package at.benjaminhubert.squeuel.core;

import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.Map;

public interface QueueService {

    /**
     * Enqueues the given data in the database.
     *
     * @param queue The queue to push the event to. Must not be null.
     * @param partition A partition key for this event. Must not be null and
     *                  must be between 1 and 64 characters long.
     * @param data The data to push. Must not be null.
     */
    void enqueue(String queue, String partition, String data);

    /**
     * Fetches the next available event from the database.
     *
     * Searches for the next available event, tries to get a lock for it, passes
     * it to the event handler. As soon as the event handler is done, the event
     * is marked as complete, and the lock is being released. If the event
     * handler ended up in an exception, the event will stay locked until the
     * maxLockTime is over, and will be retried by another handler afterwards.
     *
     * @param queue The queue to fetch events from. Must not be null.
     * @param batchSize The maximum number of events to fetch from the database.
     * @param maxLockTime The maximum time this event should be locked. When
     *                    this period is over, the event will be unlocked,
     *                    regardless if the handler finished successfully or
     *                    not. After this time another handler would take this
     *                    event, which means, the event will get stuck in an
     *                    endless loop until a handler was able to process it.
     *                    Note that this also blocks any other event in the
     *                    same partition.
     * @param eventHandler The handler that handles the event. Note that if no
     *                     event is queued at the moment, this handler will not
     *                     be called. Must not be null.
     */
    void handleNext(String queue, Integer batchSize, TemporalAmount maxLockTime, EventHandler eventHandler);

    /**
     * Cleans up old events from the given queue, which were enqueued before
     * the given UTC timestamp and have been successfully handled.
     *
     * @param queue The queue to delete messages from.
     * @param olderThanUtc UTC timestamp. Events which were pushed before this
     *                     timestamp will be removed.
     */
    void cleanup(String queue, LocalDateTime olderThanUtc);

    /**
     * Returns the list of queues, which are currently part of the system.
     *
     * @return The list of all queues with the number of currently enqueued
     * events. May be empty, but never null.
     */
    Map<String, QueueStats> listQueues();

    /**
     * Replaces any previously registered {@link MetricsRecorder} with the given
     * one.
     *
     * @param metricsRecorder The new {@link MetricsRecorder}. Must not be null.
     */
    void replaceMetricsRecorder(MetricsRecorder metricsRecorder);

}
