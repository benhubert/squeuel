package at.benjaminhubert.squeuel.core;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface StorageProvider {

    /**
     * Enqueues the given event in the given event queue.
     *
     * @param queue The queue to store the event in.
     * @param partition A partition key for grouping events. Events with the
     *                  same partition key will be processed in the order they
     *                  have been saved and will never be executed in parallel.
     * @param data The data to store.
     */
    void saveEvent(String queue, String partition, String data);

    /**
     * Searches the database for the next available events which can be
     * processed.
     *
     * Note that these events need to be locked separately before they can be
     * handled.
     *
     * @param queue The queue to find the events in.
     * @param batchSize The maximum number of events to load.
     * @return A list of events. May be empty but never null.
     */
    List<Event> findNextAvailableEvents(String queue, Integer batchSize);

    /**
     * Tries to get a lock for the partition of the given event. Makes sure that
     * the partition is not already locked and that the event has not been
     * processed before.
     *
     * @param eventId The ID of the event to lock.
     * @param lockUntilUtc Timestamp, until this partition should be locked.
     * @return True, if the partition has been locked. False if not.
     */
    boolean lockPartition(Long eventId, LocalDateTime lockUntilUtc);

    /**
     * Removes any existing lock for the given partition.
     *
     * @param eventId The ID of the event to unlock.
     */
    void unlockPartition(Long eventId);

    /**
     * Marks the given event as processed.
     *
     * @param eventId The ID of the event to mark as processed.
     */
    void markAsProcessed(Long eventId);

    /**
     * Permanently deletes already processed events from the queue.
     *
     * @param queue The queue to delete events from.
     * @param olderThanUtc UTC timestamp. Events which have been saved before
     *                     this timestamp will be removed.
     */
    void removeProcessedEvents(String queue, LocalDateTime olderThanUtc);

}
