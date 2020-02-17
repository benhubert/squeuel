package at.benjaminhubert.squeuel.core;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * @author <a href="mailto:code@benjaminhubert.at">Benjamin Hubert</a>
 */
public class QueueStats {

    private final Long numberOfWaitingEvents;
    private final Long numberOfProcessedEvents;
    private final Long numberOfPartitions;
    private final LocalDateTime timestampOfOldestQueuedEventUtc;

    public QueueStats(Long numberOfWaitingEvents,
                      Long numberOfProcessedEvents,
                      Long numberOfPartitions,
                      LocalDateTime timestampOfOldestQueuedEventUtc) {
        this.numberOfWaitingEvents = numberOfWaitingEvents;
        this.numberOfProcessedEvents = numberOfProcessedEvents;
        this.numberOfPartitions = numberOfPartitions;
        this.timestampOfOldestQueuedEventUtc = timestampOfOldestQueuedEventUtc;
    }

    public Long getNumberOfWaitingEvents() {
        return numberOfWaitingEvents;
    }

    public Long getNumberOfProcessedEvents() {
        return numberOfProcessedEvents;
    }

    public Long getNumberOfPartitions() {
        return numberOfPartitions;
    }

    public Optional<LocalDateTime> getTimestampOfOldestQueuedEventUtc() {
        return Optional.ofNullable(timestampOfOldestQueuedEventUtc);
    }

    public static class Builder {

        private Long numberOfWaitingEvents;
        private Long numberOfProcessedEvents;
        private Long numberOfPartitions;
        private LocalDateTime timestampOfOldestQueuedEventUtc;

        public void withNumberOfWaitingEvents(Long numberOfWaitingEvents) {
            this.numberOfWaitingEvents = numberOfWaitingEvents;
        }

        public void withNumberOfProcessedEvents(Long numberOfProcessedEvents) {
            this.numberOfProcessedEvents = numberOfProcessedEvents;
        }

        public void withNumberOfPartitions(Long numberOfPartitions) {
            this.numberOfPartitions = numberOfPartitions;
        }

        public void withTimestampOfOldestQueuedEventUtc(LocalDateTime timestampOfOldestQueuedEventUtc) {
            this.timestampOfOldestQueuedEventUtc = timestampOfOldestQueuedEventUtc;
        }

        public QueueStats build() {
            return new QueueStats(
                    Optional.ofNullable(numberOfWaitingEvents).orElse(0L),
                    Optional.ofNullable(numberOfProcessedEvents).orElse(0L),
                    Optional.ofNullable(numberOfPartitions).orElse(0L),
                    timestampOfOldestQueuedEventUtc
            );
        }
    }
    
}
