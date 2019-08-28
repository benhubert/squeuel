package at.benjaminhubert.squeuel.core;

import java.time.LocalDateTime;

public class Event {

    private final Long id;
    private final String queue;
    private final String partition;
    private final LocalDateTime createdUtc;
    private final String data;
    private final Boolean processed;

    public Event(Long id, String queue, String partition, String data, LocalDateTime createdUtc, Boolean processed) {
        this.id = id;
        this.createdUtc = createdUtc;
        this.queue = queue;
        this.partition = partition;
        this.data = data;
        this.processed = processed;
    }

    public Long getId() {
        return id;
    }

    public LocalDateTime getCreatedUtc() {
        return createdUtc;
    }

    public String getQueue() {
        return queue;
    }

    public String getPartition() {
        return partition;
    }

    public String getData() {
        return data;
    }

    public Boolean getProcessed() {
        return processed;
    }

}
