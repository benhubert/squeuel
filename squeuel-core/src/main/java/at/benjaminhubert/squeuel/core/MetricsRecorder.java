package at.benjaminhubert.squeuel.core;

/**
 * @author <a href="mailto:code@benjaminhubert.at">Benjamin Hubert</a>
 */
public interface MetricsRecorder {

	void recordEnqueue(String queue, int length);

	void recordHandleNext(String queue, int size);

	void recordEventFailed(String queue);

	void recordEventHandled(String queue, long durationNanos);

	void recordPartitionLockRejected(String queue);

	void recordPartitionLockReleased(String queue);

	void recordPartitionLockAquired(String queue, int size);

	void recordCleanup(String queue);

}
