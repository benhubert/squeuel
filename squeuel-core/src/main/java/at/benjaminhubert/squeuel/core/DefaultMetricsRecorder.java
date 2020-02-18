package at.benjaminhubert.squeuel.core;

/**
 * @author <a href="mailto:code@benjaminhubert.at">Benjamin Hubert</a>
 */
public class DefaultMetricsRecorder implements MetricsRecorder {

	@Override
	public void recordEnqueue(String queue, int length) {
		// do nothing
	}

	@Override
	public void recordHandleNext(String queue, int size) {
		// do nothing
	}

	@Override
	public void recordEventFailed(String queue, long durationNanos) {
		// do nothing
	}

	@Override
	public void recordEventHandled(String queue, long durationNanos) {
		// do nothing
	}

	@Override
	public void recordPartitionLockRejected(String queue) {
		// do nothing
	}

	@Override
	public void recordPartitionLockReleased(String queue) {
		// do nothing
	}

	@Override
	public void recordPartitionLockAquired(String queue, int size) {
		// do nothing
	}

	@Override
	public void recordCleanup(String queue) {
		// do nothing
	}

}
