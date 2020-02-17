package at.benjaminhubert.squeuel.metrics.micrometer;

import at.benjaminhubert.squeuel.core.MetricsRecorder;
import at.benjaminhubert.squeuel.core.QueueService;
import at.benjaminhubert.squeuel.core.QueueStats;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Micrometer {@link MeterBinder} implementation for monitoring squeuel queues.
 *
 * @author <a href="mailto:code@benjaminhubert.at">Benjamin Hubert</a>
 */
public class SqueuelMeterBinder implements MeterBinder, MetricsRecorder {

	private final QueueService queueService;
	private final String[] initialQueueNames;
	private final Map<String, SqueuelMeters> meters = new HashMap<>();
	private final Lock metersRegistrationLock = new ReentrantLock();
	private final CachingStatsProvider statsCache = new CachingStatsProvider();
	private MeterRegistry meterRegistry;

	/**
	 * Creates a new instance which automatically registers as meter binder at
	 * the given {@link QueueService}.
	 *
	 * It automatically monitors every queue managed by the given queue service
	 * as soon as the first event for one specific queue comes in. It is highly
	 * recommended (but not required) to pass a list of initial queue names. If
	 * given, the meters for the given queues are initialized as soon as the
	 * binder gets bind to a {@link MeterRegistry}. Otherwise it would bind them
	 * on demand.
	 *
	 * @param queueService The queue service to register at. Must not be null.
	 * @param initialQueueNames An optional list of queue names.
	 */
	public SqueuelMeterBinder(QueueService queueService, String... initialQueueNames) {
		this.queueService = queueService;
		this.queueService.replaceMetricsRecorder(this);
		this.initialQueueNames = initialQueueNames;
	}

	@Override
	public void bindTo(MeterRegistry meterRegistry) {
		if (meterRegistry == null) return;
		this.meterRegistry = meterRegistry;
		initializeMeters();
	}

	private Optional<SqueuelMeters> forQueue(String queue) {
		if (meterRegistry == null) return Optional.empty();
		SqueuelMeters m = meters.get(queue);
		if (m == null) {
			m = registerQueue(queue);
		}
		return Optional.of(m);
	}

	private SqueuelMeters registerQueue(String queue) {
		metersRegistrationLock.lock();
		SqueuelMeters m = meters.get(queue);
		if (m == null) {
			m = new SqueuelMeters(meterRegistry, queue);
			meters.put(queue, m);
		}
		metersRegistrationLock.unlock();
		return m;
	}

	private void initializeMeters() {
		for (String queue : initialQueueNames) {
			registerQueue(queue);
		}
	}

	@Override
	public void recordEnqueue(String queue, int length) {
		forQueue(queue).ifPresent(m -> m.eventEnqueuedCounter.increment());
	}

	@Override
	public void recordHandleNext(String queue, int size) {
		forQueue(queue).ifPresent(m -> m.batchHandledCounter.increment());
	}

	@Override
	public void recordEventFailed(String queue) {
		forQueue(queue).ifPresent(m -> m.eventHandledFailedCounter.increment());
	}

	@Override
	public void recordEventHandled(String queue, long durationNanos) {
		forQueue(queue).ifPresent(m -> m.eventHandledSuccessTimer.record(durationNanos, TimeUnit.NANOSECONDS));
	}

	@Override
	public void recordPartitionLockRejected(String queue) {
		forQueue(queue).ifPresent(m -> m.partitionlockRejectedCounter.increment());
	}

	@Override
	public void recordPartitionLockReleased(String queue) {
		forQueue(queue).ifPresent(m -> m.partitionlockReleasedCounter.increment());
	}

	@Override
	public void recordPartitionLockAquired(String queue, int size) {
		forQueue(queue).ifPresent(m -> m.partitionlockAquiredCounter.increment());
	}

	@Override
	public void recordCleanup(String queue) {
		forQueue(queue).ifPresent(m -> m.cleanupCounter.increment());
	}

	private class SqueuelMeters {

		final Counter eventEnqueuedCounter;
		final Counter batchHandledCounter;
		final Counter eventHandledFailedCounter;
		final Timer eventHandledSuccessTimer;
		final Counter partitionlockRejectedCounter;
		final Counter partitionlockReleasedCounter;
		final Counter partitionlockAquiredCounter;
		final Counter cleanupCounter;

		SqueuelMeters(MeterRegistry registry, String queue) {
			eventEnqueuedCounter = Counter.builder("squeuel.enqueued.event")
					.tag("queue", queue)
					.register(registry);
			batchHandledCounter = Counter.builder("squeuel.handled.batch")
					.tag("queue", queue)
					.register(registry);
			eventHandledFailedCounter = Counter.builder("squeuel.handled.event")
					.tag("queue", queue)
					.tag("result", "failed")
					.register(registry);
			eventHandledSuccessTimer = Timer.builder("squeuel.handled.event")
					.tag("queue", queue)
					.tag("result", "success")
					.register(registry);
			partitionlockRejectedCounter = Counter.builder("squeuel.partitionlock")
					.tag("queue", queue)
					.tag("result", "rejected")
					.register(registry);
			partitionlockReleasedCounter = Counter.builder("squeuel.partitionlock")
					.tag("queue", queue)
					.tag("result", "released")
					.register(registry);
			partitionlockAquiredCounter = Counter.builder("squeuel.partitionlock")
					.tag("queue", queue)
					.tag("result", "aquired")
					.register(registry);
			cleanupCounter = Counter.builder("squeuel.cleanup")
					.tag("queue", queue)
					.register(registry);
			Gauge.builder("squeuel.queue.events", () -> statsCache.statsForQueue(queue)
					.map(QueueStats::getNumberOfWaitingEvents).orElse(0L))
					.tag("queue", queue)
					.tag("state", "waiting")
					.register(registry);
			Gauge.builder("squeuel.queue.events", () -> statsCache.statsForQueue(queue)
					.map(QueueStats::getNumberOfProcessedEvents).orElse(0L))
					.tag("queue", queue)
					.tag("state", "processed")
					.register(registry);
			Gauge.builder("squeuel.queue.partitions", () -> statsCache.statsForQueue(queue)
					.map(QueueStats::getNumberOfPartitions).orElse(0L))
					.tag("queue", queue)
					.register(registry);
			Gauge.builder("squeuel.queue.queue.oldest", () -> statsCache.statsForQueue(queue)
					.flatMap(QueueStats::getTimestampOfOldestQueuedEventUtc)
					.map(d -> d.toEpochSecond(ZoneOffset.UTC))
					.orElse(Instant.now().getEpochSecond()))
					.tag("queue", queue)
					.register(registry);
		}

	}

	private class CachingStatsProvider {

		private static final long STATS_CACHE_LIFETIME_MILLIS = 10_000;

		long timestamp = 0;
		Map<String, QueueStats> cachedStats = null;

		Optional<QueueStats> statsForQueue(String queue) {
			loadIfOutdated();
			return Optional.ofNullable(cachedStats).map(m -> m.get(queue));
		}

		private void loadIfOutdated() {
			long now = now();
			if (cachedStats == null || timestamp < (now-STATS_CACHE_LIFETIME_MILLIS)) {
				cachedStats = queueService.listQueues();
				timestamp = now;
			}
		}

		private long now() {
			return TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
		}

	}
}
