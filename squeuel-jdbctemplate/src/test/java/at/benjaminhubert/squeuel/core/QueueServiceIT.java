package at.benjaminhubert.squeuel.core;

import at.benjaminhubert.squeuel.jdbctemplate.JdbcStorageProvider;
import at.benjaminhubert.squeuel.testutils.Database;
import at.benjaminhubert.squeuel.testutils.DatabaseTables;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.hamcrest.MatcherAssert.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author <a href="mailto:code@benjaminhubert.at">Benjamin Hubert</a>
 */
@TestInstance(PER_CLASS)
public class QueueServiceIT {

	private Database database;

	@BeforeAll
	void startupDatabase() {
		this.database = Database.createPostgresDatabase();
	}

	@AfterAll
	void shutdownDatabase() {
		database.stop();
	}

	private QueueService initQueueService(DatabaseTables tables) {
		JdbcStorageProvider storageProvider = new JdbcStorageProvider(database.getJdbcTemplate(), tables.event(), tables.lock());
		return new DefaultQueueService(storageProvider);
	}

	@Test
	void testThatMultipleWorkersProcessEventsOfAPartitionInFifoOrder() {
		int numberOfEvents = 1000;
		int numberOfThreads = 5;

		// prepare
		DatabaseTables tables = database.createTables();
		QueueService queueService = initQueueService(tables);
		for (int i = 0; i < numberOfEvents; i++) {
			queueService.enqueue("test_queue", "partition_1", "" + i);
		}

		// perform
		final List<Integer> processedEvents = Collections.synchronizedList(new ArrayList<>());
		ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
		EventHandler eventHandler = (event) -> {
			Integer value = Integer.valueOf(event.getData());
			processedEvents.add(value);
		};
		for (int threadNum = 0; threadNum < numberOfThreads; threadNum++) {
			executor.execute(() -> {
				while (true) {
					queueService.handleNext("test_queue", 20, Duration.ofHours(1L), eventHandler);
				}
			});
		}

		// wait until all events are handled
		// TODO add timeout
		while (processedEvents.size() < numberOfEvents) {
			System.out.println(processedEvents.size() + " events processed. Waiting ...");
			try { Thread.sleep(1000); } catch (Exception e) {}
		}
		executor.shutdownNow();

		// check
		Integer previous = -1;
		for (Integer i : processedEvents) {
			System.out.println(i);
			assertThat(i, greaterThan(previous));
			previous = i;
		}
	}

	@Test
	void testHowFastMultipleWorkersProcessEventsOfDifferentPartitions() {
		int numberOfEvents = 1000;
		int numberOfThreads = 5;

		// prepare
		DatabaseTables tables = database.createTables();
		QueueService queueService = initQueueService(tables);
		for (int i = 0; i < numberOfEvents; i++) {
			queueService.enqueue("test_queue", "partition_" + i, "" + i);
		}

		// perform
		final List<Integer> processedEvents = Collections.synchronizedList(new ArrayList<>());
		ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
		EventHandler eventHandler = (event) -> {
			Integer value = Integer.valueOf(event.getData());
			processedEvents.add(value);
		};
		for (int threadNum = 0; threadNum < numberOfThreads; threadNum++) {
			executor.execute(() -> {
				while (true) {
					queueService.handleNext("test_queue", 20, Duration.ofHours(1L), eventHandler);
				}
			});
		}

		// wait until all events are handled
		// TODO add timeout
		while (processedEvents.size() < numberOfEvents) {
			System.out.println(processedEvents.size() + " events processed. Waiting ...");
			try { Thread.sleep(1000); } catch (Exception e) {}
		}
		executor.shutdownNow();
	}

}
