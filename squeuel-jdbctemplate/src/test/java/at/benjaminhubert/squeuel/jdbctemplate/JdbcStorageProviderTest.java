package at.benjaminhubert.squeuel.jdbctemplate;

import at.benjaminhubert.squeuel.core.Event;
import at.benjaminhubert.squeuel.core.QueueStats;
import at.benjaminhubert.squeuel.testutils.Database;
import at.benjaminhubert.squeuel.testutils.DatabaseTables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.jdbc.core.RowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import static org.exparity.hamcrest.date.LocalDateTimeMatchers.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle.*;

@TestInstance(PER_CLASS)
public class JdbcStorageProviderTest {

    private Database database;

    @BeforeAll
    void startupDatabase() {
        this.database = Database.createPostgresDatabase();
    }

    @AfterAll
    void shutdownDatabase() {
        database.stop();
    }

    private JdbcStorageProvider initializeStorageProvider(DatabaseTables tables) {
        return new JdbcStorageProvider(database.getDataSource(), tables.event(), tables.lock());
    }

    @Test
    void saveEvent_savesEventAsExpected() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        storageProvider.saveEvent("test_queue", "part01", "some test data");

        // check
        List<EventRow> events = fetchEventsFromDatabase(tables);
        assertThat(events, hasSize(1));
        EventRow event = events.get(0);
        assertThat(event.queue, equalTo("test_queue"));
        assertThat(event.partition, equalTo("part01"));
        assertThat(event.data, equalTo("some test data"));
        assertThat(event.id, not(nullValue()));
        assertThat(event.createdUtc, within(1, ChronoUnit.MINUTES, now()));
        assertThat(event.processed, equalTo(false));
    }

    @Test
    void saveEvent_savesMultipleEvents() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        storageProvider.saveEvent("test_queue", "part01", "some test data");
        storageProvider.saveEvent("test_queue", "part01", "some other test data");
        storageProvider.saveEvent("other_queue", "part01", "some test data");

        // check
        List<EventRow> events = fetchEventsFromDatabase(tables);
        assertThat(events, hasSize(3));
    }

    @Test
    void saveEvent_savesEventsAtColumnLimits() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        storageProvider.saveEvent("1", "part01", "some test data");
        storageProvider.saveEvent("test_queue", "1", "some test data");
        storageProvider.saveEvent("1234567890123456789012345678901234567890123456789012345678901234", "part01", "some test data");
        storageProvider.saveEvent("test_queue", "1234567890123456789012345678901234567890123456789012345678901234", "some other test data");

        // check
        List<EventRow> events = fetchEventsFromDatabase(tables);
        assertThat(events, hasSize(4));
    }

    @Test
    void saveEvent_validatesParameters() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        assertThrows(IllegalArgumentException.class, () -> storageProvider.saveEvent(null, "part01", "some test data"));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.saveEvent("test_queue", null, "some test data"));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.saveEvent("test_queue", "part01", null));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.saveEvent("", "part01", "some test data"));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.saveEvent("test_queue", "", "some test data"));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.saveEvent(" ", "part01", "some test data"));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.saveEvent("test_queue", " ", "some test data"));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.saveEvent("12345678901234567890123456789012345678901234567890123456789012345", "part01", "some test data"));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.saveEvent("test_queue", "12345678901234567890123456789012345678901234567890123456789012345", "some test data"));
    }

    @Test
    void saveEvent_withEmptyDataIsAllowed() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        storageProvider.saveEvent("test_queue", "partition", "");

        // check
        List<EventRow> events = fetchEventsFromDatabase(tables);
        assertThat(events, hasSize(1));
        EventRow event = events.get(0);
        assertThat(event.data, equalTo(""));
    }

    @Test
    void findNextAvailableEvents_returnsOnlyRequestedBatchSize() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(5, ChronoUnit.MINUTES), false);
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_1", "partition_3", "data", now().minus(3, ChronoUnit.MINUTES), false);
        insertEvent(tables, 4L, "queue_1", "partition_4", "data", now().minus(2, ChronoUnit.MINUTES), false);
        insertEvent(tables, 5L, "queue_1", "partition_5", "data", now().minus(1, ChronoUnit.MINUTES), false);

        // perform
        List<Event> events = storageProvider.findNextAvailableEvents("queue_1", 3);

        // check
        assertThat(events, hasSize(3));
        assertThat(events.get(0).getId(), equalTo(1L));
        assertThat(events.get(1).getId(), equalTo(2L));
        assertThat(events.get(2).getId(), equalTo(3L));
    }

    @Test
    void findNextAvailableEvents_returnsNoLockedEvents() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(5, ChronoUnit.MINUTES), false);
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_1", "partition_3", "data", now().minus(3, ChronoUnit.MINUTES), false);
        insertEvent(tables, 4L, "queue_1", "partition_4", "data", now().minus(2, ChronoUnit.MINUTES), false);
        insertEvent(tables, 5L, "queue_1", "partition_5", "data", now().minus(1, ChronoUnit.MINUTES), false);
        insertLock(tables, 1L, "queue_1", "partition_2", now().plus(1, ChronoUnit.HOURS));

        // perform
        List<Event> events = storageProvider.findNextAvailableEvents("queue_1", 3);

        // check
        assertThat(events, hasSize(3));
        assertThat(events.get(0).getId(), equalTo(1L));
        assertThat(events.get(1).getId(), equalTo(3L));
        assertThat(events.get(2).getId(), equalTo(4L));
    }

    @Test
    void findNextAvailableEvents_returnsNoEventsOfALockedPartition() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(5, ChronoUnit.MINUTES), false);
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_1", "partition_1", "data", now().minus(3, ChronoUnit.MINUTES), false);
        insertEvent(tables, 4L, "queue_1", "partition_2", "data", now().minus(2, ChronoUnit.MINUTES), false);
        insertLock(tables, 1L, "queue_1", "partition_2", now().plus(1, ChronoUnit.HOURS));

        // perform
        List<Event> events = storageProvider.findNextAvailableEvents("queue_1", 5);

        // check
        assertThat(events, hasSize(2));
        assertThat(events.get(0).getId(), equalTo(1L));
        assertThat(events.get(1).getId(), equalTo(3L));
    }

    @Test
    void findNextAvailableEvents_returnsEventsOfAPartitionWhichHasTheSameNameAsALockedPartitionOfAnotherQueue() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(5, ChronoUnit.MINUTES), false);
        insertEvent(tables, 2L, "queue_2", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_1", "partition_2", "data", now().minus(3, ChronoUnit.MINUTES), false);
        insertEvent(tables, 4L, "queue_1", "partition_1", "data", now().minus(2, ChronoUnit.MINUTES), false);
        insertLock(tables, 1L, "queue_1", "partition_2", LocalDateTime.now(Clock.systemDefaultZone()).plus(1, ChronoUnit.HOURS));

        // perform
        List<Event> eventsOfQueue1 = storageProvider.findNextAvailableEvents("queue_1", 5);
        List<Event> eventsOfQueue2 = storageProvider.findNextAvailableEvents("queue_2", 5);

        // check
        assertThat(eventsOfQueue1, hasSize(2));
        assertThat(eventsOfQueue1.get(0).getId(), equalTo(1L));
        assertThat(eventsOfQueue1.get(1).getId(), equalTo(4L));
        assertThat(eventsOfQueue2, hasSize(1));
        assertThat(eventsOfQueue2.get(0).getId(), equalTo(2L));
    }

    @Test
    void findNextAvailableEvents_doesNotReturnAlreadyProcessedEvents() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(5, ChronoUnit.MINUTES), true);
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_1", "partition_3", "data", now().minus(3, ChronoUnit.MINUTES), false);

        // perform
        List<Event> events = storageProvider.findNextAvailableEvents("queue_1", 5);

        // check
        assertThat(events, hasSize(2));
        assertThat(events.get(0).getId(), equalTo(2L));
        assertThat(events.get(1).getId(), equalTo(3L));
    }

    @Test
    void findNextAvailableEvents_returnsEventsInCorrectOrder() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", now().minus(3, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_1", "partition_3", "data", now().minus(5, ChronoUnit.MINUTES), false);

        // perform
        List<Event> events = storageProvider.findNextAvailableEvents("queue_1", 5);

        // check
        assertThat(events, hasSize(3));
        assertThat(events.get(0).getId(), equalTo(3L));
        assertThat(events.get(1).getId(), equalTo(1L));
        assertThat(events.get(2).getId(), equalTo(2L));
    }

    @Test
    void findNextAvailableEvents_ReturnsEventsWithOutdatedLocks() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(4, ChronoUnit.HOURS), false);
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", now().minus(3, ChronoUnit.HOURS), false);
        insertLock(tables, 1L, "queue_1", "partition_2", now().minus(1, ChronoUnit.HOURS));

        // perform
        List<Event> events = storageProvider.findNextAvailableEvents("queue_1", 5);

        // check
        assertThat(events, hasSize(2));
        assertThat(events.get(0).getId(), equalTo(1L));
        assertThat(events.get(1).getId(), equalTo(2L));
    }

    @Test
    void findNextAvailableEvents_validatesParameters() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        assertThrows(IllegalArgumentException.class, () -> storageProvider.findNextAvailableEvents(null, 5));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.findNextAvailableEvents("", 5));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.findNextAvailableEvents(" ", 5));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.findNextAvailableEvents("queue", 0));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.findNextAvailableEvents("queue", -1));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.findNextAvailableEvents("queue", null));
    }

    @Test
    void lockPartition_insertsLockAsExpected() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 12L, "queue_1", "partition_1", "data", now().minus(4, ChronoUnit.MINUTES), false);

        // perform
        Boolean locked = storageProvider.lockPartition(12L, now().plusHours(1));

        // check
        assertThat(locked, equalTo(true));
        List<LockRow> lockRows = fetchLocksFromDatabase(tables);
        assertThat(lockRows, hasSize(1));
        assertThat(lockRows.get(0).queue, equalTo("queue_1"));
        assertThat(lockRows.get(0).partition, equalTo("partition_1"));
        assertThat(lockRows.get(0).lockedUntilUtc, within(1, ChronoUnit.MINUTES, now().plusHours(1)));
    }

    @Test
    void lockPartition_doesNotLockIfAlreadyLocked() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 12L, "queue_1", "partition_1", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertLock(tables, 233L, "queue_1", "partition_1", now().plusHours(1));

        // perform
        Boolean locked = storageProvider.lockPartition(12L, now().plusHours(2));

        // check
        assertThat(locked, equalTo(false));
        List<LockRow> lockRows = fetchLocksFromDatabase(tables);
        assertThat(lockRows, hasSize(1));
        assertThat(lockRows.get(0).id, equalTo(233L));
        assertThat(lockRows.get(0).queue, equalTo("queue_1"));
        assertThat(lockRows.get(0).partition, equalTo("partition_1"));
        assertThat(lockRows.get(0).lockedUntilUtc, within(1, ChronoUnit.MINUTES, now().plusHours(1)));
    }

    @Test
    void lockPartition_doesNotLockAnythingIfEventDoesNotExist() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        Boolean locked = storageProvider.lockPartition(12L, now().plusHours(2));

        // check
        assertThat(locked, equalTo(false));
        List<LockRow> lockRows = fetchLocksFromDatabase(tables);
        assertThat(lockRows, hasSize(0));
    }

    @Test
    void lockPartition_locksIfPrevoiusLockIsAlreadyOutdated() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 73L, "queue_1", "partition_1", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertLock(tables, 233L, "queue_1", "partition_1", now().minusHours(1));

        // perform
        Boolean locked = storageProvider.lockPartition(73L, now().plusHours(2));

        // check
        assertThat(locked, equalTo(true));
        List<LockRow> lockRows = fetchLocksFromDatabase(tables);
        assertThat(lockRows, hasSize(1));
        assertThat(lockRows.get(0).id, not(equalTo(233L)));
        assertThat(lockRows.get(0).queue, equalTo("queue_1"));
        assertThat(lockRows.get(0).partition, equalTo("partition_1"));
        assertThat(lockRows.get(0).lockedUntilUtc, within(1, ChronoUnit.MINUTES, now().plusHours(2)));
    }

    @Test
    void lockPartition_doesNotLockAlreadyProcessedEvent() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(4, ChronoUnit.MINUTES), true);
        boolean locked = storageProvider.lockPartition(1L, now().plusHours(1));

        // check
        assertThat(locked, equalTo(false));
    }

    @Test
    void lockEvent_validatesParameters() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        assertThrows(IllegalArgumentException.class, () -> storageProvider.lockPartition(null, now()));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.lockPartition(123L, null));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.lockPartition(123L, now().minusMinutes(1)));
    }

    @Test
    void markAsProcessed_marksEventAsProcessed() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 73L, "queue_1", "partition_1", "data", now(), false);

        // perform
        storageProvider.markAsProcessed(73L);

        // check
        List<EventRow> events = fetchEventsFromDatabase(tables);
        assertThat(events, hasSize(1));
        EventRow event = events.get(0);
        assertThat(event.id, equalTo(73L));
        assertThat(event.processed, equalTo(true));
    }

    @Test
    void markAsProcessed_doesNothingIfEventAlreadyMarkedProcessed() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 73L, "queue_1", "partition_1", "data", now(), true);

        // perform
        storageProvider.markAsProcessed(73L);

        // check
        List<EventRow> events = fetchEventsFromDatabase(tables);
        assertThat(events, hasSize(1));
        EventRow event = events.get(0);
        assertThat(event.id, equalTo(73L));
        assertThat(event.processed, equalTo(true));
    }

    @Test
    void markAsProcessed_validatesParameters() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        assertThrows(IllegalArgumentException.class, () -> storageProvider.markAsProcessed(null));
    }

    // TODO Test removeProcessedEvents

    @Test
    void listQueues_calculatesCorrectNumberOfProcessedEvents() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(5, ChronoUnit.MINUTES), true);
        insertEvent(tables, 2L, "queue_2", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_2", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 4L, "queue_1", "partition_2", "data", now().minus(3, ChronoUnit.MINUTES), true);
        insertEvent(tables, 5L, "queue_1", "partition_1", "data", now().minus(2, ChronoUnit.MINUTES), false);

        // perform
        Map<String, QueueStats> stats = storageProvider.listQueues();

        // check
        assertThat(stats.entrySet(), hasSize(2));
        assertThat(stats.get("queue_1").getNumberOfProcessedEvents(), equalTo(2L));
        assertThat(stats.get("queue_1").getNumberOfWaitingEvents(), equalTo(1L));
        assertThat(stats.get("queue_2").getNumberOfProcessedEvents(), equalTo(0L));
        assertThat(stats.get("queue_2").getNumberOfWaitingEvents(), equalTo(2L));
    }

    @Test
    void listQueues_calculatesCorrectNumberOfPartitions() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(5, ChronoUnit.MINUTES), false);
        insertEvent(tables, 2L, "queue_2", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_2", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 4L, "queue_1", "partition_2", "data", now().minus(3, ChronoUnit.MINUTES), false);
        insertEvent(tables, 5L, "queue_1", "partition_1", "data", now().minus(2, ChronoUnit.MINUTES), false);

        // perform
        Map<String, QueueStats> stats = storageProvider.listQueues();

        // check
        assertThat(stats.entrySet(), hasSize(2));
        assertThat(stats.get("queue_1").getNumberOfPartitions(), equalTo(2L));
        assertThat(stats.get("queue_2").getNumberOfPartitions(), equalTo(1L));
    }

    @Test
    void listQueues_returnsExpectedDateForOldestWaitingEvent() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        LocalDateTime t5MinutesAgo = now().minus(5, ChronoUnit.MINUTES);
        LocalDateTime t4MinutesAgo = now().minus(4, ChronoUnit.MINUTES);
        LocalDateTime t3MinutesAgo = now().minus(3, ChronoUnit.MINUTES);
        LocalDateTime t2MinutesAgo = now().minus(2, ChronoUnit.MINUTES);
        LocalDateTime t1MinutesAgo = now().minus(1, ChronoUnit.MINUTES);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", t5MinutesAgo, true);
        insertEvent(tables, 2L, "queue_2", "partition_2", "data", t4MinutesAgo, false);
        insertEvent(tables, 3L, "queue_2", "partition_2", "data", t3MinutesAgo, false);
        insertEvent(tables, 4L, "queue_1", "partition_2", "data", t2MinutesAgo, true);
        insertEvent(tables, 5L, "queue_1", "partition_1", "data", t1MinutesAgo, false);
        insertEvent(tables, 6L, "queue_3", "partition_2", "data", t2MinutesAgo, true);
        insertEvent(tables, 7L, "queue_3", "partition_1", "data", t1MinutesAgo, true);

        // perform
        Map<String, QueueStats> stats = storageProvider.listQueues();

        // check
        assertThat(stats.entrySet(), hasSize(3));
        assertThat(stats.get("queue_1").getTimestampOfOldestQueuedEventUtc().isPresent(), equalTo(true));
        assertThat(stats.get("queue_1").getTimestampOfOldestQueuedEventUtc().get(), equalTo(t1MinutesAgo));
        assertThat(stats.get("queue_2").getTimestampOfOldestQueuedEventUtc().isPresent(), equalTo(true));
        assertThat(stats.get("queue_2").getTimestampOfOldestQueuedEventUtc().get(), equalTo(t4MinutesAgo));
        assertThat(stats.get("queue_3").getTimestampOfOldestQueuedEventUtc().isPresent(), equalTo(false));
    }

    @Test
    void removeProcessedEvents_deletesWantedAndKeepsUnprocessedOrNewerEvents() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        LocalDateTime t5MinutesAgo = now().minus(5, ChronoUnit.MINUTES);
        LocalDateTime t4MinutesAgo = now().minus(4, ChronoUnit.MINUTES);
        LocalDateTime t3MinutesAgo = now().minus(3, ChronoUnit.MINUTES);
        LocalDateTime t2MinutesAgo = now().minus(2, ChronoUnit.MINUTES);
        LocalDateTime t1MinutesAgo = now().minus(1, ChronoUnit.MINUTES);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", t5MinutesAgo, true);  // delete
        insertEvent(tables, 2L, "queue_2", "partition_2", "data", t4MinutesAgo, false); // keep
        insertEvent(tables, 3L, "queue_2", "partition_2", "data", t3MinutesAgo, false); // keep
        insertEvent(tables, 4L, "queue_1", "partition_2", "data", t2MinutesAgo, true);  // keep
        insertEvent(tables, 5L, "queue_1", "partition_1", "data", t1MinutesAgo, false); // keep
        insertEvent(tables, 6L, "queue_3", "partition_2", "data", t4MinutesAgo, true);  // delete
        insertEvent(tables, 7L, "queue_3", "partition_1", "data", t3MinutesAgo, true);  // keep

        // perform
        storageProvider.removeProcessedEvents("queue_1", t3MinutesAgo);
        // check
        List<EventRow> events = fetchEventsFromDatabase(tables);
        assertThat(events, hasSize(6));
        assertThat(events.stream().anyMatch(e -> e.id.equals(1L)), is(false));

        // perform
        storageProvider.removeProcessedEvents("queue_2", t3MinutesAgo);
        // check
        events = fetchEventsFromDatabase(tables);
        assertThat(events, hasSize(6));

        // perform
        storageProvider.removeProcessedEvents("queue_3", t3MinutesAgo);
        // check
        events = fetchEventsFromDatabase(tables);
        assertThat(events, hasSize(5));
        assertThat(events.stream().anyMatch(e -> e.id.equals(6L)), is(false));
    }

    @Test
    void removeProcessedEvents_withDateInFutureRemovesAllProcessedEvents() {
        // prepare
        DatabaseTables tables = database.createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        LocalDateTime t5MinutesAgo = now().minus(5, ChronoUnit.MINUTES);
        LocalDateTime t2MinutesAgo = now().minus(2, ChronoUnit.MINUTES);
        LocalDateTime t1MinutesAgo = now().minus(1, ChronoUnit.MINUTES);
        LocalDateTime t9MinutesInTheFuture = now().plus(9, ChronoUnit.MINUTES);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", t5MinutesAgo, true);  // delete
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", t2MinutesAgo, true);  // delete
        insertEvent(tables, 3L, "queue_1", "partition_1", "data", t1MinutesAgo, false); // keep

        // perform
        storageProvider.removeProcessedEvents("queue_1", t9MinutesInTheFuture);

        // check
        List<EventRow> events = fetchEventsFromDatabase(tables);
        assertThat(events, hasSize(1));
        assertThat(events.stream().anyMatch(e -> e.id.equals(1L)), is(false));
        assertThat(events.stream().anyMatch(e -> e.id.equals(2L)), is(false));
        assertThat(events.stream().anyMatch(e -> e.id.equals(3L)), is(true));
    }

    private LocalDateTime now() {
        return LocalDateTime.now(Clock.systemUTC());
    }

    private void insertEvent(DatabaseTables tables,
                             Long id,
                             String queue,
                             String partition,
                             String data,
                             LocalDateTime createdUtc,
                             Boolean processed) {
        database.getJdbcTemplate()
                .update("INSERT INTO " + tables.event() +
                " (id, queue, partition, data, created_utc, processed) " +
                " VALUES (?, ?, ?, ?, ?, ?)", id, queue, partition, data, Timestamp.valueOf(createdUtc), processed);
    }

    private void insertLock(DatabaseTables tables,
                            Long lockId,
                            String queue,
                            String partition,
                            LocalDateTime lockUntil) {
        database.getJdbcTemplate()
                .update("INSERT INTO " + tables.lock() + " " +
                " (id, queue, partition, locked_until_utc) " +
                " VALUES (?, ?, ?, ?)", lockId, queue, partition, Timestamp.valueOf(lockUntil));
    }

    private List<EventRow> fetchEventsFromDatabase(DatabaseTables tables) {
        return database.getJdbcTemplate().query("SELECT * FROM " + tables.event(), new EventRowMapper());
    }

    private List<LockRow> fetchLocksFromDatabase(DatabaseTables tables) {
        return database.getJdbcTemplate().query("SELECT * FROM " + tables.lock(), new LockRowMapper());
    }

    private static class EventRowMapper implements RowMapper<EventRow> {
        @Override
        public EventRow mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            EventRow eventRow = new EventRow();
            eventRow.id = resultSet.getLong("id");
            eventRow.queue = resultSet.getString("queue");
            eventRow.partition = resultSet.getString("partition");
            eventRow.createdUtc = resultSet.getTimestamp("created_utc").toLocalDateTime();
            eventRow.data = resultSet.getString("data");
            eventRow.processed = resultSet.getBoolean("processed");
            return eventRow;
        }
    }

    private static class EventRow {
        Long id;
        String queue;
        String partition;
        LocalDateTime createdUtc;
        String data;
        Boolean processed;
    }

    private static class LockRowMapper implements RowMapper<LockRow> {
        @Override
        public LockRow mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            LockRow lockRow = new LockRow();
            lockRow.id = resultSet.getLong("id");
            lockRow.queue = resultSet.getString("queue");
            lockRow.partition = resultSet.getString("partition");
            lockRow.lockedUntilUtc = resultSet.getTimestamp("locked_until_utc").toLocalDateTime();
            return lockRow;
        }
    }

    private static class LockRow {
        Long id;
        String queue;
        String partition;
        LocalDateTime lockedUntilUtc;
    }

}
