package at.benjaminhubert.squeuel.jdbctemplate;

import at.benjaminhubert.squeuel.core.Event;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import static org.exparity.hamcrest.date.LocalDateTimeMatchers.within;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class JdbcStorageProviderTest {

    private PostgreSQLContainer database;
    private JdbcTemplate jdbcTemplate;

    @BeforeAll
    void startupDatabase() {
        database = new PostgreSQLContainer("postgres:11");
        database.start();
        DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource();
        driverManagerDataSource.setUrl(database.getJdbcUrl());
        driverManagerDataSource.setUsername(database.getUsername());
        driverManagerDataSource.setPassword(database.getPassword());
        driverManagerDataSource.setDriverClassName(database.getDriverClassName());
        jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(driverManagerDataSource);
    }

    @AfterAll
    void shutdownDatabase() {
        database.stop();
    }

    private DatabaseTables createTables() {
        DatabaseTables tables = new DatabaseTables();
        jdbcTemplate.execute("CREATE TABLE " + tables.event() + " ( " +
                "  id BIGSERIAL PRIMARY KEY, " +
                "  queue VARCHAR(64) NOT NULL, " +
                "  partition VARCHAR(64) NOT NULL, " +
                "  created_utc TIMESTAMP WITHOUT TIME ZONE NOT NULL, " +
                "  data TEXT, " +
                "  processed BOOLEAN NOT NULL " +
                " )");
        jdbcTemplate.execute("CREATE TABLE " + tables.lock() + " ( " +
                "  id BIGSERIAL PRIMARY KEY, " +
                "  queue VARCHAR(64) NOT NULL, " +
                "  partition VARCHAR(64) NOT NULL, " +
                "  locked_until_utc TIMESTAMP WITHOUT TIME ZONE NOT NULL, " +
                "  UNIQUE (queue, partition)  " +
                " )");
        return tables;
    }

    private JdbcStorageProvider initializeStorageProvider(DatabaseTables tables) {
        return new JdbcStorageProvider(jdbcTemplate, tables.event(), tables.lock());
    }

    @Test
    void saveEvent_savesEventAsExpected() {
        // prepare
        DatabaseTables tables = createTables();
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
        DatabaseTables tables = createTables();
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
        DatabaseTables tables = createTables();
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
        DatabaseTables tables = createTables();
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
        DatabaseTables tables = createTables();
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
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(5, ChronoUnit.MINUTES), false);
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_1", "partition_3", "data", now().minus(3, ChronoUnit.MINUTES), false);
        insertEvent(tables, 4L, "queue_1", "partition_4", "data", now().minus(2, ChronoUnit.MINUTES), false);
        insertEvent(tables, 5L, "queue_1", "partition_5", "data", now().minus(1, ChronoUnit.MINUTES), false);

        // perform
        List<Long> events = storageProvider.findNextAvailableEvents("queue_1", 3);

        // check
        assertThat(events, hasSize(3));
        assertThat(events.get(0), equalTo(1L));
        assertThat(events.get(1), equalTo(2L));
        assertThat(events.get(2), equalTo(3L));
    }

    @Test
    void findNextAvailableEvents_returnsNoLockedEvents() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(5, ChronoUnit.MINUTES), false);
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_1", "partition_3", "data", now().minus(3, ChronoUnit.MINUTES), false);
        insertEvent(tables, 4L, "queue_1", "partition_4", "data", now().minus(2, ChronoUnit.MINUTES), false);
        insertEvent(tables, 5L, "queue_1", "partition_5", "data", now().minus(1, ChronoUnit.MINUTES), false);
        insertLock(tables, 1L, "queue_1", "partition_2", now().plus(1, ChronoUnit.HOURS));

        // perform
        List<Long> events = storageProvider.findNextAvailableEvents("queue_1", 3);

        // check
        assertThat(events, hasSize(3));
        assertThat(events.get(0), equalTo(1L));
        assertThat(events.get(1), equalTo(3L));
        assertThat(events.get(2), equalTo(4L));
    }

    @Test
    void findNextAvailableEvents_returnsNoEventsOfALockedPartition() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(5, ChronoUnit.MINUTES), false);
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_1", "partition_1", "data", now().minus(3, ChronoUnit.MINUTES), false);
        insertEvent(tables, 4L, "queue_1", "partition_2", "data", now().minus(2, ChronoUnit.MINUTES), false);
        insertLock(tables, 1L, "queue_1", "partition_2", now().plus(1, ChronoUnit.HOURS));

        // perform
        List<Long> events = storageProvider.findNextAvailableEvents("queue_1", 5);

        // check
        assertThat(events, hasSize(2));
        assertThat(events.get(0), equalTo(1L));
        assertThat(events.get(1), equalTo(3L));
    }

    @Test
    void findNextAvailableEvents_returnsEventsOfAPartitionWhichHasTheSameNameAsALockedPartitionOfAnotherQueue() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(5, ChronoUnit.MINUTES), false);
        insertEvent(tables, 2L, "queue_2", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_1", "partition_2", "data", now().minus(3, ChronoUnit.MINUTES), false);
        insertEvent(tables, 4L, "queue_1", "partition_1", "data", now().minus(2, ChronoUnit.MINUTES), false);
        insertLock(tables, 1L, "queue_1", "partition_2", LocalDateTime.now(Clock.systemDefaultZone()).plus(1, ChronoUnit.HOURS));

        // perform
        List<Long> eventsOfQueue1 = storageProvider.findNextAvailableEvents("queue_1", 5);
        List<Long> eventsOfQueue2 = storageProvider.findNextAvailableEvents("queue_2", 5);

        // check
        assertThat(eventsOfQueue1, hasSize(2));
        assertThat(eventsOfQueue1.get(0), equalTo(1L));
        assertThat(eventsOfQueue1.get(1), equalTo(4L));
        assertThat(eventsOfQueue2, hasSize(1));
        assertThat(eventsOfQueue2.get(0), equalTo(2L));
    }

    @Test
    void findNextAvailableEvents_doesNotReturnAlreadyProcessedEvents() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(5, ChronoUnit.MINUTES), true);
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_1", "partition_3", "data", now().minus(3, ChronoUnit.MINUTES), false);

        // perform
        List<Long> events = storageProvider.findNextAvailableEvents("queue_1", 5);

        // check
        assertThat(events, hasSize(2));
        assertThat(events.get(0), equalTo(2L));
        assertThat(events.get(1), equalTo(3L));
    }

    @Test
    void findNextAvailableEvents_returnsEventsInCorrectOrder() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", now().minus(3, ChronoUnit.MINUTES), false);
        insertEvent(tables, 3L, "queue_1", "partition_3", "data", now().minus(5, ChronoUnit.MINUTES), false);

        // perform
        List<Long> events = storageProvider.findNextAvailableEvents("queue_1", 5);

        // check
        assertThat(events, hasSize(3));
        assertThat(events.get(0), equalTo(3L));
        assertThat(events.get(1), equalTo(1L));
        assertThat(events.get(2), equalTo(2L));
    }

    @Test
    void findNextAvailableEvents_ReturnsEventsWithOutdatedLocks() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(4, ChronoUnit.HOURS), false);
        insertEvent(tables, 2L, "queue_1", "partition_2", "data", now().minus(3, ChronoUnit.HOURS), false);
        insertLock(tables, 1L, "queue_1", "partition_2", now().minus(1, ChronoUnit.HOURS));

        // perform
        List<Long> events = storageProvider.findNextAvailableEvents("queue_1", 5);

        // check
        assertThat(events, hasSize(2));
        assertThat(events.get(0), equalTo(1L));
        assertThat(events.get(1), equalTo(2L));
    }

    @Test
    void findNextAvailableEvents_validatesParameters() {
        // prepare
        DatabaseTables tables = createTables();
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
    void fetchEvent_fetchesEventFromDatabase() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 234L, "queue_1", "partition_1", "data123", now(), true);

        // perform
        Optional<Event> event = storageProvider.fetchEvent(234L);

        // verify
        assertThat(event.isPresent(), equalTo(true));
        assertThat(event.get().getId(), equalTo(234L));
        assertThat(event.get().getQueue(), equalTo("queue_1"));
        assertThat(event.get().getPartition(), equalTo("partition_1"));
        assertThat(event.get().getData(), equalTo("data123"));
        assertThat(event.get().getCreatedUtc(), within(1, ChronoUnit.MINUTES, now()));
        assertThat(event.get().getProcessed(), equalTo(true));
    }

    @Test
    void fetchEvent_fetchesNothingIfEventDoesNotExist() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        Optional<Event> event = storageProvider.fetchEvent(1L);

        // verify
        assertThat(event.isPresent(), equalTo(false));
    }

    @Test
    void fetchEvent_validatesParameters() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        assertThrows(IllegalArgumentException.class, () -> storageProvider.fetchEvent(null));
    }

    @Test
    void lockEvent_insertsLockAsExpected() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 12L, "queue_1", "partition_1", "data", now().minus(4, ChronoUnit.MINUTES), false);

        // perform
        Boolean locked = storageProvider.lockEvent(12L, now().plusHours(1));

        // check
        assertThat(locked, equalTo(true));
        List<LockRow> lockRows = fetchLocksFromDatabase(tables);
        assertThat(lockRows, hasSize(1));
        assertThat(lockRows.get(0).queue, equalTo("queue_1"));
        assertThat(lockRows.get(0).partition, equalTo("partition_1"));
        assertThat(lockRows.get(0).lockedUntilUtc, within(1, ChronoUnit.MINUTES, now().plusHours(1)));
    }

    @Test
    void lockEvent_doesNotLockIfAlreadyLocked() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 12L, "queue_1", "partition_1", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertLock(tables, 233L, "queue_1", "partition_1", now().plusHours(1));

        // perform
        Boolean locked = storageProvider.lockEvent(12L, now().plusHours(2));

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
    void lockEvent_doesNotLockAnythingIfEventDoesNotExist() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        Boolean locked = storageProvider.lockEvent(12L, now().plusHours(2));

        // check
        assertThat(locked, equalTo(false));
        List<LockRow> lockRows = fetchLocksFromDatabase(tables);
        assertThat(lockRows, hasSize(0));
    }

    @Test
    void lockEvent_locksIfPrevoiusLockIsAlreadyOutdated() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);
        insertEvent(tables, 73L, "queue_1", "partition_1", "data", now().minus(4, ChronoUnit.MINUTES), false);
        insertLock(tables, 233L, "queue_1", "partition_1", now().minusHours(1));

        // perform
        Boolean locked = storageProvider.lockEvent(73L, now().plusHours(2));

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
    void lockEvent_doesNotLockAlreadyProcessedEvent() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        insertEvent(tables, 1L, "queue_1", "partition_1", "data", now().minus(4, ChronoUnit.MINUTES), true);
        boolean locked = storageProvider.lockEvent(1L, now().plusHours(1));

        // check
        assertThat(locked, equalTo(false));
    }

    @Test
    void lockEvent_validatesParameters() {
        // prepare
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        assertThrows(IllegalArgumentException.class, () -> storageProvider.lockEvent(null, now()));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.lockEvent(123L, null));
        assertThrows(IllegalArgumentException.class, () -> storageProvider.lockEvent(123L, now().minusMinutes(1)));
    }

    @Test
    void markAsProcessed_marksEventAsProcessed() {
        // prepare
        DatabaseTables tables = createTables();
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
        DatabaseTables tables = createTables();
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
        DatabaseTables tables = createTables();
        JdbcStorageProvider storageProvider = initializeStorageProvider(tables);

        // perform
        assertThrows(IllegalArgumentException.class, () -> storageProvider.markAsProcessed(null));
    }

    // TODO Test removeProcessedEvents

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
        jdbcTemplate.update("INSERT INTO " + tables.event() +
                " (id, queue, partition, data, created_utc, processed) " +
                " VALUES (?, ?, ?, ?, ?, ?)", id, queue, partition, data, Timestamp.valueOf(createdUtc), processed);
    }

    private void insertLock(DatabaseTables tables,
                            Long lockId,
                            String queue,
                            String partition,
                            LocalDateTime lockUntil) {
        jdbcTemplate.update("INSERT INTO " + tables.lock() + " " +
                " (id, queue, partition, locked_until_utc) " +
                " VALUES (?, ?, ?, ?)", lockId, queue, partition, Timestamp.valueOf(lockUntil));
    }

    private List<EventRow> fetchEventsFromDatabase(DatabaseTables tables) {
        return jdbcTemplate.query("SELECT * FROM " + tables.event(), new EventRowMapper());
    }

    private List<LockRow> fetchLocksFromDatabase(DatabaseTables tables) {
        return jdbcTemplate.query("SELECT * FROM " + tables.lock(), new LockRowMapper());
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
