package at.benjaminhubert.squeuel.jdbctemplate;

import at.benjaminhubert.squeuel.core.Event;
import at.benjaminhubert.squeuel.core.StorageProvider;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;


public class JdbcStorageProvider implements StorageProvider {

    private static final int QUEUE_NAME_MAX_LENGTH = 64;
    private static final int PARTITION_NAME_MAX_LENGTH = 64;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final String eventTable;
    private final String lockTable;

    public JdbcStorageProvider(JdbcTemplate jdbcTemplate,
                               String eventTable,
                               String lockTable) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        this.eventTable = eventTable;
        this.lockTable = lockTable;
    }

    @Override
    public void saveEvent(String queue, String partition, String data) {
        queue = validateQueue(queue);
        partition = validatePartition(partition);
        data = validateData(data);
        jdbcTemplate.update("INSERT INTO " + eventTable + " ( " +
                "   queue, " +
                "   partition, " +
                "   created_utc, " +
                "   data, " +
                "   processed " +
                " ) VALUES (" +
                "   :queue, " +
                "   :partition," +
                "   :created_utc, " +
                "   :data, " +
                "   :processed " +
                ")", new MapSqlParameterSource()
                .addValue("queue", queue)
                .addValue("partition", partition)
                .addValue("created_utc", Timestamp.valueOf(LocalDateTime.now(Clock.systemUTC())))
                .addValue("data", data)
                .addValue("processed", Boolean.FALSE)
        );
    }

    @Override
    public List<Long> findNextAvailableEvents(String queue, Integer batchSize) {
        queue = validateQueue(queue);
        batchSize = validateBatchSize(batchSize);

        String sql = "SELECT e.id FROM " + eventTable + " e " +
                " LEFT OUTER JOIN " + lockTable + " l ON (e.queue = l.queue) " +
                "                                     AND (e.partition = l.partition) " +
                " WHERE e.queue = :queue " +
                " AND e.processed = FALSE " +
                " AND (" +
                "   l.locked_until_utc IS NULL" +
                "   OR l.locked_until_utc < :now" +
                " ) " +
                " ORDER BY e.created_utc " +
                " LIMIT :batchSize ";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("queue", queue)
                .addValue("batchSize", batchSize)
                .addValue("now", Timestamp.valueOf(LocalDateTime.now(Clock.systemUTC())));

        List<Long> ids = jdbcTemplate.query(sql, params, (rs, i) -> {
            return rs.getLong("id");
        });

        return ids;
    }

    @Override
    public Optional<Event> fetchEvent(Long eventId) {
        eventId = validateEventId(eventId);

        String sql = "SELECT id, queue, partition, created_utc, data, processed " +
                " FROM " + eventTable + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", eventId);

        try {
            Event event = jdbcTemplate.queryForObject(sql, params, (rs, i) -> {
                Long id = rs.getLong("id");
                String queue = rs.getString("queue");
                String partition = rs.getString("partition");
                String data = rs.getString("data");
                LocalDateTime createdUtc = rs.getTimestamp("created_utc").toLocalDateTime();
                Boolean processed = rs.getBoolean("processed");
                return new Event(id, queue, partition, data, createdUtc, processed);
            });
            return Optional.ofNullable(event);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public boolean lockEvent(Long eventId, LocalDateTime lockUntilUtc) {
        eventId = validateEventId(eventId);
        lockUntilUtc = validateLockUntilUtc(lockUntilUtc);
        deleteOldLockForEvent(eventId);
        return insertNewLockForEvent(eventId, lockUntilUtc);
    }

    private void deleteOldLockForEvent(Long eventId) {
        String sql = "DELETE FROM " + lockTable + " ldel " +
                " WHERE EXISTS ( " +
                "   SELECT l.id FROM " + lockTable + " l " +
                "   JOIN " + eventTable + " e ON (l.partition = e.partition AND l.queue = e.queue) " +
                "   WHERE ldel.id = l.id " +
                "   AND e.id = :event_id " +
                "   AND l.locked_until_utc < :now " +
                " )";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("event_id", eventId)
                .addValue("now", Timestamp.valueOf(LocalDateTime.now(Clock.systemUTC())));
        jdbcTemplate.update(sql, params);
    }

    private boolean insertNewLockForEvent(Long eventId, LocalDateTime lockUntilUtc) {
        String sql = "INSERT INTO " + lockTable + " (queue, partition, locked_until_utc)" +
                " SELECT e.queue, e.partition, :locked_until_utc AS locked_until_utc " +
                " FROM " + eventTable + " e" +
                " WHERE e.id = :event_id ";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("event_id", eventId)
                .addValue("locked_until_utc", Timestamp.valueOf(lockUntilUtc));
        try {
            int inserted = jdbcTemplate.update(sql, params);
            return inserted > 0;
        } catch (DataIntegrityViolationException e) {
            return false;
        }
    }

    @Override
    public void markAsProcessed(Long eventId) {
        eventId = validateEventId(eventId);
        String sql = "UPDATE " + eventTable + " SET processed = TRUE WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", eventId);
        jdbcTemplate.update(sql, params);
    }

    @Override
    public void removeProcessedEvents(String queue, LocalDateTime olderThanUtc) {
    }

    private String normalizeString(String value) {
        return value != null ? value.trim() : null;
    }

    private Long validateEventId(Long eventId) {
        if (eventId == null) throw new IllegalArgumentException("Event ID is required but was null");
        return eventId;
    }

    private String validateQueue(String queue) {
        return validateString("Queue name", queue, QUEUE_NAME_MAX_LENGTH);
    }

    private String validatePartition(String partition) {
        return validateString("Queue name", partition, PARTITION_NAME_MAX_LENGTH);
    }

    private String validateString(String fieldName, String value, int maxLength) {
        String normalized = normalizeString(value);
        if (normalized == null) throw new IllegalArgumentException(fieldName + " is required but was null");
        if (normalized.isEmpty()) throw new IllegalArgumentException(fieldName + " is required but was empty");
        if (normalized.length() > maxLength) throw new IllegalArgumentException(fieldName + " is too long: " + value.length() + " (maximum is " + maxLength + ")");
        return normalized;
    }

    private Integer validateBatchSize(Integer batchSize) {
        if (batchSize == null) throw new IllegalArgumentException("Batch size is required but was null");
        if (batchSize <= 0) throw new IllegalArgumentException("Batch size must be a positive integer but was " + batchSize);
        return batchSize;
    }

    private String validateData(String data) {
        if (data == null) throw new IllegalArgumentException("Data is required but was null");
        return data;
    }

    private LocalDateTime validateLockUntilUtc(LocalDateTime lockUntilUtc) {
        if (lockUntilUtc == null) throw new IllegalArgumentException("Timestamp until event should be locked is required but was null");
        if (lockUntilUtc.isBefore(LocalDateTime.now(Clock.systemUTC()))) throw new IllegalArgumentException("Timestamp until event should be locked must be in the future");
        return lockUntilUtc;
    }

}
