package at.benjaminhubert.squeuel.testutils;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * @author <a href="mailto:code@benjaminhubert.at">Benjamin Hubert</a>
 */
public class Database {

	private final JdbcDatabaseContainer database;
	private final DriverManagerDataSource dataSource;
	private final JdbcTemplate jdbcTemplate;

	public Database(JdbcDatabaseContainer database) {
		this.database = database;
		this.dataSource = initDataSource(database);
		this.jdbcTemplate = initJdbcTemplate(dataSource);
	}

	private static DriverManagerDataSource initDataSource(JdbcDatabaseContainer database) {
		DriverManagerDataSource ds = new DriverManagerDataSource();
		ds.setUrl(database.getJdbcUrl());
		ds.setUsername(database.getUsername());
		ds.setPassword(database.getPassword());
		ds.setDriverClassName(database.getDriverClassName());
		return ds;
	}

	private static JdbcTemplate initJdbcTemplate(DriverManagerDataSource dataSource) {
		JdbcTemplate jdbcTemplate = new JdbcTemplate();
		jdbcTemplate.setDataSource(dataSource);
		return jdbcTemplate;
	}

	public JdbcDatabaseContainer getDatabase() {
		return database;
	}

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public DatabaseTables createTables() {
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
//		jdbcTemplate.execute("CREATE INDEX ON " + tables.event() + " USING HASH (queue)");
//		jdbcTemplate.execute("CREATE INDEX ON " + tables.event() + " USING HASH (partition)");
		jdbcTemplate.execute("CREATE INDEX ON " + tables.event() + " USING BTREE (id)");
		jdbcTemplate.execute("CREATE INDEX ON " + tables.event() + " USING HASH (queue)");
		jdbcTemplate.execute("CREATE INDEX ON " + tables.event() + " USING BTREE (queue, partition)");
		jdbcTemplate.execute("CREATE INDEX ON " + tables.event() + " USING BTREE (created_utc)");
//		jdbcTemplate.execute("CREATE INDEX ON " + tables.lock() + " USING HASH (queue)");
//		jdbcTemplate.execute("CREATE INDEX ON " + tables.lock() + " USING HASH (partition)");
		jdbcTemplate.execute("CREATE INDEX ON " + tables.lock() + " USING BTREE (queue, partition)");
		jdbcTemplate.execute("CREATE INDEX ON " + tables.lock() + " USING BTREE (locked_until_utc)");
		return tables;
	}

	public void stop() {
		database.stop();
	}

	public static Database createPostgresDatabase() {
		PostgreSQLContainer database = new PostgreSQLContainer("postgres:11");
		database.start();
		return new Database(database);
	}

}
