package at.benjaminhubert.squeuel.testutils;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import javax.sql.DataSource;

/**
 * @author <a href="mailto:code@benjaminhubert.at">Benjamin Hubert</a>
 */
public class Database {

	private final JdbcDatabaseContainer container;
	private final DriverManagerDataSource dataSource;
	private final JdbcTemplate jdbcTemplate;

	public Database(JdbcDatabaseContainer container) {
		this.container = container;
		this.dataSource = initDataSource(container);
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

	public JdbcDatabaseContainer getContainer() {
		return container;
	}

	public DataSource getDataSource() {
		return dataSource;
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
		jdbcTemplate.execute("CREATE UNLOGGED TABLE " + tables.lock() + " ( " +
				"  id BIGSERIAL PRIMARY KEY, " +
				"  queue VARCHAR(64) NOT NULL, " +
				"  partition VARCHAR(64) NOT NULL, " +
				"  locked_until_utc TIMESTAMP WITHOUT TIME ZONE NOT NULL, " +
				"  UNIQUE (queue, partition)  " +
				" )");
		return tables;
	}

	public void stop() {
		container.stop();
	}

	public static Database createPostgresDatabase() {
		PostgreSQLContainer database = new PostgreSQLContainer("postgres:11");
		database.start();
		return new Database(database);
	}

}
