package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.MemoryMetaData;
import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PostgresqlDriverTest {

	private static final String URL = "jdbc:postgresql://localhost:5432/progressive";
	private static final String USER = "postgres";
	private static final String PASSWORD = "postgres";

	private static final String TABLE_NAME = "test";

	private static Connection connection;

	private static SqlDialect dialect;

	@BeforeAll
	static void init() throws SQLException {
		connection = DriverManager.getConnection(URL, USER, PASSWORD);
		dialect = new PostgresqlSqlDialect(PostgresqlSqlDialect.EMPTY_CONTEXT);
	}

	@BeforeEach
	void setUp() throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(String.format("drop table if exists %s", TABLE_NAME));
			statement.execute(String.format("create table %s (a integer, b varchar(100))", TABLE_NAME));
			statement.execute(String.format("insert into %s values (1, 'a')", TABLE_NAME));
			statement.execute(String.format("insert into %s values (2, 'b')", TABLE_NAME));
			statement.execute(String.format("insert into %s values (3, 'c')", TABLE_NAME));
		}
	}

	@AfterAll
	static void clean() throws SQLException {
		if (connection != null) {
			connection.close();
		}
	}

	@Test
	void testGetCount() {
		final PostgresqlDriver driver = new PostgresqlDriver(dialect);

		assertEquals(3, driver.getCount(connection, TABLE_NAME));
	}

	@Test
	void testPrepare1() {
		final PostgresqlDriver driver = new PostgresqlDriver.Builder(dialect)
				.partitionSize(1)
				.build();

		final MetaData metaData = new MemoryMetaData();
		driver.prepareTable(connection, TABLE_NAME, metaData);
		final List<Partition> partitions = metaData.getPartitions(TABLE_NAME);

		assertEquals(3, partitions.size());
		partitions.forEach(p -> assertEquals(1, p.getEntries()));

		assertColumn(metaData, "a", new Column(TABLE_NAME, "a", 1, 3));
		assertColumn(metaData, "b", null);
	}

	@Test
	void testPrepare2() {
		final PostgresqlDriver driver = new PostgresqlDriver.Builder(dialect)
				.partitionSize(2)
				.build();

		final MetaData metaData = new MemoryMetaData();
		driver.prepareTable(connection, TABLE_NAME, metaData);
		final List<Partition> partitions = metaData.getPartitions(TABLE_NAME);

		assertEquals(2, partitions.size());
		partitions.sort(Comparator.comparingInt(p -> (int) p.getEntries()));
		assertEquals(1, partitions.get(0).getEntries());
		assertEquals(2, partitions.get(1).getEntries());

		assertColumn(metaData, "a", new Column(TABLE_NAME, "a", 1, 3));
		assertColumn(metaData, "b", null);
	}

	private void assertColumn(MetaData metaData, String columnName, Column expected) {
		final Column column = metaData.getColumn(TABLE_NAME, columnName);

		if (expected == null) {
			assertNull(column);
		} else {
			assertNotNull(column);
			assertEquals(expected.getMin(), column.getMin());
			assertEquals(expected.getMax(), column.getMax());
		}
	}
}