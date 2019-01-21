package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.driver.SqliteDriver;
import de.tuda.progressive.db.model.Partition;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

class ContextFactoryTest {

	private static final String TABLE_NAME = "test";
	private static final String CACHE_TABLE_NAME = "cache";
	private static final Partition PARTITION = new Partition(TABLE_NAME, TABLE_NAME, 0, 0);

	private static final DbDriver driver = SqliteDriver.INSTANCE;

	private static Connection connection;

	@BeforeAll
	static void beforeAll() throws SQLException {
		connection = DriverManager.getConnection("jdbc:sqlite::memory:");
	}

	@BeforeEach
	void beforeEach() throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(String.format("drop table if exists %s", TABLE_NAME));
			statement.execute(String.format("drop table if exists %s", CACHE_TABLE_NAME));
			statement.execute(String.format("create table %s (a integer, b integer, c varchar(100), _partition integer)", TABLE_NAME));
			statement.execute(String.format("insert into %s values (2, 4, 'a', 0)", TABLE_NAME));
			statement.execute(String.format("insert into %s values (8, 16, 'b', 1)", TABLE_NAME));
		}
	}

	@AfterAll
	static void afterAll() throws SQLException {
		if (connection != null) {
			connection.close();
		}
	}

	private void assertContextNotNull(StatementContext context) {
		assertNotNull(context);
		assertNotNull(context.getCreateCache());
		assertNotNull(context.getSelectSource());
		assertNotNull(context.getInsertCache());
		assertNotNull(context.getSelectCache());
	}

	private void testSingleAggregation(String sql, int[] expectedValues) throws Exception {
		final SqlSelect select = (SqlSelect) SqlParser.create(sql).parseQuery();
		final StatementContext context = ContextFactory.instance.create(connection, driver, select, PARTITION, CACHE_TABLE_NAME);

		assertContextNotNull(context);

		try (Statement statement = connection.createStatement()) {
			statement.execute(driver.toSql(context.getCreateCache()));

			final String selectSource = driver.toSql(context.getSelectSource());
			try (PreparedStatement sourceSelectStatement = connection.prepareStatement(selectSource)) {
				final String insertCache = driver.toSql(context.getInsertCache());

				try (PreparedStatement insertCacheStatement = connection.prepareStatement(insertCache)) {
					final String selectCache = driver.toSql(context.getSelectCache());

					try (PreparedStatement selectCacheStatement = connection.prepareStatement(selectCache)) {
						for (int i = 0; i < expectedValues.length; i++) {
							sourceSelectStatement.setInt(1, i);
							try (ResultSet result = sourceSelectStatement.executeQuery()) {
								assertTrue(result.next());

								final int columnCount = result.getMetaData().getColumnCount();
								for (int j = 1; j < columnCount; j++) {
									insertCacheStatement.setObject(j, result.getObject(j));
								}
								insertCacheStatement.setObject(columnCount, i);

								assertFalse(insertCacheStatement.execute());
								assertFalse(result.next());
							}

							switch (context.getAggregations().get(0)) {
								case COUNT:
								case SUM:
									selectCacheStatement.setDouble(1, (double) (i + 1) / (double) expectedValues.length);
									selectCacheStatement.setInt(2, i);
									selectCacheStatement.setDouble(3, i / expectedValues.length);
									break;
								default:
									selectCacheStatement.setInt(1, i);
									selectCacheStatement.setDouble(2, i / expectedValues.length);
							}

							try (ResultSet result = selectCacheStatement.executeQuery()) {
								assertTrue(result.next());

								assertEquals(expectedValues[i], result.getInt(1));
								assertEquals(i, result.getInt(2));

								assertFalse(result.next());
							}
						}
					}
				}
			}
		}
	}

	@Test
	void testAvg() throws Exception {
		final String sql = String.format("select avg(a) from %s", TABLE_NAME);
		final int[] expectedValues = {2, 5};

		testSingleAggregation(sql, expectedValues);
	}

	@Test
	void testCount() throws Exception {
		final String sql = String.format("select count(a) from %s", TABLE_NAME);
		final int[] expectedValues = {2, 2};

		testSingleAggregation(sql, expectedValues);
	}

	@Test
	void testSum() throws Exception {
		final String sql = String.format("select sum(a) from %s", TABLE_NAME);
		final int[] expectedValues = {4, 10};

		testSingleAggregation(sql, expectedValues);
	}

	@Test
	void testAvgWhere() throws Exception {
		final String sql = String.format("select avg(a) from %s where c = 'a'", TABLE_NAME);
		final int[] expectedValues = {2, 2};

		testSingleAggregation(sql, expectedValues);
	}

	@Test
	void testCountWhere() throws Exception {
		final String sql = String.format("select count(a) from %s where c = 'a'", TABLE_NAME);
		final int[] expectedValues = {2, 1};

		testSingleAggregation(sql, expectedValues);
	}

	@Test
	void testSumWhere() throws Exception {
		final String sql = String.format("select sum(a) from %s where c = 'a'", TABLE_NAME);
		final int[] expectedValues = {4, 2};

		testSingleAggregation(sql, expectedValues);
	}

	@Test
	void testColumnAlias() throws Exception {
		final String sql = String.format("select sum(a) a from %s where c = 'a'", TABLE_NAME);
		final SqlSelect select = (SqlSelect) SqlParser.create(sql).parseQuery();
		final StatementContext context = ContextFactory.instance.create(connection, driver, select, PARTITION, CACHE_TABLE_NAME);

		assertContextNotNull(context);
	}
}