package de.tuda.progressive.db.benchmark.adapter;

import de.tuda.progressive.db.benchmark.utils.IOUtils;
import de.tuda.progressive.db.benchmark.utils.UncheckedSQLException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

public abstract class AbstractJdbcAdapter implements JdbcAdapter {

	private static final String TABLE_FILE = "table.sql";

	private static final String COUNT_SQL = "select count(*) from %s";
	private static final String DROP_SQL = "drop table if exists %s";

	private final Connection connection;

	public AbstractJdbcAdapter(String url) {
		try {
			this.connection = DriverManager.getConnection(url);
		} catch (SQLException e) {
			throw new UncheckedSQLException(e);
		}
	}

	@Override
	public void close() {
		IOUtils.closeSafe(connection);
	}

	protected abstract String getCopyTemplate();

	@Override
	public void createTable(String table) {
		createTable(table, Collections.emptyList(), "");
	}

	protected final void dropTable(String table) {
		execute(DROP_SQL, table);
	}

	protected final void createTable(String table, String column, String suffix) {
		createTable(table, Collections.singletonList(column), suffix);
	}

	protected final void createTable(String table, List<String> columns, String suffix) {
		final String fileName = String.format("/%s", TABLE_FILE);

		try (InputStream input = getClass().getResourceAsStream(fileName)) {
			final String sql = IOUtils.read(input);
			final String columnsDefinition = getColumnsDefinition(columns);

			dropTable(table);
			execute(sql, table, columnsDefinition, suffix);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private String getColumnsDefinition(List<String> columns) {
		if (columns.size() == 0) {
			return "";
		}
		return String.format(", %s", String.join(", ", columns));
	}

	@Override
	public void copy(String table, File file) {
		execute(getCopyTemplate(), table, file.getAbsolutePath());
	}

	@Override
	public int getCount(String table) {
		try (Statement statement = connection.createStatement()) {
			final String sql = String.format(COUNT_SQL, table);
			ResultSet result = statement.executeQuery(sql);
			result.next();
			return result.getInt(1);
		} catch (SQLException e) {
			throw new UncheckedSQLException(e);
		}
	}

	@Override
	public void benchmark(String table, String query) {
		execute(query, table);
	}

	protected final void execute(String template, Object... args) {
		try (Statement statement = connection.createStatement()) {
			final String sql = String.format(template, args);
			statement.execute(sql);
		} catch (SQLException e) {
			throw new UncheckedSQLException(e);
		}
	}

	@Override
	public String getPartitionTable(String table, int partition) {
		return String.format("%s_%d", table, partition);
	}
}
