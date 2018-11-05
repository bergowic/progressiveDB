package de.tuda.progressive.db.benchmark.adapter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

public abstract class AbstractJdbcAdapter implements JdbcAdapter {

	private static final String TABLE_FILE = "table.sql";

	private static final String DROP_SQL = "drop table if exists %s";

	private final Connection connection;

	public AbstractJdbcAdapter(String url) throws SQLException {
		this.connection = DriverManager.getConnection(url);
	}

	@Override
	public void close() {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				// do nothing
			}
		}
	}

	protected abstract String getCopyTemplate();

	@Override
	public void createTable(String table) throws SQLException {
		final String fileName = String.format("/%s", TABLE_FILE);

		try (InputStream input = getClass().getResourceAsStream(fileName)) {
			final String sql = loadSql(input);

			execute(DROP_SQL, table);
			execute(sql, table);
		} catch (IOException e) {
			throw new IllegalStateException("could not load table definition", e);
		}
	}

	@Override
	public void copy(String table, File file) throws SQLException {
		execute(getCopyTemplate(), table, file.getAbsolutePath());
	}

	@Override
	public void benchmark(String table, File file) throws SQLException {
		final String sql = loadSql(file);
		execute(sql, table);
	}

	private void execute(String template, String... args) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			final String sql = String.format(template, args);
			statement.execute(sql);
		}
	}

	private String loadSql(InputStream input) {
		return new BufferedReader(new InputStreamReader(input))
				.lines().collect(Collectors.joining("\n"));
	}

	private String loadSql(File file) {
		try (InputStream input = new FileInputStream(file)) {
			return loadSql(input);
		} catch (IOException e) {
			throw new IllegalArgumentException("could not read file", e);
		}
	}
}
