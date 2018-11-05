package de.tuda.progressive.db.benchmark.adapter;

import java.io.File;
import java.sql.SQLException;

public interface JdbcAdapter extends AutoCloseable {
	void createTable(String table) throws SQLException;

	void copy(String table, File file) throws SQLException;

	void benchmark(String table, File file) throws SQLException;
}
