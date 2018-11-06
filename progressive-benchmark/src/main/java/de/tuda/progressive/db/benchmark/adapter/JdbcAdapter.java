package de.tuda.progressive.db.benchmark.adapter;

import java.io.File;
import java.sql.SQLException;

public interface JdbcAdapter extends AutoCloseable {
	void createTable(String table) throws SQLException;

	void splitTable(String table, int partitions) throws SQLException;

	void copy(String table, File file) throws SQLException;

	int getCount(String table) throws SQLException;

	void benchmark(String table, String query) throws SQLException;

	String getPartitionTable(String table, int partition);

	void analyze(String table) throws SQLException;
}
