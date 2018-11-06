package de.tuda.progressive.db.benchmark.adapter;

import java.io.File;

public interface JdbcAdapter extends AutoCloseable {
	void createTable(String table);

	void splitTable(String table, int partitions);

	void copy(String table, File file);

	int getCount(String table);

	void benchmark(String table, String query);

	String getPartitionTable(String table, int partition);

	void analyze(String table);

	String getDriverName();
}
