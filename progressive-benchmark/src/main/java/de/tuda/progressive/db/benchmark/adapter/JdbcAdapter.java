package de.tuda.progressive.db.benchmark.adapter;

import de.tuda.progressive.db.benchmark.Benchmark;

import java.io.File;

public interface JdbcAdapter extends AutoCloseable {
	void createTable(String table);

	void splitTable(String table, int partitions);

	void cleanup(String table, int partitions);

	void copy(String table, File file);

	int getCount(String table);

	Benchmark.Result benchmark(String table, int partitions, String query);

	String getPartitionTable(String table, int partition);

	void analyze(String table);

	String getDriverName();
}
