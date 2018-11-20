package de.tuda.progressive.db.benchmark.utils;

import de.tuda.progressive.db.benchmark.adapter.JdbcAdapter;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AdapterUtils {

	private AdapterUtils() {
	}

	public static List<String> getPartitionTables(JdbcAdapter adapter, String table, int partitions) {
		return IntStream.range(0, partitions)
				.mapToObj(i -> adapter.getPartitionTable(table, i))
				.collect(Collectors.toList());
	}
}
