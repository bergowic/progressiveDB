package de.tuda.progressive.db.benchmark;

import de.tuda.progressive.db.benchmark.adapter.JdbcAdapter;

import java.util.ArrayList;
import java.util.List;

public class SimpleBenchmark implements Benchmark {

	@Override
	public List<Long> run(JdbcAdapter adapter, List<String> tables, List<String> queries) {
		List<Long> results = new ArrayList<>(queries.size());
		for (String query : queries) {
			long time = 0;
			for (String table : tables) {
				time += run(adapter, table, query);
			}
			results.add(time);
		}
		return results;
	}

	private long run(JdbcAdapter adapter, String table, String query) {
		long start = System.nanoTime();
		adapter.benchmark(table, query);
		long end = System.nanoTime();
		return (end - start) / 1000000;
	}
}
