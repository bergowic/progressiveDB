package de.tuda.progressive.db.benchmark;

import de.tuda.progressive.db.benchmark.adapter.JdbcAdapter;

import java.util.ArrayList;
import java.util.List;

public class SimpleBenchmark implements Benchmark {

	@Override
	public List<Result> run(JdbcAdapter adapter, String table, int partitions, List<String> queries) {
		List<Result> results = new ArrayList<>(queries.size());
		for (String query : queries) {
			Result result = adapter.benchmark(table, partitions, query);
			results.add(result);
		}
		return results;
	}
}
