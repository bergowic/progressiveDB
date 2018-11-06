package de.tuda.progressive.db.benchmark;

import de.tuda.progressive.db.benchmark.adapter.JdbcAdapter;

import java.util.ArrayList;
import java.util.List;

public class SimpleBenchmark implements Benchmark {

	@Override
	public List<Result> run(JdbcAdapter adapter, List<String> tables, List<String> queries) {
		List<Result> results = new ArrayList<>(queries.size());
		for (String query : queries) {
			List<Long> times = new ArrayList<>(tables.size());
			long timeSum = 0;
			for (String table : tables) {
				long time = run(adapter, table, query);
				timeSum += time;
				times.add(time);
			}

			Result result = new Result(timeSum, times);
			results.add(result);
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
