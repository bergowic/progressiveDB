package de.tuda.progressive.db.benchmark;

import de.tuda.progressive.db.benchmark.adapter.JdbcAdapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleBenchmark implements Benchmark {

	private static final int RUNS = 10;

	@Override
	public List<Result> run(JdbcAdapter adapter, String table, int partitions, List<String> queries) {
		List<Result> results = new ArrayList<>(queries.size());
		for (String query : queries) {
			Result result = run(adapter, table, partitions, query);
			results.add(result);
		}
		return results;
	}

	private Result run(JdbcAdapter adapter, String table, int partitions, String query) {
		final List<Result> results = new ArrayList<>(RUNS);

		for (int i = 0; i < RUNS; i++) {
			Result result = adapter.benchmark(table, partitions, query);
			results.add(result);
		}

		results.remove(0);
		Collections.sort(results);
		results.remove(0);
		results.remove(results.size() - 1);

		final long totalAverage = (long) results.stream().mapToLong(Result::getTime).average().getAsDouble();
		final List<Long> partitionAverages = IntStream.range(0, partitions)
				.mapToObj(i -> (long) IntStream.range(0, RUNS - 3)
						.mapToLong(j -> results.get(j).getTableTimes().get(i))
						.average()
						.getAsDouble()
				).collect(Collectors.toList());

		return new Result(totalAverage, partitionAverages);
	}
}
