package de.tuda.progressive.db.benchmark;

import de.tuda.progressive.db.benchmark.adapter.JdbcAdapter;
import de.tuda.progressive.db.benchmark.adapter.impl.PostgresAdapter;
import de.tuda.progressive.db.benchmark.utils.IOUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {

	private static final org.slf4j.Logger log = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) throws Throwable {
		final String table = "lineorder_full";
		final int partitionSize = 1000000;
		final File dataDir = new File(args[0]);
		final List<String> queries = loadQueries(new File(args[1]));
		final String url = args[2];
		final Benchmark benchmark = new SimpleBenchmark();

		try (JdbcAdapter adapter = new PostgresAdapter(url)) {
			adapter.createTable(table);

			log.info("loading data");
			for (File file : dataDir.listFiles()) {
				log.info("load file: {}", file.getName());
				adapter.copy(table, file);
			}
			log.info("data loaded");

			log.info("analyze table");
			adapter.analyze(table);
			log.info("table analyzed");

			final int count = adapter.getCount(table);
			final int partitions = (int) Math.ceil(((double) count / (double) partitionSize));
			log.info("table contains {} entries", count);
			log.info("create {} partitions with size: {}", partitions, partitionSize);

			log.info("execute base benchmarks");
			List<Long> baseTimes = benchmark.run(adapter, table, queries);
			log.info("benchmarks executed");

			log.info("split table");
			adapter.splitTable(table, partitions);
			log.info("table split");

			List<String> partitionTables = IntStream.range(0, partitions)
					.mapToObj(i -> adapter.getPartitionTable(table, i))
					.collect(Collectors.toList());

			log.info("execute partitions benchmarks");
			List<Long> partitionsTimes = benchmark.run(adapter, partitionTables, queries);
			log.info("benchmarks executed");

			for (int i = 0; i < queries.size(); i++) {
				long partitionsTime = partitionsTimes.get(i);

				log.info("query-{}: base table took {}ms", i, baseTimes.get(i));
				log.info("query-{}: partitions tables took {}ms", i, partitionsTime);
				log.info("query-{}: partitions tables took in average {}ms", i, partitionsTime / partitions);
			}
		}
	}

	private static List<String> loadQueries(File dir) {
		return Arrays.stream(dir.listFiles())
				.map(IOUtils::read)
				.collect(Collectors.toList());
	}
}
