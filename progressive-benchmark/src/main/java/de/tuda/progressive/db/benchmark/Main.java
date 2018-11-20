package de.tuda.progressive.db.benchmark;

import de.tuda.progressive.db.benchmark.adapter.JdbcAdapter;
import de.tuda.progressive.db.benchmark.adapter.JdbcAdapterFactory;
import de.tuda.progressive.db.benchmark.adapter.SimpleJdbcAdapterFactory;
import de.tuda.progressive.db.benchmark.utils.IOUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class Main {

	private static final org.slf4j.Logger log = LoggerFactory.getLogger(Main.class);

	private static final List<String> csvHeader = Arrays.asList("Query", "Base", "Partitions Sum", "Partitions Avg");

	public static void main(String[] args) throws Throwable {
		final Properties props = IOUtils.loadProperties(new File(args[0]));
		final Benchmark benchmark = new SimpleBenchmark();
		final JdbcAdapterFactory factory = new SimpleJdbcAdapterFactory();

		final String table = props.getProperty("table", "lineorder_full");
		final List<Integer> partitionSizes = getPartitionSizes(props.getProperty("partitionSize"));
		final String dataPath = props.getProperty("dataDir");
		final List<String> queries = loadQueries(new File(props.getProperty("queriesDir")));
		final String url = props.getProperty("url");
		final String type = props.getProperty("type");

		try (JdbcAdapter adapter = factory.create(url, type)) {
			if (dataPath != null) {
				adapter.createTable(table);

				log.info("loading data");
				for (File file : new File(dataPath).listFiles()) {
					log.info("load file: {}", file.getName());
					adapter.copy(table, file);
				}
				log.info("data loaded");

				log.info("analyze table");
				adapter.analyze(table);
				log.info("table analyzed");
			}

			log.info("count entries");
			final int count = adapter.getCount(table);
			log.info("table contains {} entries", count);

			log.info("execute base benchmarks");
			List<Benchmark.Result> baseTimes = benchmark.run(adapter, table, 0, queries);
			log.info("benchmarks executed");

			for (int partitionSize : partitionSizes) {
				final int partitions = (int) Math.ceil(((double) count / (double) partitionSize));

				log.info("create {} partitions with size: {}", partitions, partitionSize);
				adapter.splitTable(table, partitions);
				log.info("table partitioned");

				log.info("execute partitions benchmarks");
				List<Benchmark.Result> partitionsTimes = benchmark.run(adapter, table, partitions, queries);
				log.info("benchmarks executed");

				for (int i = 0; i < queries.size(); i++) {
					Benchmark.Result result = partitionsTimes.get(i);
					long partitionsTime = result.getTime();
					long tableTimes = result.getTableTimes().stream().mapToLong(Long::longValue).sum();

					log.info("query-{}: base table took {}ms", i, baseTimes.get(i).getTime());
					log.info("query-{}: partitions tables took {}ms", i, partitionsTime);
					log.info("query-{}: partitions tables took in average {}ms", i, partitionsTime / partitions);
					log.info("query-{}: partitions tables took individual in average {}ms", i, tableTimes / partitions);
					for (Long time : partitionsTimes.get(i).getTableTimes()) {
						log.info("{}", time);
					}
				}

				final String outPath = props.getProperty("outDir");
				if (outPath != null) {
					File file = new File(outPath, String.format("%s-%d.csv", adapter.getDriverName(), partitionSize));
					if (file.exists()) {
						if (!file.delete()) {
							log.warn("file could not be written: {}", file.getName());
						}
					}

					if (!file.exists() && file.createNewFile()) {
						writeCSV(file, baseTimes, partitionsTimes);
					}
				}

				adapter.cleanup(table, partitions);
			}
		}
	}

	private static void writeCSV(File file, List<Benchmark.Result> baseTimes, List<Benchmark.Result> partitionsTimes) {
		try (OutputStream output = new FileOutputStream(file)) {
			IOUtils.writeCSVRow(output, csvHeader);

			for (int i = 0; i < baseTimes.size(); i++) {
				long baseTime = baseTimes.get(i).getTime();
				Benchmark.Result partitionResult = partitionsTimes.get(i);
				long partitionTime = partitionResult.getTime();

				List<String> row = LongStream.of(i, baseTime, partitionTime, partitionTime / partitionResult.getTableTimes().size())
						.mapToObj(Long::toString)
						.collect(Collectors.toList());

				IOUtils.writeCSVRow(output, row);
			}

			output.flush();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static List<String> loadQueries(File dir) {
		return Arrays.stream(dir.listFiles())
				.map(IOUtils::read)
				.collect(Collectors.toList());
	}

	private static List<Integer> getPartitionSizes(String value) {
		return Arrays.stream(value.split(","))
				.map(Integer::parseInt)
				.collect(Collectors.toList());
	}
}
