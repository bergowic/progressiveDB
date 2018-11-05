package de.tuda.progressive.db.benchmark;

import de.tuda.progressive.db.benchmark.adapter.JdbcAdapter;
import de.tuda.progressive.db.benchmark.adapter.impl.PostgresAdapter;
import org.slf4j.LoggerFactory;

import java.io.File;

public class Main {

	private static final org.slf4j.Logger log = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) throws Throwable {
		final String table = "lineorder_full";
		final File dataDir = new File(args[0]);
		final File scriptsDir = new File(args[1]);
		final String url = args[2];

		if (!dataDir.isDirectory()) {
			throw new IllegalArgumentException("data dir is not valid");
		}

		try (JdbcAdapter adapter = new PostgresAdapter(url)) {
			adapter.createTable(table);

			log.info("loading data");
			for (File file : dataDir.listFiles()) {
				log.info("load file: {}", file.getName());
				adapter.copy(table, file);
			}
			log.info("data loaded");

			for (File file : scriptsDir.listFiles()) {
				long start = System.nanoTime();
				adapter.benchmark(table, file);
				long end = System.nanoTime();

				log.info("{} took: {}ms", file.getName(), (end - start) / 1000000);
			}
		}
	}
}
