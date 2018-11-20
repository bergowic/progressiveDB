package de.tuda.progressive.db.benchmark.utils;

import java.sql.SQLException;

public class BenchmarkUtils {

	private BenchmarkUtils() {
	}

	public static long measure(SQLRunnable runnable) {
		try {
			long start = System.nanoTime();
			runnable.run();
			long end = System.nanoTime();
			return (end - start) / 1000000;
		} catch (SQLException e) {
			throw new UncheckedSQLException(e);
		}
	}
}
