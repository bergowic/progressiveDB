package de.tuda.progressive.db.benchmark;

import de.tuda.progressive.db.benchmark.adapter.JdbcAdapter;

import java.util.Collections;
import java.util.List;

public interface Benchmark {

	List<Result> run(JdbcAdapter adapter, String table, int partitions, List<String> queries);

	class Result {
		private final long time;
		private final List<Long> tableTimes;

		public Result(long time, List<Long> tableTimes) {
			this.time = time;
			this.tableTimes = tableTimes;
		}

		public long getTime() {
			return time;
		}

		public List<Long> getTableTimes() {
			return tableTimes;
		}
	}
}
