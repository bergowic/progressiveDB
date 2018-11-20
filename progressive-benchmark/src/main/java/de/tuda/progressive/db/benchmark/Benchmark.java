package de.tuda.progressive.db.benchmark;

import de.tuda.progressive.db.benchmark.adapter.JdbcAdapter;

import java.util.Collections;
import java.util.List;

public interface Benchmark {

	List<Result> run(JdbcAdapter adapter, String table, int partitions, List<String> queries);

	class Result implements Comparable<Result> {
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

		@Override
		public int compareTo(Result o) {
			long compare = time - o.time;
			if (compare > 0) {
				return 1;
			}
			if (compare < 0) {
				return -1;
			}
			return 0;
		}
	}
}
