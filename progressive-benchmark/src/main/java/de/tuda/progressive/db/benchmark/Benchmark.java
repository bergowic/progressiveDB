package de.tuda.progressive.db.benchmark;

import de.tuda.progressive.db.benchmark.adapter.JdbcAdapter;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public interface Benchmark {

	default List<Long> run(JdbcAdapter adapter, String table, List<String> queries) throws SQLException {
		return run(adapter, Collections.singletonList(table), queries);
	}

	List<Long> run(JdbcAdapter adapter, List<String> tables, List<String> queries) throws SQLException;
}
