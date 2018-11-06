package de.tuda.progressive.db.benchmark.adapter.impl;

import de.tuda.progressive.db.benchmark.adapter.AbstractJdbcAdapter;

public class PostgresAdapter extends AbstractJdbcAdapter {

	private static final String PART_COLUMN_NAME = "id";
	private static final String PART_COLUMN_DEF = String.format("%s serial", PART_COLUMN_NAME);
	private static final String PART_DEF = String.format("partition by hash(%s)", PART_COLUMN_NAME);

	private static final String COPY_TPL = "copy %s from '%s' delimiter '|' null ''";
	private static final String PARTITION_TPL = "create table %s partition of %s for values with (modulus %d, remainder %d)";
	private static final String ANALYZE_TPL = "analyze %s";
	private static final String INSERT_FROM_TPL = "insert into %s select * from %s";


	public PostgresAdapter(String url) {
		super(url);
	}

	@Override
	protected String getCopyTemplate() {
		return COPY_TPL;
	}

	@Override
	public void splitTable(String table, int partitions) {
		final String tmpTable = String.format("%s_tmp", table);
		createTable(tmpTable, PART_COLUMN_DEF, "", PART_DEF);

		for (int i = 0; i < partitions; i++) {
			final String partitionName = getPartitionTable(table, i);
			dropTable(partitionName);
			execute(PARTITION_TPL, partitionName, tmpTable, partitions, i);
		}

		execute(INSERT_FROM_TPL, tmpTable, table);
		analyze(tmpTable);
	}

	@Override
	public void analyze(String table) {
		execute(ANALYZE_TPL, table);
	}

	@Override
	protected String escapePath(String path) {
		return path;
	}

	@Override
	public String getDriverName() {
		return "postgresql";
	}
}
