package de.tuda.progressive.db.benchmark.adapter.impl;

public class PostgresHashPartitionAdapter extends AbstractPostgresAdapter {

	private static final String PART_COLUMN_NAME = "id";
	private static final String PART_COLUMN_DEF = String.format("%s serial", PART_COLUMN_NAME);
	private static final String PART_DEF = String.format("partition by hash(%s)", PART_COLUMN_NAME);

	private static final String PARTITION_TPL = "create table %s partition of %s for values with (modulus %d, remainder %d)";
	private static final String INSERT_FROM_TPL = "insert into %s select * from %s";

	public PostgresHashPartitionAdapter(String url) {
		super(url);
	}

	@Override
	public void splitTable(String table, int partitions) {
		final String tmpTable = getTmpTable(table);
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
	public void cleanup(String table, int partitions) {
		final String tmpTable = getTmpTable(table);
		dropTable(tmpTable);
	}
}
