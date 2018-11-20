package de.tuda.progressive.db.benchmark.adapter.impl;

import java.util.Arrays;

public class PostgresRangePartitionAdapter extends AbstractPostgresAdapter {

	private static final String ID_COLUMN_NAME = "_id";
	private static final String ID_COLUMN_DEF = String.format("%s serial", ID_COLUMN_NAME);
	private static final String PART_COLUMN_NAME = "_partition";
	private static final String PART_COLUMN_DEF = String.format("%s integer not null", PART_COLUMN_NAME);
	private static final String PART_DEF = String.format("partition by list(%s)", PART_COLUMN_NAME);

	private static final String DROP_AUTO_COLUMN = String.format("alter table %%s drop column if exists %s", ID_COLUMN_NAME);
	private static final String ADD_AUTO_COLUMN = String.format("alter table %%s add column %s", ID_COLUMN_DEF);

	private static final String PARTITION_TPL = "create table %s partition of %s for values in (%s)";
	private static final String INSERT_FROM_TPL = String.format("insert into %%s select t.*, t.%s %%%% %%s from %%s t", ID_COLUMN_NAME);

	public PostgresRangePartitionAdapter(String url) {
		super(url);
	}

	@Override
	public void splitTable(String table, int partitions) {
		final String tmpTable = getTmpTable(table);

		execute(DROP_AUTO_COLUMN, table);
		execute(ADD_AUTO_COLUMN, table);

		createTable(tmpTable, Arrays.asList(ID_COLUMN_DEF, PART_COLUMN_DEF), "", PART_DEF);

		for (int i = 0; i < partitions; i++) {
			final String partitionName = getPartitionTable(table, i);
			dropTable(partitionName);
			execute(PARTITION_TPL, partitionName, tmpTable, i);
		}

		execute(INSERT_FROM_TPL, tmpTable, partitions, table);
		analyze(tmpTable);
	}

	@Override
	public void cleanup(String table, int partitions) {
		final String tmpTable = getTmpTable(table);

		execute(DROP_AUTO_COLUMN, table);
		dropTable(tmpTable);
	}
}
