package de.tuda.progressive.db.benchmark.adapter.impl;

import de.tuda.progressive.db.benchmark.adapter.AbstractJdbcAdapter;

public class MonetDBAdapter extends AbstractJdbcAdapter {

	private static final String PART_COLUMN_NAME = "id";
	private static final String PART_COLUMN_DEF = String.format("%s int auto_increment", PART_COLUMN_NAME);

	private static final String COPY_TPL = "copy into %s from '%s' delimiters '|', '\n', ''";
	private static final String ANALYZE_TPL = "analyze sys.%s";
	private static final String INSERT_FROM_TPL = "insert into %s select * from %s";
	private static final String INSERT_PART_TPL = String.format(INSERT_FROM_TPL, "%s", "%s where %s %% %s = %s");
	private static final String ALTER_TABLE_TPL = "alter table %s %s column %s";

	public MonetDBAdapter(String url) {
		super(url);
	}

	@Override
	protected String getCopyTemplate() {
		return COPY_TPL;
	}

	@Override
	public void splitTable(String table, int partitions) {
		final String tmpTable = String.format("%s_tmp", table);

		createTable(tmpTable);
		execute(INSERT_FROM_TPL, tmpTable, table);
		execute(ALTER_TABLE_TPL, tmpTable, "add", PART_COLUMN_DEF);

		for (int i = 0; i < partitions; i++) {
			final String partitionName = getPartitionTable(table, i);

			createTable(partitionName, PART_COLUMN_DEF);
			execute(INSERT_PART_TPL, partitionName, tmpTable, PART_COLUMN_NAME, partitions, i);
			execute(ALTER_TABLE_TPL, partitionName, "drop", PART_COLUMN_NAME);
			analyze(partitionName);
		}
		dropTable(tmpTable);
	}

	@Override
	public void analyze(String table) {
		execute(ANALYZE_TPL, table);
	}

	@Override
	protected String escapePath(String path) {
		return path.replace("\\", "\\\\");
	}

	@Override
	public String getDriverName() {
		return "monetdb";
	}
}
