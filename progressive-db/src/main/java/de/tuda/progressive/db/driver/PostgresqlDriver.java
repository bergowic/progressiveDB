package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class PostgresqlDriver extends AbstractDriver {

	private static final int PARTITION_SIZE = 1000000;

	private static final String PART_COLUMN_NAME = "_partition";
	private static final String PART_DEF = String.format("partition by list(%s)", PART_COLUMN_NAME);

	private static final String PARTITION_TPL = "create table %s partition of %s for values in (%d)";
	private static final String INSERT_FROM_TPL = "insert into %s select t.*, row_number() over() %% %d from %s t";

	private static final SqlNode PARTITION_COLUMN = SqlUtils.createColumn(PART_COLUMN_NAME, SqlTypeName.INTEGER, 8, 0);

	private int partitionSize = PARTITION_SIZE;

	public PostgresqlDriver(SqlDialect dialect) {
		super(dialect);
	}

	@Override
	protected List<Partition> split(Connection connection, String table) {
		final String partitionTable = getPartitionTable(table);
		final int partitionCount = getPartitionCount(connection, table);

		dropPartitionTable(connection, partitionTable);
		createPartitionTable(connection, table, partitionTable);
		for (int i = 0; i < partitionCount; i++) {
			final String partitionName = getPartitionTable(table, i);
			createPartition(connection, partitionTable, partitionName, i);
		}
		insertData(connection, table, partitionTable, partitionCount);

		return getPartitions(connection, table, partitionCount);
	}

	private int getPartitionCount(Connection connection, String table) {
		final long count = getCount(connection, table);
		return (int) Math.ceil(((double) count / (double) partitionSize));
	}

	private String getPartitionTable(String table) {
		return table + PART_COLUMN_NAME;
	}

	private String getPartitionTable(String table, int id) {
		return String.format("%s_%d", getPartitionTable(table), id);
	}

	private List<Partition> getPartitions(Connection connection, String table, int partitionCount) {
		final List<Partition> partitions = new ArrayList<>();
		for (int i = 0; i < partitionCount; i++) {
			final String partitionName = getPartitionTable(table, i);
			final Partition partition = new Partition();
			partition.setSrcTable(table);
			partition.setTableName(partitionName);
			partition.setId(i);
			partition.setEntries(getCount(connection, partitionName));
			partitions.add(partition);
		}
		return partitions;
	}

	private void dropPartitionTable(Connection connection, String partitionTable) {
		try (Statement statement = connection.createStatement()) {
			final String sql = toSql(SqlUtils.dropTable(partitionTable));
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void createPartitionTable(Connection connection, String srcTable, String destTable) {
		try (PreparedStatement srcStatement = connection.prepareStatement(getSelectAll(srcTable))) {
			final ResultSetMetaData metaData = srcStatement.getMetaData();
			final SqlCreateTable createTable = SqlUtils.createTable(this, destTable, metaData, PARTITION_COLUMN);
			final String createTableSql = String.format("%s %s", toSql(createTable), PART_DEF);

			try (Statement destStatement = connection.createStatement()) {
				destStatement.execute(createTableSql);
			}
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void createPartition(Connection connection, String parentTable, String partitionTable, int id) {
		try (Statement statement = connection.createStatement()) {
			final String sql = String.format(PARTITION_TPL, partitionTable, parentTable, id);
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void insertData(Connection connection, String srcTable, String destTable, int partitions) {
		try (Statement statement = connection.createStatement()) {
			final String sql = String.format(INSERT_FROM_TPL, destTable, partitions, srcTable);
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	public static class Builder {
		private final PostgresqlDriver driver;

		public Builder(SqlDialect dialect) {
			this.driver = new PostgresqlDriver(dialect);
		}

		public Builder partitionSize(int partitionSize) {
			driver.partitionSize = partitionSize;
			return this;
		}

		public PostgresqlDriver build() {
			return driver;
		}
	}
}
