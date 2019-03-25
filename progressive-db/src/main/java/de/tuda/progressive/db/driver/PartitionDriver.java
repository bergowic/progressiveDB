package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public abstract class PartitionDriver extends AbstractDriver {

  private static final Logger log = LoggerFactory.getLogger(PartitionDriver.class);

  public PartitionDriver(SqlDialect dialect, int partitionSize) {
    super(dialect, partitionSize);
  }

  @Override
  protected List<Partition> split(Connection connection, String table) {
    final String partitionTable = getPartitionTable(table);
    log.info("get count of partitions of table {} with size {}", table, partitionSize);
    final int partitionCount = getPartitionCount(connection, table, partitionSize);
    log.info("create {} partitions", partitionCount);

    dropPartitionTable(connection, partitionTable);
    createPartitions(connection, table, partitionCount);

    log.info("insert data");
    insertData(connection, table, partitionCount);

    log.info("read meta data");
    return getPartitions(connection, table, partitionCount);
  }

  protected int getPartitionCount(Connection connection, String table, int partitionSize) {
    final long count = getCount(connection, table);
    return (int) Math.ceil(((double) count / (double) partitionSize));
  }

  protected String getPartitionTable(String table, int id) {
    return String.format("%s_%d", getPartitionTable(table), id);
  }

  protected void dropPartitionTable(Connection connection, String partitionTable) {
    try (Statement statement = connection.createStatement()) {
      final String sql = toSql(SqlUtils.dropTable(partitionTable));
      statement.execute(sql);
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  protected List<Partition> getPartitions(Connection connection, String table, int partitionCount) {
    final List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < partitionCount; i++) {
      final String partitionName = getPartitionTable(table, i);
      final Partition partition = new Partition();
      partition.setSrcTable(table);
      partition.setTableName(partitionName);
      partition.setId(i);
      partition.setEntries(getCount(connection, getPartitionTable(table), i, PART_COLUMN_NAME));
      partitions.add(partition);
    }
    return partitions;
  }

  protected abstract void createPartitions(Connection connection, String table, int partitions);

  protected abstract void insertData(Connection connection, String table, int partitions);

  protected void insertData(
      String template, Connection connection, String srcTable, String destTable, int partitions) {
    try (Statement statement = connection.createStatement()) {
      final String sql = String.format(template, destTable, partitions, srcTable);
      statement.execute(sql);
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }
}
