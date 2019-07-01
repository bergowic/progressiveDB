package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.PartitionInfo;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public abstract class ProgressiveBaseStatement implements ProgressiveStatement<DataBuffer> {

  private static final Logger log = LoggerFactory.getLogger(ProgressiveBaseStatement.class);

  private static final ExecutorService executor = Executors.newCachedThreadPool();

  private final DbDriver driver;

  private final PartitionInfo partitionInfo;

  private PreparedStatement preparedStatement;

  private final LinkedHashMap<String, Integer> currentReadPartitions;

  private int readPartitions = 0;

  protected final ResultSetMetaData metaData;

  protected final DataBuffer dataBuffer;

  private boolean isClosed;

  private final Connection connection;

  private final SqlSelect selectSource;

  private final List<SqlIdentifier> sourceTables;

  public ProgressiveBaseStatement(
      DbDriver driver,
      Connection connection,
      JdbcSourceContext context,
      DataBuffer dataBuffer,
      PartitionInfo partitionInfo) {
    this.driver = driver;
    this.dataBuffer = dataBuffer;
    this.partitionInfo = partitionInfo;
    this.connection = connection;
    this.selectSource = context.getSelectSource();
    this.sourceTables = context.getSourceTables();
    this.currentReadPartitions = getReadPartitions(sourceTables);

    try {
      preparedStatement =
          driver.hasPartitions() ? connection.prepareStatement(driver.toSql(selectSource)) : null;

      metaData = dataBuffer.getMetaData();
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  private LinkedHashMap<String, Integer> getReadPartitions(List<SqlIdentifier> tables) {
    return tables.stream()
        .collect(
            Collectors.toMap(
                SqlIdentifier::getSimple,
                $ -> 0,
                ($1, $2) -> {
                  throw new RuntimeException();
                },
                LinkedHashMap::new));
  }

  @Override
  public void run() {
    startFetching();
  }

  private void startFetching() {
    executor.submit(
        () -> {
          try {
            while (!isDone()) {
              synchronized (this) {
                if (isClosed) {
                  break;
                }
              }

              query();
            }
          } catch (Throwable t) {
            // TODO
            t.printStackTrace();
          }
        });
  }

  private void query() {
    log.info("query next partition: {}", currentReadPartitions);
    try {
      ResultSet resultSet = getResult(currentReadPartitions);

      log.info("data received");
      dataBuffer.add(resultSet);
      log.info("received data handled");

      incPartition();

      queryHandled();
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  private ResultSet getResult(LinkedHashMap<String, Integer> partitions) throws SQLException {
    if (driver.hasPartitions()) {
      int i = 1;
      for (int partitionId : partitions.values()) {
        preparedStatement.setInt(i++, partitionId);
      }

      log.info("next statement {}", preparedStatement.toString().replaceAll("\\r\\n", " "));

      return preparedStatement.executeQuery();
    } else {
      throw new UnsupportedOperationException();
      // TODO
      // final SqlIdentifier from = (SqlIdentifier) selectSource.getFrom();
      //    final String table = driver.getPartitionTable(from.getSimple(), partition.getId());
      // final SqlSelect select = (SqlSelect) selectSource.clone(SqlParserPos.ZERO);
      //    select.setFrom(SqlUtils.getIdentifier(table));
      //
      //    final String sql = driver.toSql(select);
      //  log.info("next statement {}", sql.replaceAll("\\r\\n", " "));
      //
      // try (Statement statement = connection.createStatement()) {
      // return statement.executeQuery(sql);
      // }
    }
  }

  protected abstract void queryHandled();

  @Override
  public ResultSetMetaData getMetaData() {
    return metaData;
  }

  @Override
  public DataBuffer getDataBuffer() {
    return dataBuffer;
  }

  @Override
  public synchronized boolean isDone() {
    return readPartitions == partitionInfo.getPartitionCount();
  }

  @Override
  public synchronized void close() {
    isClosed = true;

    SqlUtils.closeSafe(dataBuffer);
    SqlUtils.closeSafe(preparedStatement);
  }

  protected final synchronized int getReadPartitions() {
    return readPartitions;
  }

  protected final synchronized double getProgress() {
    return (double) readPartitions / (double) partitionInfo.getPartitionCount();
  }

  private synchronized void incPartition() {
    readPartitions++;

    for (SqlIdentifier table : sourceTables) {
      final String tableName = table.getSimple();
      final int readPartition = currentReadPartitions.get(tableName);
      final int maxPartition = partitionInfo.getPartitionCount(tableName) - 1;
      if (maxPartition > 0) {
        if (readPartition == maxPartition) {
          currentReadPartitions.put(tableName, 0);
        } else {
          currentReadPartitions.put(tableName, readPartition + 1);
          break;
        }
      }
    }
  }
}
