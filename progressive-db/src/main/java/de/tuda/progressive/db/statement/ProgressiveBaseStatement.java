package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class ProgressiveBaseStatement implements ProgressiveStatement<DataBuffer> {

  private static final Logger log = LoggerFactory.getLogger(ProgressiveBaseStatement.class);

  private static final ExecutorService executor = Executors.newCachedThreadPool();

  private final DbDriver driver;

  private final List<Partition> partitions;

  private PreparedStatement preparedStatement;

  private int readPartitions;

  protected final ResultSetMetaData metaData;

  protected final DataBuffer dataBuffer;

  private boolean isClosed;

  private final Connection connection;

  private final SqlSelect selectSource;

  public ProgressiveBaseStatement(
      DbDriver driver,
      Connection connection,
      JdbcSourceContext context,
      DataBuffer dataBuffer,
      List<Partition> partitions) {
    this.driver = driver;
    this.dataBuffer = dataBuffer;
    this.partitions = partitions;
    this.connection = connection;
    this.selectSource = context.getSelectSource();

    try {
      preparedStatement =
          driver.hasPartitions()
              ? connection.prepareStatement(driver.toSql(selectSource))
              : null;

      metaData = dataBuffer.getMetaData();
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    startFetching();
  }

  private void startFetching() {
    executor.submit(
        () -> {
          try {
            for (Partition partition : partitions) {
              synchronized (this) {
                if (isClosed) {
                  break;
                }
              }

              query(partition);
            }
          } catch (Throwable t) {
            // TODO
            t.printStackTrace();
          }
        });
  }

  private void query(Partition partition) {
    log.info("query next partition: {}", partition.getId());
    try {
      ResultSet resultSet = getResult(partition);

      log.info("data received");
      dataBuffer.add(resultSet);
      log.info("received data handled");

      synchronized (this) {
        readPartitions++;
      }

      queryHandled(partition);
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  private ResultSet getResult(Partition partition) throws SQLException {
    if (driver.hasPartitions()) {
      preparedStatement.setInt(1, partition.getId());
      log.info("next statement {}", preparedStatement.toString().replaceAll("\\r\\n", " "));

      return preparedStatement.executeQuery();
    } else {
      final SqlIdentifier from = (SqlIdentifier) selectSource.getFrom();
      final String table = driver.getPartitionTable(from.getSimple(), partition.getId());
      final SqlSelect select = (SqlSelect) selectSource.clone(SqlParserPos.ZERO);
      select.setFrom(SqlUtils.getIdentifier(table));

      final String sql = driver.toSql(select);
      log.info("next statement {}", sql.replaceAll("\\r\\n", " "));

      try (Statement statement = connection.createStatement()) {
        return statement.executeQuery(sql);
      }
    }
  }

  protected abstract void queryHandled(Partition partition);

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
    return readPartitions == partitions.size();
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
    return (double) readPartitions / (double) partitions.size();
  }
}
