package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class ProgressiveBaseStatement implements ProgressiveStatement<DataBuffer> {

  private static final Logger log = LoggerFactory.getLogger(ProgressiveBaseStatement.class);

  private static final ExecutorService executor = Executors.newCachedThreadPool();

  private final List<Partition> partitions;

  private PreparedStatement preparedStatement;

  private int readPartitions;

  protected final ResultSetMetaData metaData;

  protected final DataBuffer dataBuffer;

  private boolean isClosed;

  private final Connection connection;

  public ProgressiveBaseStatement(
      DbDriver driver,
      Connection connection,
      JdbcSourceContext context,
      DataBuffer dataBuffer,
      List<Partition> partitions) {
    this.dataBuffer = dataBuffer;
    this.partitions = partitions;
    this.connection = connection;

    try {
      preparedStatement = connection.prepareStatement(driver.toSql(context.getSelectSource()));

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
      preparedStatement.setInt(1, partition.getId());
      log.info("next statement {}", preparedStatement.toString().replaceAll("\\r\\n", " "));

      ResultSet resultSet = preparedStatement.executeQuery();

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
    SqlUtils.closeSafe(connection);
  }

  protected final synchronized int getReadPartitions() {
    return readPartitions;
  }

  protected final synchronized double getProgress() {
    return (double) readPartitions / (double) partitions.size();
  }
}
