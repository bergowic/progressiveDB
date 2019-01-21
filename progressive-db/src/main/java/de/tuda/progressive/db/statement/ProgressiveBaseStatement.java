package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.statement.context.StatementContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class ProgressiveBaseStatement implements ProgressiveStatement {

	private static final Logger log = LoggerFactory.getLogger(ProgressiveBaseStatement.class);

	private static final ExecutorService executor = Executors.newCachedThreadPool();

	private final DbDriver driver;

	private final List<Partition> partitions;

	private final Connection tmpConnection;

	private PreparedStatement preparedStatement;

	private PreparedStatement tmpInsertStatement;

	private PreparedStatement tmpSelectStatement;

	private int readPartitions;

	protected final ResultSetMetaData metaData;

	private boolean isClosed;

	public ProgressiveBaseStatement(
			DbDriver driver,
			Connection connection,
			Connection tmpConnection,
			StatementContext context,
			List<Partition> partitions
	) {
		this.driver = driver;
		this.partitions = partitions;
		this.tmpConnection = tmpConnection;

		try {
			try (Statement statement = tmpConnection.createStatement()) {
				statement.execute(driver.toSql(context.getCreateCache()));
			}

			preparedStatement = connection.prepareStatement(driver.toSql(context.getSelectSource()));
			tmpInsertStatement = tmpConnection.prepareStatement(driver.toSql(context.getInsertCache()));
			setTmpSelectStatement(context.getSelectCache());

			metaData = new ResultSetMetaDataWrapper(tmpSelectStatement.getMetaData());
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
		executor.submit(() -> {
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

			ResultSet resultSet = preparedStatement.executeQuery();

			log.info("data received");
			addResult(resultSet, partition);
			log.info("received data handled");

			synchronized (this) {
				readPartitions++;
			}

			queryHandled(partition);
		} catch (SQLException e) {
			//TODO
			throw new RuntimeException(e);
		}
	}

	protected abstract void queryHandled(Partition partition);

	private void addResult(ResultSet resultSet, Partition partition) {
		try {
			int internalCount = preparedStatement.getMetaData().getColumnCount();
			while (!resultSet.isClosed() && resultSet.next()) {
				for (int i = 1; i < internalCount; i++) {
					tmpInsertStatement.setObject(i, resultSet.getObject(i));
				}
				tmpInsertStatement.setInt(internalCount, partition.getId());
				tmpInsertStatement.executeUpdate();
			}
		} catch (SQLException e) {
			//TODO
			e.printStackTrace();
//			throw new RuntimeException(e);
		}
	}

	@Override
	public ResultSetMetaData getMetaData() {
		return metaData;
	}

	@Override
	public synchronized boolean isDone() {
		return readPartitions == partitions.size();
	}

	@Override
	public synchronized void close() {
		isClosed = true;

		SqlUtils.closeSafe(preparedStatement);
		SqlUtils.closeSafe(tmpInsertStatement);
		SqlUtils.closeSafe(tmpSelectStatement);
	}

	@Override
	public synchronized int getReadPartitions() {
		return readPartitions;
	}

	protected PreparedStatement getTmpSelectStatement() {
		return tmpSelectStatement;
	}

	protected void setTmpSelectStatement(SqlSelect select) {
		try {
			tmpSelectStatement = tmpConnection.prepareStatement(driver.toSql(select));
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}
}
