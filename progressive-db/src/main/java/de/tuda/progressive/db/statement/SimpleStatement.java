package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.util.SqlUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleStatement implements ProgressiveStatement {

	private static final ExecutorService executor = Executors.newCachedThreadPool();

	private final StatementContext context;

	private final List<Partition> partitions;

	private PreparedStatement preparedStatement;

	private PreparedStatement tmpInsertStatement;

	private PreparedStatement tmpSelectStatement;

	private long readPartitions;

	private ResultSetMetaData metaData;

	private List<Object[]> results = new ArrayList<>();

	public SimpleStatement(
			DbDriver driver,
			Connection connection,
			Connection tmpConnection,
			StatementContext context,
			List<Partition> partitions
	) {
		this.context = context;
		this.partitions = partitions;

		try {
			try (Statement statement = tmpConnection.createStatement()) {
				statement.execute(driver.toSql(context.getCreateCache()));
			}

			preparedStatement = connection.prepareStatement(driver.toSql(context.getSelectSource()));
			tmpInsertStatement = tmpConnection.prepareStatement(driver.toSql(context.getInsertCache()));
			tmpSelectStatement = tmpConnection.prepareStatement(driver.toSql(context.getSelectCache()));

			metaData = new ResultSetMetaDataWrapper(tmpSelectStatement.getMetaData());
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}

		startFetching();
	}

	private void startFetching() {
		executor.submit(() -> {
			try {
				for (Partition partition : partitions) {
					query(partition);
				}
			} catch (Throwable t) {
				// TODO
				t.printStackTrace();
			}
		});
	}

	private void query(Partition partition) {
		try {
			preparedStatement.setInt(1, partition.getId());

			ResultSet resultSet = preparedStatement.executeQuery();
			addResult(resultSet, partition);
		} catch (SQLException e) {
			//TODO
			throw new RuntimeException(e);
		}
	}

	private void addResult(ResultSet resultSet, Partition partition) {
		try {
			int internalCount = preparedStatement.getMetaData().getColumnCount();
			while (resultSet.next()) {
				for (int i = 1; i < internalCount; i++) {
					tmpInsertStatement.setObject(i, resultSet.getObject(i));
				}
				tmpInsertStatement.setInt(internalCount, partition.getId());
				tmpInsertStatement.executeUpdate();
			}

			synchronized (this) {
				readPartitions++;

				int pos = 1;
				for (Aggregation aggregation : context.getAggregations()) {
					switch (aggregation) {
						case COUNT:
						case SUM:
							tmpSelectStatement.setDouble(pos++, (double) readPartitions / (double) partitions.size());
							break;
					}
				}
				tmpSelectStatement.setInt(pos, partition.getId());

				ResultSet resultSet1 = tmpSelectStatement.executeQuery();
				while (resultSet1.next()) {
					Object[] row = new Object[metaData.getColumnCount()];
					for (int i = 1; i <= row.length; i++) {
						row[i - 1] = resultSet1.getObject(i);
					}
					results.add(row);
				}
				notify();
			}

		} catch (SQLException e) {
			//TODO
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized ResultSet getResultSet() {
		if (results.isEmpty() && !isDone()) {
			try {
				wait();
			} catch (InterruptedException e) {
				// do nothing
			}
		}

		ResultSet resultSet = new ProgressiveResultSet(metaData, new LinkedList<>(results));
		results.clear();
		return resultSet;
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
		SqlUtils.closeSafe(preparedStatement);
		SqlUtils.closeSafe(tmpInsertStatement);
		SqlUtils.closeSafe(tmpSelectStatement);
		notify();
	}
}
