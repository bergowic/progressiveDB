package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.statement.context.Aggregation;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.StatementContext;
import de.tuda.progressive.db.util.SqlUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ProgressiveSelectStatement extends ProgressiveBaseStatement {

	private final StatementContext context;

	private final List<Partition> partitions;

	private List<Object[]> results = new ArrayList<>();

	public ProgressiveSelectStatement(
			DbDriver driver,
			Connection connection,
			Connection tmpConnection,
			StatementContext context,
			List<Partition> partitions
	) {
		super(driver, connection, tmpConnection, context, partitions);

		this.context = context;
		this.partitions = partitions;
	}

	@Override
	protected synchronized void queryHandled(Partition partition) {
		final PreparedStatement tmpSelectStatement = getTmpSelectStatement();

		try {
			int pos = 1;
			for (Aggregation aggregation : context.getAggregations()) {
				switch (aggregation) {
					case COUNT:
					case SUM:
						tmpSelectStatement.setDouble(pos++, (double) getReadPartitions() / (double) partitions.size());
						break;
				}
			}

			setMetaFields(tmpSelectStatement);

			ResultSet resultSet = tmpSelectStatement.executeQuery();
			while (resultSet.next()) {
				Object[] row = new Object[metaData.getColumnCount()];
				for (int i = 1; i <= row.length; i++) {
					row[i - 1] = resultSet.getObject(i);
				}
				results.add(row);
			}
			notify();
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void setMetaFields(PreparedStatement statement) {
		final int readPartitions = getReadPartitions();

		context.getMetaFieldPosition(MetaField.PARTITION).ifPresent(SqlUtils.consumer(pos -> {
			statement.setInt(pos + 1, readPartitions - 1);
		}));
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
	public synchronized void close() {
		super.close();

		notify();
	}
}
