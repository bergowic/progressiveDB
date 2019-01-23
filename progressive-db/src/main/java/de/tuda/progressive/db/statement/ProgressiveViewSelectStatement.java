package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.SimpleStatementContext;
import de.tuda.progressive.db.util.SqlUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class ProgressiveViewSelectStatement implements ProgressiveStatement, ProgressiveListener {

	private final ProgressiveViewStatement viewStatement;

	private final SimpleStatementContext context;

	private final PreparedStatement selectStatement;

	private final ResultSetMetaData metaData;

	private List<Object[]> results = new ArrayList<>();

	public ProgressiveViewSelectStatement(
			Connection tmpConnection,
			DbDriver driver,
			ProgressiveViewStatement viewStatement,
			SimpleStatementContext context
	) {
		this.viewStatement = viewStatement;
		this.context = context;

		try {
			this.selectStatement = tmpConnection.prepareStatement(driver.toSql(context.getSelectCache()));
			this.metaData = new ResultSetMetaDataWrapper(selectStatement.getMetaData());
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized void handle(Partition partition) {
		try {
			SqlUtils.setScale(selectStatement, context, getProgress());
			SqlUtils.setMetaFields(selectStatement, context, new HashMap<MetaField, Object>() {{
				put(MetaField.PARTITION, getReadPartitions() - 1);
				put(MetaField.PROGRESS, getProgress());
			}});

			ResultSet resultSet = selectStatement.executeQuery();
			while (resultSet.next()) {
				Object[] row = new Object[getMetaData().getColumnCount()];
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

	@Override
	public synchronized ResultSet getResultSet() {
		if (results.isEmpty() && !isDone()) {
			try {
				wait();
			} catch (InterruptedException e) {
				// do nothing
			}
		}

		ResultSet resultSet = new ProgressiveResultSet(getMetaData(), new LinkedList<>(results));
		results.clear();
		return resultSet;
	}

	@Override
	public ResultSetMetaData getMetaData() {
		return metaData;
	}

	@Override
	public int getReadPartitions() {
		return viewStatement.getReadPartitions();
	}

	@Override
	public double getProgress() {
		return viewStatement.getProgress();
	}

	@Override
	public boolean isDone() {
		return false;
	}

	@Override
	public void run() {
		viewStatement.addListener(this);
	}

	@Override
	public void close() {
		viewStatement.removeListener(this);
	}

	@Override
	public SimpleStatementContext getContext() {
		return context;
	}
}
