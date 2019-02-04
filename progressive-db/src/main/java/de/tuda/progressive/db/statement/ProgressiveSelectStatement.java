package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.StatementContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class ProgressiveSelectStatement extends ProgressiveBaseStatement {

	private static final Logger log = LoggerFactory.getLogger(ProgressiveSelectStatement.class);

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
			log.info("prepare cache query");
			final double scale = (double) getReadPartitions() / (double) partitions.size();

			SqlUtils.setScale(tmpSelectStatement, context, scale);
			SqlUtils.setMetaFields(tmpSelectStatement, context, new HashMap<MetaField, Object>() {{
				put(MetaField.PARTITION, getReadPartitions() - 1);
				put(MetaField.PROGRESS, getProgress());
			}});

			log.info("run cache query");

			ResultSet resultSet = tmpSelectStatement.executeQuery();
			while (resultSet.next()) {
				Object[] row = new Object[metaData.getColumnCount()];
				for (int i = 1; i <= row.length; i++) {
					row[i - 1] = resultSet.getObject(i);
				}
				results.add(row);
			}

			log.info("cache results received");

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
