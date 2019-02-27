package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.statement.context.BaseContext;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.old.context.StatementContext;
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

	private final DataBuffer dataBuffer;

	private List<Object[]> results = new ArrayList<>();

	public ProgressiveSelectStatement(
			DbDriver driver,
			Connection connection,
			BaseContext context,
			DataBuffer dataBuffer,
			List<Partition> partitions
	) {
		super(driver, connection, context, dataBuffer, partitions);

		this.dataBuffer = dataBuffer;
	}

	@Override
	protected synchronized void queryHandled(Partition partition) {
		log.info("run cache query");

		List<Object[]> rows = dataBuffer.get(getReadPartitions(), getProgress());
		results.addAll(rows);

		log.info("cache results received");

		notify();
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
