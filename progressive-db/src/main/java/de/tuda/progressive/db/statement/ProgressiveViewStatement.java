package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.statement.old.context.SimpleStatementContext;
import de.tuda.progressive.db.statement.old.context.StatementContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class ProgressiveViewStatement extends ProgressiveBaseStatement {

	private final Set<ProgressiveListener> listeners = new HashSet<>();

	private final ResultSet resultSet = new ProgressiveResultSet(metaData, new LinkedList<>());

	private Partition lastPartition;

	public ProgressiveViewStatement(
			DbDriver driver,
			Connection connection,
			Connection tmpConnection,
			StatementContext context,
			List<Partition> partitions
	) {
		super(driver, connection, null, null, partitions);
	}

	@Override
	protected synchronized void queryHandled(Partition partition) {
		lastPartition = partition;

		listeners.forEach(l -> l.handle(partition));

		if (isDone()) {
			notify();
		}
	}

	@Override
	public synchronized boolean isDone() {
		return true;
	}

	@Override
	public synchronized ResultSet getResultSet() {
		try {
			wait();
		} catch (InterruptedException e) {
			// do nothing
		}
		return resultSet;
	}

	public synchronized void addListener(ProgressiveListener listener) {
		listeners.add(listener);

		if (lastPartition != null) {
			listener.handle(lastPartition);
		}
	}

	public synchronized void removeListener(ProgressiveListener listener) {
		listeners.remove(listener);
	}

}
