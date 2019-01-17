package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Partition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import java.sql.Connection;
import java.util.List;

public class SimpleStatementFactory implements ProgressiveStatementFactory {

	private final DbDriver driver;

	private final Connection connection;

	private final Connection tmpConnection;

	private final MetaData metaData;

	public SimpleStatementFactory(DbDriver driver, Connection connection, Connection tmpConnection, MetaData metaData) {
		this.driver = driver;
		this.connection = connection;
		this.tmpConnection = tmpConnection;
		this.metaData = metaData;
	}

	@Override
	public ProgressiveStatement prepare(SqlSelect select) {
		assertValid(select);

		final SqlIdentifier from = (SqlIdentifier) select.getFrom();
		final List<Partition> partitions = metaData.getPartitions(from.getSimple());

		final StatementContext context = ContextFactory.instance.create(connection, driver, select, partitions.get(0));

		return new SimpleStatement(driver, connection, tmpConnection, context, partitions);
	}

	private void assertValid(SqlSelect select) {
		SqlNode fromNode = select.getFrom();
		if (!(fromNode instanceof SqlIdentifier)) {
			throw new IllegalArgumentException("from is not of type SqlIdentifier");
		}

		SqlIdentifier fromId = (SqlIdentifier) fromNode;
		if (fromId.names.size() != 1) {
			throw new IllegalArgumentException("from does not contain exact 1 source");
		}
	}
}
