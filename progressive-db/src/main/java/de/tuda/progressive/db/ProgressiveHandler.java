package de.tuda.progressive.db;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlDropProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlPrepareTable;
import de.tuda.progressive.db.statement.ProgressiveStatement;
import de.tuda.progressive.db.statement.ProgressiveStatementFactory;
import de.tuda.progressive.db.statement.SimpleStatementFactory;
import org.apache.calcite.sql.SqlSelect;

import java.sql.Connection;

public class ProgressiveHandler {

	private final DbDriver driver;

	private final Connection connection;

	private final Connection tmpConnection;

	private final MetaData metaData;

	private final ProgressiveStatementFactory statementFactory;

	public ProgressiveHandler(DbDriver driver, Connection connection, Connection tmpConnection, MetaData metaData) {
		this.driver = driver;
		this.connection = connection;
		this.tmpConnection = tmpConnection;
		this.metaData = metaData;

		this.statementFactory = new SimpleStatementFactory(
				driver,
				connection,
				tmpConnection,
				metaData
		);
	}

	public ProgressiveStatement handle(SqlPrepareTable prepareTable) {
		// TODO
		return null;
	}

	public ProgressiveStatement handle(SqlCreateProgressiveView createProgressiveView) {
		return statementFactory.prepare(createProgressiveView);
	}

	public ProgressiveStatement handle(SqlDropProgressiveView dropProgressiveView) {
		// TODO
		return null;
	}

	public ProgressiveStatement handle(SqlSelect select) {
		return statementFactory.prepare(select);
	}

//	public Optional<ProgressiveStatement> handle
}
