package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleStatementFactory implements ProgressiveStatementFactory {

	private static final Logger log = LoggerFactory.getLogger(SimpleStatementFactory.class);

	private final Map<String, StatementContext> viewContexts = new HashMap<>();

	private final Map<String, ProgressiveViewStatement> viewStatements = new HashMap<>();

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
		log.info("prepare select: {}", select);
		assertValid(select);

		final ProgressiveViewStatement viewStatement = getProgressiveView(select);

		if (viewStatement == null) {
			log.info("no view found");
			final List<Partition> partitions = getPartitions(select);
			final StatementContext context = ContextFactory.instance.create(connection, driver, select, partitions.get(0));

			return new ProgressiveSelectStatement(driver, connection, tmpConnection, context, partitions);
		} else {
			log.info("view found");
			return new ProgressiveViewSelectStatement(tmpConnection, viewStatement, driver.toSql(select));
		}
	}

	private ProgressiveViewStatement getProgressiveView(SqlSelect select) {
		final SqlIdentifier from = (SqlIdentifier) select.getFrom();
		return viewStatements.get(from.getSimple().toUpperCase());
	}

	@Override
	public ProgressiveStatement prepare(SqlCreateProgressiveView createProgressiveView) {
		log.info("prepare progressive view: {}", createProgressiveView);

		final SqlNode query = createProgressiveView.getQuery();
		if (!(query instanceof SqlSelect)) {
			throw new IllegalArgumentException("query must be a select");
		}

		final SqlSelect select = (SqlSelect) query;
		assertValid(select);

		final SqlIdentifier view = createProgressiveView.getName();
		final String viewName = view.getSimple().toUpperCase();
		final StatementContext context = ContextFactory.instance.create(connection, driver, createProgressiveView);

		if (viewContexts.containsKey(viewName)) {
			final StatementContext oldContext = viewContexts.get(viewName);
			if (driver.toSql(oldContext.getSelectSource()).equals(driver.toSql(context.getSelectSource()))) {
				log.info("same data source, update select");
				final ProgressiveViewStatement statement = viewStatements.get(viewName);

				statement.setSelect(context.getSelectCache());

				return statement;
			} else {
				log.info("replace old view");
				final ProgressiveStatement oldStatement = viewStatements.get(viewName);

				oldStatement.close();

				return addViewStatement(select, viewName, context);
			}
		} else {
			log.info("create new view");
			return addViewStatement(select, viewName, context);
		}
	}

	private List<Partition> getPartitions(SqlSelect select) {
		final SqlIdentifier from = (SqlIdentifier) select.getFrom();
		return metaData.getPartitions(from.getSimple());
	}

	private ProgressiveViewStatement addViewStatement(SqlSelect select, String viewName, StatementContext context) {
		final List<Partition> partitions = getPartitions(select);
		final ProgressiveViewStatement statement = new ProgressiveViewStatement(
				driver,
				connection,
				tmpConnection,
				context,
				partitions
		);

		viewContexts.put(viewName, context);
		viewStatements.put(viewName, statement);

		return statement;
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
