package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.buffer.DataBufferFactory;
import de.tuda.progressive.db.buffer.SelectDataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;
import de.tuda.progressive.db.statement.context.impl.BaseContext;
import de.tuda.progressive.db.statement.context.impl.BaseContextFactory;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SimpleStatementFactory implements ProgressiveStatementFactory {

  private static final Logger log = LoggerFactory.getLogger(SimpleStatementFactory.class);

  private final Map<String, ProgressiveViewStatement> viewStatements = new HashMap<>();

  private final DbDriver driver;

  private final MetaData metaData;

  private final Function<Pair<String, String>, Column> columnMapper;

  private final BaseContextFactory contextFactory;

  private final DataBufferFactory dataBufferFactory;

  public SimpleStatementFactory(
      DbDriver driver,
      MetaData metaData,
      BaseContextFactory contextFactory,
      DataBufferFactory dataBufferFactory) {
    this.driver = driver;
    this.metaData = metaData;
    this.columnMapper = metaData::getColumn;
    this.contextFactory = contextFactory;
    this.dataBufferFactory = dataBufferFactory;
  }

  @Override
  public ProgressiveStatement prepare(Connection connection, SqlSelectProgressive select) {
    log.info("prepare select: {}", select);
    assertValid(select);

    final ProgressiveViewStatement viewStatement = getProgressiveView(select);
    final Function<Pair<String, String>, Column> columnMapper = metaData::getColumn;

    if (viewStatement == null) {
      log.info("no view found");

      final List<Partition> partitions = getPartitions(select);
      final JdbcSourceContext context = contextFactory.create(connection, select, columnMapper);
      final DataBuffer dataBuffer = dataBufferFactory.create(context);

      return new ProgressiveSelectStatement(driver, connection, context, dataBuffer, partitions);
    } else {
      log.info("view found");

      final BaseContext context =
          contextFactory.create(viewStatement.getDataBuffer(), select, columnMapper);
      final SelectDataBuffer dataBuffer =
          dataBufferFactory.create(viewStatement.getDataBuffer(), context);
      return new ProgressiveViewSelectStatement(viewStatement, dataBuffer);
    }
  }

  @Override
  public ProgressiveStatement prepare(
      Connection connection, SqlCreateProgressiveView createProgressiveView) {
    log.info("prepare progressive view: {}", createProgressiveView);

    final SqlNode query = createProgressiveView.getQuery();
    if (!(query instanceof SqlSelect)) {
      throw new IllegalArgumentException("query must be a select");
    }

    final SqlSelect select = (SqlSelect) query;
    assertValid(select);

    final SqlIdentifier view = createProgressiveView.getName();
    final String viewName = view.getSimple().toUpperCase();
    final JdbcSourceContext context =
        contextFactory.create(connection, createProgressiveView, columnMapper);

    if (viewStatements.containsKey(viewName)) {
      throw new IllegalStateException("view already exists");
    } else {
      log.info("create new view");
      return addViewStatement(connection, context, select, viewName);
    }
  }

  private ProgressiveViewStatement getProgressiveView(SqlSelect select) {
    final SqlIdentifier from = (SqlIdentifier) select.getFrom();
    return viewStatements.get(from.getSimple().toUpperCase());
  }

  private List<Partition> getPartitions(SqlSelect select) {
    final SqlIdentifier from = (SqlIdentifier) select.getFrom();
    return metaData.getPartitions(from.getSimple());
  }

  private ProgressiveViewStatement addViewStatement(
      Connection connection, JdbcSourceContext context, SqlSelect select, String viewName) {
    final DataBuffer dataBuffer = dataBufferFactory.create(context);
    final List<Partition> partitions = getPartitions(select);
    final ProgressiveViewStatement statement =
        new ProgressiveViewStatement(driver, connection, context, dataBuffer, partitions);

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
