package de.tuda.progressive.db;

import de.tuda.progressive.db.buffer.DataBufferFactory;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlDropProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlPrepareTable;
import de.tuda.progressive.db.statement.ProgressiveStatement;
import de.tuda.progressive.db.statement.ProgressiveStatementFactory;
import de.tuda.progressive.db.statement.SimpleStatementFactory;
import de.tuda.progressive.db.statement.context.impl.BaseContextFactory;
import org.apache.calcite.sql.SqlSelect;

import java.sql.Connection;

public class ProgressiveHandler {

  private final Connection connection;

  private final ProgressiveStatementFactory statementFactory;

  public ProgressiveHandler(
      DbDriver driver,
      Connection connection,
      MetaData metaData,
      BaseContextFactory contextFactory,
      DataBufferFactory dataBufferFactory) {
    this.connection = connection;

    this.statementFactory =
        new SimpleStatementFactory(driver, metaData, contextFactory, dataBufferFactory);
  }

  public ProgressiveStatement handle(SqlPrepareTable prepareTable) {
    // TODO
    return null;
  }

  public ProgressiveStatement handle(SqlCreateProgressiveView createProgressiveView) {
    return statementFactory.prepare(connection, createProgressiveView);
  }

  public ProgressiveStatement handle(SqlDropProgressiveView dropProgressiveView) {
    // TODO
    return null;
  }

  public ProgressiveStatement handle(SqlSelect select) {
    return statementFactory.prepare(connection, select);
  }

  //	public Optional<ProgressiveStatement> handle
}
