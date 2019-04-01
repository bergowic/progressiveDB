package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;

import java.sql.Connection;

public interface ProgressiveStatementFactory {

  ProgressiveStatement prepare(Connection connection, SqlSelectProgressive select);

  ProgressiveStatement prepare(
      Connection connection, SqlCreateProgressiveView createProgressiveView);
}
