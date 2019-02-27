package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import org.apache.calcite.sql.SqlSelect;

import java.sql.Connection;

public interface ProgressiveStatementFactory {

	ProgressiveStatement prepare(Connection connection, SqlSelect select);

//	ProgressiveStatement prepare(SqlCreateProgressiveView createProgressiveView);
}
