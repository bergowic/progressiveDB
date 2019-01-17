package de.tuda.progressive.db.statement;

import org.apache.calcite.sql.SqlSelect;

public interface ProgressiveStatementFactory {

	ProgressiveStatement prepare(SqlSelect select);
}
