package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import org.apache.calcite.sql.SqlSelect;

public interface ProgressiveStatementFactory {

	ProgressiveStatement prepare(SqlSelect select);

	ProgressiveStatement prepare(SqlCreateProgressiveView createProgressiveView);
}
