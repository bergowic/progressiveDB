package de.tuda.progressive.db.statement;

public interface ProgressiveStatementFactory {

	ProgressiveStatement prepare(String sql);
}
