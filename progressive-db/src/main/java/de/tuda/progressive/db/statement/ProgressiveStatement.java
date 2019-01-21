package de.tuda.progressive.db.statement;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public interface ProgressiveStatement extends AutoCloseable {

	ResultSet getResultSet();

	ResultSetMetaData getMetaData();

	int getReadPartitions();

	double getProgress();

	boolean isDone();

	void run();

	void close();
}
