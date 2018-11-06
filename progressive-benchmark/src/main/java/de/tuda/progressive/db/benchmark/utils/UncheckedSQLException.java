package de.tuda.progressive.db.benchmark.utils;

import java.sql.SQLException;

public class UncheckedSQLException extends RuntimeException {

	public UncheckedSQLException(SQLException cause) {
		super(cause);
	}

	public UncheckedSQLException(String message, SQLException cause) {
		super(message, cause);
	}

	public UncheckedSQLException(String message, SQLException cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
