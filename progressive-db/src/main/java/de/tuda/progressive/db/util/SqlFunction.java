package de.tuda.progressive.db.util;

import java.sql.SQLException;

public interface SqlFunction<I, O> {
	O get(I value) throws SQLException;
}
