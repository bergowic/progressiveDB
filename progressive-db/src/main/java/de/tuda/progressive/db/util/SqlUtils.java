package de.tuda.progressive.db.util;

import de.tuda.progressive.db.driver.DbDriver;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.function.Consumer;

public class SqlUtils {

	private SqlUtils() {
	}

	public static SqlCreateTable createTable(
			DbDriver driver,
			String cacheTableName,
			ResultSetMetaData metaData,
			SqlNode... additionalColumns
	) {
		final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);

		try {
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				final String name = metaData.getColumnName(i);
				final int columnType = metaData.getColumnType(i);
				final int precision = metaData.getPrecision(i);
				final int scale = metaData.getScale(i);

				columns.add(getColumnOfType(driver, name, columnType, precision, scale));
			}
		} catch (SQLException e) {
			//TODO
			throw new RuntimeException(e);
		}

		for (SqlNode column : additionalColumns) {
			columns.add(column);
		}

		return SqlDdlNodes.createTable(
				SqlParserPos.ZERO,
				false,
				false,
				new SqlIdentifier(cacheTableName, SqlParserPos.ZERO),
				columns,
				null
		);
	}

	private static SqlNode getColumnOfType(DbDriver driver, String name, int type, int precision, int scale) {
		SqlTypeName sqlType;

		switch (type) {
			case Types.INTEGER:
				sqlType = SqlTypeName.INTEGER;
				break;
			case Types.BIGINT:
				sqlType = SqlTypeName.BIGINT;
				break;
			case Types.VARCHAR:
				sqlType = SqlTypeName.VARCHAR;
				break;
			default:
				sqlType = driver.toSqlType(type);
				if (sqlType == null) {
					throw new IllegalArgumentException("type not supported: " + type);
				}
		}

		return createColumn(name, sqlType, precision, scale);
	}

	public static SqlNode createColumn(String name, SqlTypeName type, int precision, int scale) {
		return SqlDdlNodes.column(
				SqlParserPos.ZERO,
				new SqlIdentifier(name, SqlParserPos.ZERO),
				new SqlDataTypeSpec(
						new SqlIdentifier(type.name(), SqlParserPos.ZERO),
						precision,
						scale > 0 ? scale : -1,
						null,
						null,
						SqlParserPos.ZERO
				),
				null,
				null
		);
	}

	public static SqlDropTable dropTable(String table) {
		return dropTable(table, true);
	}

	public static SqlDropTable dropTable(String table, boolean ifExists) {
		return SqlDdlNodes.dropTable(
				SqlParserPos.ZERO,
				ifExists,
				new SqlIdentifier(table, SqlParserPos.ZERO)
		);
	}

	public static void closeSafe(AutoCloseable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception e) {
				// do nothing
			}
		}
	}

	public static <T> Consumer<T> consumer(SqlConsumer<T> consumer) {
		return value -> {
			try {
				consumer.accept(value);
			} catch (SQLException e) {
				// TODO
				throw new RuntimeException(e);
			}
		};
	}
}
