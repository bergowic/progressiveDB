package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractDriver implements DbDriver {

	private final SqlDialect dialect;

	public AbstractDriver(SqlDialect dialect) {
		this.dialect = dialect;
	}

	@Override
	public String toSql(SqlNode node) {
		return node.toSqlString(dialect).getSql();
	}

	@Override
	public SqlTypeName toSqlType(int jdbcType) {
		return null;
	}

	@Override
	public void prepareTable(Connection connection, String table, MetaData metaData) {
		final List<Partition> partitions = split(connection, table);
		final List<Column> columns = getColumns(connection, table);

		metaData.add(table, partitions, columns);
	}

	protected abstract List<Partition> split(Connection connection, String table);

	private List<Column> getColumns(Connection connection, String table) {
		final List<String> columnNames = getColumnNames(connection, table);

		try (PreparedStatement statement = connection.prepareStatement(getSelectMinMax(table, columnNames))) {
			try (ResultSet result = statement.executeQuery()) {
				final List<Column> columns = new ArrayList<>();

				result.next();

				for (int i = 0; i < columnNames.size(); i++) {
					final Column column = new Column();
					final int pos = i * 2 + 1;

					column.setTable(table);
					column.setName(columnNames.get(i));
					column.setMin(result.getLong(pos));
					column.setMax(result.getLong(pos + 1));
					columns.add(column);
				}

				return columns;
			}
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private String getSelectMinMax(String table, List<String> columnNames) {
		final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
		for (String columnName : columnNames) {
			selectList.add(createAggregator(SqlStdOperatorTable.MIN, columnName));
			selectList.add(createAggregator(SqlStdOperatorTable.MAX, columnName));
		}

		return getSelect(selectList, table);
	}

	private SqlBasicCall createAggregator(SqlAggFunction func, String columnName) {
		return new SqlBasicCall(
				func,
				new SqlNode[]{new SqlIdentifier(columnName, SqlParserPos.ZERO)},
				SqlParserPos.ZERO
		);
	}

	private List<String> getColumnNames(Connection connection, String table) {
		try (PreparedStatement statement = connection.prepareStatement(getSelectAll(table))) {
			final List<String> columnNames = new ArrayList<>();
			final ResultSetMetaData metaData = statement.getMetaData();

			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				switch (metaData.getColumnType(i)) {
					case Types.SMALLINT:
					case Types.INTEGER:
					case Types.BIGINT:
						columnNames.add(metaData.getColumnName(i));
				}
			}

			return columnNames;
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	protected final long getCount(Connection connection, String table) {
		try (Statement statement = connection.createStatement()) {
			try (ResultSet result = statement.executeQuery(getSelectCount(table))) {
				result.next();
				return result.getLong(1);
			}
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	protected final String getSelectAll(String table) {
		final SqlNode selectAll = new SqlIdentifier("*", SqlParserPos.ZERO);
		return getSelect(selectAll, table);
	}

	protected final String getSelectCount(String table) {
		final SqlNode selectCount = createAggregator(SqlStdOperatorTable.COUNT, "*");
		return getSelect(selectCount, table);
	}

	private String getSelect(SqlNode singleSelect, String table) {
		return getSelect(new SqlNodeList(Collections.singleton(singleSelect), SqlParserPos.ZERO), table);
	}

	private String getSelect(SqlNodeList selectList, String table) {
		return toSql(new SqlSelect(
				SqlParserPos.ZERO,
				new SqlNodeList(SqlParserPos.ZERO),
				selectList,
				new SqlIdentifier(table, SqlParserPos.ZERO),
				null,
				null,
				null,
				null,
				new SqlNodeList(SqlParserPos.ZERO),
				null,
				null
		));
	}
}
