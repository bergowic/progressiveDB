package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlValuesOperator;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ContextFactory {

	public static final ContextFactory instance = new ContextFactory();

	public StatementContext create(Connection connection, DbDriver driver, SqlSelect select, Partition partition) {
		final String cacheTableName = generateCacheTableName();
		return create(connection, driver, select, partition, cacheTableName);
	}

	public StatementContext create(Connection connection, DbDriver driver, SqlSelect select, Partition partition, String cacheTableName) {
		try {
			final List<Aggregation> aggregations = getAggregations(select.getSelectList());
			final SqlSelect sourceSelect = transformSelect(select, partition.getTableName(), aggregations);
			final PreparedStatement preparedStatement = prepareSourceSelect(connection, driver, sourceSelect);
			final ResultSetMetaData metaData = preparedStatement.getMetaData();
			final SqlCreateTable createCache = createCache(driver, cacheTableName, metaData);
			final SqlInsert insertCache = insertCache(cacheTableName, metaData);
			final SqlSelect selectCache = selectCache(cacheTableName, metaData, aggregations, sourceSelect.getGroup());

			return new StatementContext(sourceSelect, createCache, insertCache, selectCache, aggregations);
		} catch (SQLException e) {
			// TODO
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	protected String generateCacheTableName() {
		return "tmp_" + UUID.randomUUID().toString().replaceAll("-", "_");
	}

	private PreparedStatement prepareSourceSelect(Connection connection, DbDriver driver, SqlSelect select) {
		final String sql = driver.toSql(select);
		try {
			return connection.prepareStatement(sql);
		} catch (SQLException e) {
			// TODO
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	private SqlSelect transformSelect(SqlSelect select, String sourceTable, List<Aggregation> aggregations) {
		final SqlNodeList oldSelectList = select.getSelectList();
		final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);

		for (int i = 0; i < oldSelectList.size(); i++) {
			if (aggregations.get(i) == Aggregation.AVG) {
				SqlBasicCall avg = (SqlBasicCall) oldSelectList.get(i);
				selectList.add(createSumAggregation(avg.getOperands()));
				selectList.add(createCountAggregation(avg.getOperands()));
			} else {
				selectList.add(oldSelectList.get(i));
			}
		}

		selectList.add(new SqlBasicCall(
				new SqlAsOperator(),
				new SqlNode[]{
						SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
						new SqlIdentifier("PARTITION", SqlParserPos.ZERO)},
				SqlParserPos.ZERO
		));

		SqlIdentifier from = new SqlIdentifier(Collections.singletonList(sourceTable), select.getFrom().getParserPosition());
		SqlBasicCall where = createWhere((SqlBasicCall) select.getWhere());

		return new SqlSelect(
				select.getParserPosition(),
				null,
				selectList,
				from,
				where,
				select.getGroup(),
				select.getHaving(),
				select.getWindowList(),
				select.getOrderList(),
				select.getOffset(),
				select.getFetch()
		);
	}

	private SqlBasicCall createWhere(SqlBasicCall oldWhere) {
		final SqlBasicCall eqPartition = createWhereEqPartition();
		if (oldWhere == null) {
			return eqPartition;
		}

		return new SqlBasicCall(
				SqlStdOperatorTable.AND,
				new SqlNode[]{
						oldWhere,
						eqPartition
				},
				SqlParserPos.ZERO
		);
	}

	private SqlBasicCall createWhereEqPartition() {
		return new SqlBasicCall(
				SqlStdOperatorTable.EQUALS,
				new SqlNode[]{
						new SqlIdentifier(Collections.singletonList("_partition"), SqlParserPos.ZERO),
						new SqlDynamicParam(0, SqlParserPos.ZERO)
				},
				SqlParserPos.ZERO
		);
	}

	private List<Aggregation> getAggregations(SqlNodeList columns) {
		return StreamSupport.stream(columns.spliterator(), false)
				.map(ContextFactory::columnToAggregation)
				.collect(Collectors.toList());
	}

	private static Aggregation columnToAggregation(SqlNode column) {
		if (column instanceof SqlIdentifier || column instanceof SqlLiteral) {
			return Aggregation.NONE;
		}
		if (column instanceof SqlBasicCall) {
			SqlOperator operator = ((SqlBasicCall) column).getOperator();
			switch (operator.getKind()) {
				case AVG:
					return Aggregation.AVG;
				case COUNT:
					return Aggregation.COUNT;
				case SUM:
					return Aggregation.SUM;
			}

			throw new IllegalArgumentException("operation is not supported: " + operator.getKind());
		}

		throw new IllegalArgumentException("column type is not supported: " + column.getClass());
	}

	private SqlCreateTable createCache(DbDriver driver, String cacheTableName, ResultSetMetaData metaData) {
		final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);

		try {
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				final String name = metaData.getColumnName(i);
				final int columnType = metaData.getColumnType(i);
				final int precision = metaData.getPrecision(i);
				final int scale = metaData.getScale(i);

				columns.add(SqlDdlNodes.column(
						SqlParserPos.ZERO,
						new SqlIdentifier(name, SqlParserPos.ZERO),
						typeToSqlType(driver, columnType, precision, scale),
						null,
						null
				));
			}
		} catch (SQLException e) {
			//TODO
			e.printStackTrace();
			throw new RuntimeException(e);
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

	private SqlDataTypeSpec typeToSqlType(DbDriver driver, int type, int precision, int scale) {
		SqlTypeName sqlType;

		switch (type) {
			case Types.INTEGER:
				sqlType = SqlTypeName.INTEGER;
				break;
			case Types.BIGINT:
				sqlType = SqlTypeName.BIGINT;
				break;
			default:
				sqlType = driver.toSqlType(type);
				if (sqlType == null) {
					throw new IllegalArgumentException("type not supported: " + type);
				}
		}

		return new SqlDataTypeSpec(
				new SqlIdentifier(sqlType.name(), SqlParserPos.ZERO),
				precision,
				scale > 0 ? scale : -1,
				null,
				null,
				SqlParserPos.ZERO
		);
	}

	private SqlInsert insertCache(String cacheTableName, ResultSetMetaData metaData) throws SQLException {
		final int count = metaData.getColumnCount();
		final SqlNode[] values = new SqlNode[count];

		for (int i = 1; i <= count; i++) {
			values[i - 1] = new SqlDynamicParam(i - 1, SqlParserPos.ZERO);
		}

		return new SqlInsert(
				SqlParserPos.ZERO,
				new SqlNodeList(SqlParserPos.ZERO),
				new SqlIdentifier(cacheTableName, SqlParserPos.ZERO),
				new SqlBasicCall(
						new SqlValuesOperator(),
						new SqlNode[]{
								new SqlBasicCall(
										new SqlRowOperator(" "),
										values,
										SqlParserPos.ZERO
								)
						},
						SqlParserPos.ZERO
				),
				null
		);
	}

	private SqlSelect selectCache(String cacheTableName, ResultSetMetaData metaData, List<Aggregation> aggregations, SqlNodeList groups) {
		final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);

		int i = 0;
		int index = 0;
		try {
			for (Aggregation aggregation : aggregations) {
				final SqlIdentifier column = new SqlIdentifier(metaData.getColumnName(i + 1), SqlParserPos.ZERO);
				switch (aggregation) {
					case NONE:
						columns.add(column);
						++i;
						break;
					case AVG:
						final SqlIdentifier nextColumn = new SqlIdentifier(metaData.getColumnName(i + 2), SqlParserPos.ZERO);
						columns.add(creatAvgAggregation(column, nextColumn));
						i += 2;
						++index;
						break;
					case COUNT:
						columns.add(createCountPercentAggregation(index, column));
						++i;
						++index;
						break;
					case SUM:
						columns.add(createSumPercentAggregation(index, column));
						++i;
						++index;
						break;
				}
			}
		} catch (SQLException e) {
			// TODO
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		columns.add(new SqlDynamicParam(index, SqlParserPos.ZERO));

		return new SqlSelect(
				SqlParserPos.ZERO,
				null,
				columns,
				new SqlIdentifier(cacheTableName, SqlParserPos.ZERO),
				null,
				groups,
				null,
				null,
				null,
				null,
				null
		);
	}

	private static SqlBasicCall creatAvgAggregation(SqlNode operand1, SqlNode operand2) {
		return new SqlBasicCall(
				SqlStdOperatorTable.DIVIDE,
				new SqlNode[]{createSumAggregation(operand1), createSumAggregation(operand2)},
				SqlParserPos.ZERO);
	}

	private static SqlBasicCall createCountAggregation(SqlNode... operands) {
		return new SqlBasicCall(
				new SqlCountAggFunction("COUNT"),
				operands,
				SqlParserPos.ZERO
		);
	}

	private static SqlBasicCall createCountPercentAggregation(int index, SqlNode operand) {
		return createPercentAggregation(index, createSumAggregation(operand));
	}

	private static SqlBasicCall createSumAggregation(SqlNode... operands) {
		return new SqlBasicCall(
				new SqlSumAggFunction(null),
				operands,
				SqlParserPos.ZERO
		);
	}

	private static SqlBasicCall createSumPercentAggregation(int index, SqlNode operand) {
		return createPercentAggregation(index, createSumAggregation(operand));
	}

	private static SqlBasicCall createPercentAggregation(int index, SqlBasicCall aggregation) {
		return new SqlBasicCall(
				SqlStdOperatorTable.DIVIDE,
				new SqlNode[]{
						aggregation,
						new SqlDynamicParam(index, SqlParserPos.ZERO)
				},
				SqlParserPos.ZERO);
	}

}
