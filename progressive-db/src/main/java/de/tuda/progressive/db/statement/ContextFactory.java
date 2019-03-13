package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlFutureIdentifier;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.SimpleStatementContext;
import de.tuda.progressive.db.statement.context.StatementContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlValuesOperator;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class ContextFactory {

	private static final Logger log = LoggerFactory.getLogger(ContextFactory.class);

	public static final ContextFactory instance = new ContextFactory();

	public StatementContext create(Connection connection, DbDriver driver, SqlSelect select, Partition partition) {
		final String cacheTableName = generateCacheTableName();
		return create(connection, driver, select, partition, cacheTableName);
	}

	public StatementContext create(Connection connection, DbDriver driver, SqlSelect select, Partition partition, String cacheTableName) {
		try {
			final List<SqlIdentifier> columnAliases = getColumnAliases(select.getSelectList());
			final List<MetaField> metaFields = getMetaFields(select.getSelectList());
			final SqlSelect sourceSelect = transformSelect(driver, select, metaFields);
			final PreparedStatement preparedStatement = prepareSourceSelect(connection, driver, sourceSelect);
			final ResultSetMetaData metaData = preparedStatement.getMetaData();
			final SqlCreateTable createCache = SqlUtils.createTable(driver, metaData, null, cacheTableName);
			final SqlInsert insertCache = insertCache(cacheTableName, metaData);
			final SqlSelect selectCache = selectCache(cacheTableName, metaData, columnAliases, metaFields, sourceSelect.getGroup());

			log.info("select source: {}", sourceSelect);

			return new StatementContext(
					sourceSelect,
					createCache,
					insertCache,
					selectCache,
					metaFields
			);
		} catch (SQLException e) {
			// TODO
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public SimpleStatementContext create(SimpleStatementContext context, SqlSelect select) {
		final SqlSelect viewSelect = new SqlSelect(
				SqlParserPos.ZERO,
				null,
				select.getSelectList(),
				context.getSelectCache(),
				select.getWhere(),
				select.getGroup(),
				select.getHaving(),
				select.getWindowList(),
				select.getOrderList(),
				select.getOffset(),
				select.getFetch()
		);

		return new SimpleStatementContext(viewSelect, context.getMetaFields());
	}

	public StatementContext create(Connection connection, DbDriver driver, SqlCreateProgressiveView createProgressiveView) {
		final SqlSelect select = (SqlSelect) createProgressiveView.getQuery();
		final String viewName = createProgressiveView.getName().getSimple();

		try {
			final List<SqlIdentifier> columnAliases = getColumnAliases(select.getSelectList());
			final List<MetaField> metaFields = getMetaFields(select.getSelectList());
			final SqlSelect sourceSelect = transformSelect(driver, select, metaFields);
			final int[] sourceSelectMapping = sortSelectList(driver, sourceSelect);
			final PreparedStatement preparedStatement = prepareSourceSelect(connection, driver, sourceSelect);
			final ResultSetMetaData metaData = preparedStatement.getMetaData();
			final SqlCreateTable createCache = SqlUtils.createTable(driver, metaData, null, viewName);
			final SqlInsert insertCache = insertCache(viewName, metaData);
			final SqlSelect selectCache = selectCache(viewName, metaData, sourceSelectMapping, columnAliases, metaFields, removeFutures(select.getGroup()));

			return new StatementContext(
					sourceSelect,
					createCache,
					insertCache,
					selectCache,
					metaFields
			);
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

	private SqlSelect transformSelect(DbDriver driver, SqlSelect select, List<MetaField> metaFields) {
		final SqlNodeList oldSelectList = select.getSelectList();
		final SqlNodeList oldGroups = select.getGroup() == null ? SqlNodeList.EMPTY : select.getGroup();
		SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
		SqlNodeList groups = new SqlNodeList(SqlParserPos.ZERO);

		for (int i = 0; i < oldSelectList.size(); i++) {
			switch (metaFields.get(i)) {
				case AVG:
					SqlBasicCall call = (SqlBasicCall) oldSelectList.get(i);
					SqlBasicCall avg;
					if (call.getKind() == SqlKind.AS) {
						avg = (SqlBasicCall) call.getOperands()[0];
					} else {
						avg = call;
					}

					selectList.add(SqlUtils.createSumAggregation(avg.getOperands()));
					selectList.add(SqlUtils.createCountAggregation(avg.getOperands()));
					break;
				case COUNT:
				case SUM:
				case NONE:
					selectList.add(oldSelectList.get(i));
					break;
				case PARTITION:
				case PROGRESS:
					// don't add anything
					break;
				default:
					throw new IllegalArgumentException("metaField not supported: " + metaFields.get(i));
			}
		}

		for (SqlNode group : oldGroups) {
			if (group instanceof SqlFutureIdentifier) {
				final SqlIdentifier name = new SqlIdentifier(((SqlFutureIdentifier) group).names, SqlParserPos.ZERO);
				selectList.add(name);
				groups.add(name);
			} else {
				groups.add(group);
			}
		}

		selectList.add(new SqlBasicCall(
				new SqlAsOperator(),
				new SqlNode[]{
						SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
						new SqlIdentifier("PARTITION", SqlParserPos.ZERO)},
				SqlParserPos.ZERO
		));

		final SqlIdentifier oldFrom = (SqlIdentifier) select.getFrom();
		SqlIdentifier from = new SqlIdentifier(Collections.singletonList(driver.getPartitionTable(oldFrom.getSimple())), SqlParserPos.ZERO);
		SqlBasicCall where = createWhere((SqlBasicCall) select.getWhere());

		return new SqlSelect(
				SqlParserPos.ZERO,
				null,
				selectList,
				from,
				where,
				groups.size() > 0 ? groups : null,
				select.getHaving(),
				select.getWindowList(),
				select.getOrderList(),
				select.getOffset(),
				select.getFetch()
		);
	}

	private int[] sortSelectList(DbDriver driver, SqlSelect select) {
		final SqlNodeList selectList = select.getSelectList();
		final List<String> selectStrings = get(selectList, driver::toSql);

		int[] permutation = IntStream.range(0, selectList.size()).toArray();
		for (int i = 0; i < selectList.size() - 1; i++) {
			int index = i;
			for (int j = i + 1; j < selectList.size() - 1; j++) {
				if (selectStrings.get(permutation[j]).compareTo(selectStrings.get(permutation[index])) < 0) {
					index = j;
				}
			}
			{
				SqlNode tmp = selectList.get(i);
				selectList.set(i, selectList.get(index));
				selectList.set(index, tmp);
			}
			{
				int tmp = permutation[i];
				permutation[i] = permutation[index];
				permutation[index] = tmp;
			}
		}

		int[] mapping = new int[permutation.length];
		for (int i = 0; i < mapping.length; i++) {
			mapping[permutation[i]] = i;
		}

		return mapping;
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

	private <T> List<T> get(SqlNodeList columns, Function<SqlNode, T> func) {
		return StreamSupport.stream(columns.spliterator(), false)
				.map(func)
				.collect(Collectors.toList());
	}

	private SqlNodeList removeFutures(SqlNodeList groups) {
		return new SqlNodeList(StreamSupport.stream(groups.spliterator(), false)
				.filter(n -> !(n instanceof SqlFutureIdentifier))
				.collect(Collectors.toList())
				, SqlParserPos.ZERO
		);
	}

	private List<SqlIdentifier> getColumnAliases(SqlNodeList columns) {
		return get(columns, this::columnToAlias);
	}

	private SqlIdentifier columnToAlias(SqlNode column) {
		if (!(column instanceof SqlBasicCall)) {
			return null;
		}
		final SqlBasicCall call = (SqlBasicCall) column;
		if (call.getKind() != SqlKind.AS) {
			return null;
		}
		return (SqlIdentifier) call.operand(1);
	}

	private List<MetaField> getMetaFields(SqlNodeList columns) {
		return get(columns, this::columnToMetaField);
	}

	private MetaField columnToMetaField(SqlNode column) {
		if (column instanceof SqlIdentifier || column instanceof SqlLiteral) {
			return MetaField.NONE;
		}
		if (column instanceof SqlBasicCall) {
			SqlBasicCall call = (SqlBasicCall) column;
			SqlOperator operator = call.getOperator();

			switch (operator.getName().toUpperCase()) {
				case "AVG":
					return MetaField.AVG;
				case "COUNT":
					return MetaField.COUNT;
				case "SUM":
					return MetaField.SUM;
				case "AS":
					return columnToMetaField(call.getOperands()[0]);
				case "PROGRESSIVE_PARTITION":
					return MetaField.PARTITION;
				case "PROGRESSIVE_PROGRESS":
					return MetaField.PROGRESS;
			}

			throw new IllegalArgumentException("operation is not supported: " + operator.getName());
		}

		throw new IllegalArgumentException("column type is not supported: " + column.getClass());
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

	private SqlSelect selectCache(
			String cacheTableName,
			ResultSetMetaData metaData,
			List<SqlIdentifier> columnAliases,
			List<MetaField> metaFields,
			SqlNodeList groups
	) {
		int[] selectMapping = IntStream.range(0, metaFields.size()).toArray();
		return selectCache(cacheTableName, metaData, selectMapping, columnAliases, metaFields, groups);
	}

	SqlSelect selectCache(
			String cacheTableName,
			ResultSetMetaData metaData,
			int[] selectMapping,
			List<SqlIdentifier> columnAliases,
			List<MetaField> metaFields,
			SqlNodeList groups
	) {
		final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);

		int i = 0;
		int index = 0;
		for (int j = 0; j < columnAliases.size(); j++) {
			final SqlIdentifier alias = columnAliases.get(j);
			final MetaField metaField = metaFields.get(j);

			SqlNode newColumn;

			switch (metaField) {
				case NONE:
					newColumn = SqlUtils.getColumnIdentifier(metaData, selectMapping[i] + 1);
					i++;
					break;
				case AVG:
					newColumn = SqlUtils.createAvgAggregation(
							SqlUtils.getColumnIdentifier(metaData, i + 1),
							SqlUtils.getColumnIdentifier(metaData, i + 2)
					);
					i += 2;
					break;
				case COUNT:
					newColumn = SqlUtils.createCountPercentAggregation(
							index,
							SqlUtils.getColumnIdentifier(metaData, i + 1)
					);
					i++;
					index++;
					break;
				case SUM:
					newColumn = SqlUtils.createSumPercentAggregation(
							index,
							SqlUtils.getColumnIdentifier(metaData, i + 1)
					);
					i++;
					index++;
					break;
				case PARTITION:
					newColumn = SqlUtils.createFunctionMetaField(index, SqlTypeName.INTEGER);
					index++;
					break;
				case PROGRESS:
					newColumn = SqlUtils.createFunctionMetaField(index, SqlTypeName.FLOAT);
					index++;
					break;
				default:
					throw new IllegalArgumentException("metaField not handled: " + metaField);
			}

			selectList.add(alias == null ? newColumn : new SqlBasicCall(
					SqlStdOperatorTable.AS,
					new SqlNode[]{newColumn, alias},
					SqlParserPos.ZERO
			));
		}

		return new SqlSelect(
				SqlParserPos.ZERO,
				null,
				selectList,
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
}
