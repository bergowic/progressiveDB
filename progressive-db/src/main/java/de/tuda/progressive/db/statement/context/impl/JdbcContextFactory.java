package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.sql.parser.SqlUpsert;
import de.tuda.progressive.db.statement.context.ContextFactory;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class JdbcContextFactory extends ContextFactory<JdbcContext> {

	private final DbDriver bufferDriver;

	public JdbcContextFactory(DbDriver sourceDriver, DbDriver bufferDriver) {
		super(sourceDriver);

		if (!bufferDriver.hasUpsert()) {
			throw new IllegalArgumentException("driver does not support upsert");
		}

		this.bufferDriver = bufferDriver;
	}

	@Override
	protected JdbcContext create(
			Connection connection,
			SqlSelect select,
			List<MetaField> metaFields,
			SqlSelect selectSource
	) {
		final String sql = sourceDriver.toSql(selectSource);

		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			final ResultSetMetaData metaData = statement.getMetaData();
			final String bufferTableName = generateBufferTableName();
			final SqlCreateTable createBuffer = getCreateBuffer(metaData, bufferTableName, select.getGroup());

			final SqlSelect selectBuffer = getSelectBuffer(
					metaData,
					bufferTableName,
					getColumnAliases(select.getSelectList()),
					metaFields
			);

			return builder(
					metaData,
					bufferTableName,
					select
			)
					.metaFields(metaFields)
					.selectSource(selectSource)
					.createBuffer(createBuffer)
					.selectBuffer(selectBuffer)
					.build();
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private String generateBufferTableName() {
		return "progressive_buffer_" + UUID.randomUUID().toString().replaceAll("-", "_");
	}

	private SqlCreateTable getCreateBuffer(ResultSetMetaData metaData, String bufferTableName, SqlNodeList groups) {
		SqlNode[] additionalColumns;

		if (groups != null && groups.size() > 0) {
			additionalColumns = new SqlNode[]{
					SqlDdlNodes.primary(
							SqlParserPos.ZERO,
							new SqlIdentifier("pk_" + bufferTableName, SqlParserPos.ZERO),
							groups
					)
			};
		} else {
			additionalColumns = new SqlNode[0];
		}

		return SqlUtils.createTable(bufferDriver, bufferTableName, metaData, additionalColumns);
	}

	private SqlSelect getSelectBuffer(
			ResultSetMetaData metaData,
			String bufferTableName,
			List<SqlIdentifier> columnAliases,
			List<MetaField> metaFields
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
					newColumn = SqlUtils.getColumnIdentifier(metaData, i + 1);
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
					newColumn = SqlUtils.createPercentAggregation(
							index,
							SqlUtils.getColumnIdentifier(metaData, i + 1)
					);
				/*	newColumn = SqlUtils.createCountPercentAggregation(
							index,
							SqlUtils.getColumnIdentifier(metaData, i + 1)
					);
				*/
					i++;
					index++;
					break;
				case SUM:
					newColumn = SqlUtils.createPercentAggregation(
							index,
							SqlUtils.getColumnIdentifier(metaData, i + 1)
					);
				/*	newColumn = SqlUtils.createSumPercentAggregation(
							index,
							SqlUtils.getColumnIdentifier(metaData, i + 1)
					);
				*/
					i++;
					index++;
					break;
				case PARTITION:
					newColumn = SqlUtils.createFunctionMetaField(index, "partition", SqlTypeName.INTEGER);
					index++;
					break;
				case PROGRESS:
					newColumn = SqlUtils.createFunctionMetaField(index, "progress", SqlTypeName.FLOAT);
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
				new SqlIdentifier(bufferTableName, SqlParserPos.ZERO),
				null,
				null,
				null,
				null,
				null,
				null,
				null
		);
	}

	private JdbcContext.Builder builder(
			ResultSetMetaData metaData,
			String bufferTableName,
			SqlSelect select
	) throws SQLException {
		return new JdbcContext.Builder()
				.insertBuffer(getInsertBuffer(metaData, bufferTableName, select.getGroup()))
				.updateBuffer(getUpdateBuffer(metaData, bufferTableName, select.getGroup()));
	}

	private SqlInsert getInsertBuffer(ResultSetMetaData metaData, String bufferTableName, SqlNodeList groups) throws SQLException {
		final int count = metaData.getColumnCount();

		final SqlIdentifier targetTable = new SqlIdentifier(bufferTableName, SqlParserPos.ZERO);
		final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);
		final SqlNode[] insertValues = new SqlNode[count];
		final SqlNodeList updateValues = new SqlNodeList(SqlParserPos.ZERO);


		for (int i = 1; i <= count; i++) {
			final SqlNode param = new SqlDynamicParam(count + i - 1, SqlParserPos.ZERO);

			columns.add(SqlUtils.getColumnIdentifier(metaData, i));
			insertValues[i - 1] = new SqlDynamicParam(i - 1, SqlParserPos.ZERO);

			if (isKey(metaData.getColumnName(i), groups)) {
				updateValues.add(param);
			} else {
				updateValues.add(new SqlBasicCall(
						SqlStdOperatorTable.PLUS,
						new SqlNode[]{
								SqlUtils.getColumnIdentifier(metaData, i),
								param
						},
						SqlParserPos.ZERO
				));
			}
		}

		if (groups != null && groups.size() > 0) {
			final SqlNodeList indexColumns = new SqlNodeList(SqlParserPos.ZERO);

			for (SqlNode group : groups) {
				indexColumns.add(group);
			}

			return new SqlUpsert(
					SqlParserPos.ZERO,
					targetTable,
					columns,
					insertValues,
					indexColumns,
					new SqlUpdate(
							SqlParserPos.ZERO,
							new SqlIdentifier(Collections.emptyList(), SqlParserPos.ZERO),
							columns,
							updateValues,
							null,
							null,
							null
					)
			);
		} else {
			return new SqlInsert(
					SqlParserPos.ZERO,
					SqlNodeList.EMPTY,
					targetTable,
					SqlUtils.getValues(insertValues),
					columns
			);
		}
	}

	private boolean isKey(String column, SqlNodeList groups) {
		if (groups == null) {
			return false;
		}

		for (SqlNode group : groups) {
			if (group instanceof SqlIdentifier) {
				if (column.equals(((SqlIdentifier) group).getSimple())) {
					return true;
				}
			}
		}
		return false;
	}

	private SqlUpdate getUpdateBuffer(ResultSetMetaData metaData, String bufferTableName, SqlNodeList groups) throws SQLException {
		if (bufferDriver.hasUpsert() && groups != null) {
			return null;
		}

		final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);
		final SqlNodeList values = new SqlNodeList(SqlParserPos.ZERO);

		if (groups != null) {
			// TODO implement if driver does not support upsert
			throw new IllegalStateException("driver does not support upsert");
		}

		for (int i = 1; i <= metaData.getColumnCount(); i++) {
			final SqlNode param = new SqlDynamicParam(i - 1, SqlParserPos.ZERO);

			columns.add(SqlUtils.getColumnIdentifier(metaData, i));

			if (isKey(metaData.getColumnName(i), groups)) {
				values.add(param);
			} else {
				values.add(new SqlBasicCall(
						SqlStdOperatorTable.PLUS,
						new SqlNode[]{
								SqlUtils.getColumnIdentifier(metaData, i),
								param
						},
						SqlParserPos.ZERO
				));
			}
		}

		return new SqlUpdate(
				SqlParserPos.ZERO,
				new SqlIdentifier(bufferTableName, SqlParserPos.ZERO),
				columns,
				values,
				null,
				null,
				null
		);
	}
}
