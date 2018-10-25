package de.tuda.progressive.db;

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.ProtobufMeta;
import org.apache.calcite.avatica.remote.TypedValue;

import java.util.List;
import java.util.Map;

public class PMeta implements Meta {

	private final ProtobufMeta fallback;

	public PMeta(ProtobufMeta fallback) {
		this.fallback = fallback;
	}

	@Override
	public Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle ch) {
		return fallback.getDatabaseProperties(ch);
	}

	@Override
	public MetaResultSet getTables(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList) {
		return fallback.getTables(ch, catalog, schemaPattern, tableNamePattern, typeList);
	}

	@Override
	public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
		return fallback.getColumns(ch, catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}

	@Override
	public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
		return fallback.getSchemas(ch, catalog, schemaPattern);
	}

	@Override
	public MetaResultSet getCatalogs(ConnectionHandle ch) {
		return fallback.getCatalogs(ch);
	}

	@Override
	public MetaResultSet getTableTypes(ConnectionHandle ch) {
		return fallback.getTableTypes(ch);
	}

	@Override
	public MetaResultSet getProcedures(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat procedureNamePattern) {
		return fallback.getProcedures(ch, catalog, schemaPattern, procedureNamePattern);
	}

	@Override
	public MetaResultSet getProcedureColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat procedureNamePattern, Pat columnNamePattern) {
		return fallback.getProcedureColumns(ch, catalog, schemaPattern, procedureNamePattern, columnNamePattern);
	}

	@Override
	public MetaResultSet getColumnPrivileges(ConnectionHandle ch, String catalog, String schema, String table, Pat columnNamePattern) {
		return fallback.getColumnPrivileges(ch, catalog, schema, table, columnNamePattern);
	}

	@Override
	public MetaResultSet getTablePrivileges(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern) {
		return fallback.getTablePrivileges(ch, catalog, schemaPattern, tableNamePattern);
	}

	@Override
	public MetaResultSet getBestRowIdentifier(ConnectionHandle ch, String catalog, String schema, String table, int scope, boolean nullable) {
		return fallback.getBestRowIdentifier(ch, catalog, schema, table, scope, nullable);
	}

	@Override
	public MetaResultSet getVersionColumns(ConnectionHandle ch, String catalog, String schema, String table) {
		return fallback.getVersionColumns(ch, catalog, schema, table);
	}

	@Override
	public MetaResultSet getPrimaryKeys(ConnectionHandle ch, String catalog, String schema, String table) {
		return fallback.getPrimaryKeys(ch, catalog, schema, table);
	}

	@Override
	public MetaResultSet getImportedKeys(ConnectionHandle ch, String catalog, String schema, String table) {
		return fallback.getImportedKeys(ch, catalog, schema, table);
	}

	@Override
	public MetaResultSet getExportedKeys(ConnectionHandle ch, String catalog, String schema, String table) {
		return fallback.getExportedKeys(ch, catalog, schema, table);
	}

	@Override
	public MetaResultSet getCrossReference(ConnectionHandle ch, String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) {
		return fallback.getCrossReference(ch, parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
	}

	@Override
	public MetaResultSet getTypeInfo(ConnectionHandle ch) {
		return fallback.getTypeInfo(ch);
	}

	@Override
	public MetaResultSet getIndexInfo(ConnectionHandle ch, String catalog, String schema, String table, boolean unique, boolean approximate) {
		return fallback.getIndexInfo(ch, catalog, schema, table, unique, approximate);
	}

	@Override
	public MetaResultSet getUDTs(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types) {
		return fallback.getUDTs(ch, catalog, schemaPattern, typeNamePattern, types);
	}

	@Override
	public MetaResultSet getSuperTypes(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern) {
		return fallback.getSuperTypes(ch, catalog, schemaPattern, typeNamePattern);
	}

	@Override
	public MetaResultSet getSuperTables(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern) {
		return fallback.getSuperTables(ch, catalog, schemaPattern, tableNamePattern);
	}

	@Override
	public MetaResultSet getAttributes(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern, Pat attributeNamePattern) {
		return fallback.getAttributes(ch, catalog, schemaPattern, typeNamePattern, attributeNamePattern);
	}

	@Override
	public MetaResultSet getClientInfoProperties(ConnectionHandle ch) {
		return fallback.getClientInfoProperties(ch);
	}

	@Override
	public MetaResultSet getFunctions(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat functionNamePattern) {
		return fallback.getFunctions(ch, catalog, schemaPattern, functionNamePattern);
	}

	@Override
	public MetaResultSet getFunctionColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat functionNamePattern, Pat columnNamePattern) {
		return fallback.getFunctionColumns(ch, catalog, schemaPattern, functionNamePattern, columnNamePattern);
	}

	@Override
	public MetaResultSet getPseudoColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
		return fallback.getPseudoColumns(ch, catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}

	@Override
	public Iterable<Object> createIterable(StatementHandle stmt, QueryState state, Signature signature, List<TypedValue> parameters, Frame firstFrame) {
		return fallback.createIterable(stmt, state, signature, parameters, firstFrame);
	}

	@Override
	public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
		return fallback.prepare(ch, sql, maxRowCount);
	}

	@Override
	@Deprecated
	public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, PrepareCallback callback) throws NoSuchStatementException {
		return prepareAndExecute(h, sql, maxRowCount, AvaticaUtils.toSaturatedInt(maxRowCount), callback);
	}

	@Override
	public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
		return fallback.prepareAndExecute(h, sql, maxRowCount, maxRowsInFirstFrame, callback);
	}

	@Override
	public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h, List<String> sqlCommands) throws NoSuchStatementException {
		return fallback.prepareAndExecuteBatch(h, sqlCommands);
	}

	@Override
	public ExecuteBatchResult executeBatch(StatementHandle h, List<List<TypedValue>> parameterValues) throws NoSuchStatementException {
		return fallback.executeBatch(h, parameterValues);
	}

	@Override
	public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) throws NoSuchStatementException, MissingResultsException {
		return fallback.fetch(h, offset, fetchMaxRowCount);
	}

	@Override
	@Deprecated
	public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, long maxRowCount) throws NoSuchStatementException {
		return fallback.execute(h, parameterValues, maxRowCount);
	}

	@Override
	public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame) throws NoSuchStatementException {
		return fallback.execute(h, parameterValues, maxRowsInFirstFrame);
	}

	@Override
	public StatementHandle createStatement(ConnectionHandle ch) {
		return fallback.createStatement(ch);
	}

	@Override
	public void closeStatement(StatementHandle h) {
		fallback.closeStatement(h);
	}

	@Override
	public void openConnection(ConnectionHandle ch, Map<String, String> info) {
		fallback.openConnection(ch, info);
	}

	@Override
	public void closeConnection(ConnectionHandle ch) {
		fallback.closeConnection(ch);
	}

	@Override
	public boolean syncResults(StatementHandle sh, QueryState state, long offset) throws NoSuchStatementException {
		return fallback.syncResults(sh, state, offset);
	}

	@Override
	public void commit(ConnectionHandle ch) {
		fallback.commit(ch);
	}

	@Override
	public void rollback(ConnectionHandle ch) {
		fallback.rollback(ch);
	}

	@Override
	public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
		return fallback.connectionSync(ch, connProps);
	}
}
