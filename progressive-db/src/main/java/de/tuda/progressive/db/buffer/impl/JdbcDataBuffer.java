package de.tuda.progressive.db.buffer.impl;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.statement.ResultSetMetaDataWrapper;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.impl.JdbcContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

class JdbcDataBuffer implements DataBuffer {

	protected final DbDriver driver;

	protected final Connection connection;

	private final JdbcContext context;

	private final PreparedStatement insertBuffer;

	private final PreparedStatement updateBuffer;

	private final PreparedStatement selectBuffer;

	private final ResultSetMetaData metaData;

	JdbcDataBuffer(DbDriver driver, Connection connection, JdbcContext context) {
		this.driver = driver;
		this.connection = connection;
		this.context = context;

		createBuffer(context.getCreateBuffer());
		this.insertBuffer = prepare(context.getInsertBuffer());
		this.updateBuffer = prepare(context.getUpdateBuffer());
		this.selectBuffer = prepare(context.getSelectBuffer());
		this.metaData = getMetaData(selectBuffer);
	}

	private void createBuffer(SqlCreateTable create) {
		final String sql = driver.toSql(create);
		try (Statement statement = connection.createStatement()) {
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private PreparedStatement prepare(SqlCall call) {
		if (call == null) {
			return null;
		}

		final String sql = driver.toSql(call);
		try {
			return connection.prepareStatement(sql);
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private ResultSetMetaData getMetaData(PreparedStatement statement) {
		try {
			return new ResultSetMetaDataWrapper(statement.getMetaData());
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void add(ResultSet result) {
		try {
			addInternal(result);
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void addInternal(ResultSet result) throws SQLException {
		final int internalCount = result.getMetaData().getColumnCount();

		// TODO execute update if exists

		while (!result.isClosed() && result.next()) {
			for (int i = 1; i <= internalCount; i++) {
				insertBuffer.setObject(i, result.getObject(i));
				insertBuffer.setObject(i + internalCount, result.getObject(i));
			}
			insertBuffer.executeUpdate();
		}
	}

	@Override
	public final List<Object[]> get(int partition, double progress) {
		SqlUtils.setScale(selectBuffer, context.getMetaFields(), progress);
		SqlUtils.setMetaFields(selectBuffer, context::getFunctionMetaFieldPos, new HashMap<MetaField, Object>() {{
			put(MetaField.PARTITION, partition);
			put(MetaField.PROGRESS, progress);
		}});

		final List<Object[]> results = new ArrayList<>();

		try (ResultSet resultSet = selectBuffer.executeQuery()) {
			while (resultSet.next()) {
				Object[] row = new Object[selectBuffer.getMetaData().getColumnCount()];
				for (int i = 1; i <= row.length; i++) {
					row[i - 1] = resultSet.getObject(i);
				}
				results.add(row);
			}
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}

		return results;
	}

	@Override
	public void close() throws Exception {
		SqlUtils.closeSafe(insertBuffer);
		SqlUtils.closeSafe(updateBuffer);
		SqlUtils.closeSafe(selectBuffer);
	}

	@Override
	public ResultSetMetaData getMetaData() {
		return metaData;
	}
}
