package de.tuda.progressive.db.buffer.impl;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.sql.parser.SqlUpsert;
import de.tuda.progressive.db.statement.context.impl.JdbcUpsertContext;
import de.tuda.progressive.db.util.SqlUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcUpsertDataBuffer extends JdbcDataBuffer {

	private final PreparedStatement upsertBuffer;

	public JdbcUpsertDataBuffer(DbDriver driver, Connection connection, JdbcUpsertContext context) {
		super(driver, connection, context);

		this.upsertBuffer = prepareUpsertBuffer(context.getUpsertBuffer());
	}

	private PreparedStatement prepareUpsertBuffer(SqlUpsert upsert) {
		final String sql = driver.toSql(upsert);
		try {
			return connection.prepareStatement(sql);
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void addInternal(ResultSet result) throws SQLException {
		final int internalCount = result.getMetaData().getColumnCount();

		while (!result.isClosed() && result.next()) {
			for (int i = 1; i <= internalCount; i++) {
				upsertBuffer.setObject(i, result.getObject(i));
				upsertBuffer.setObject(i + internalCount, result.getObject(i));
			}
			upsertBuffer.executeUpdate();
		}
	}

	@Override
	public void close() throws Exception {
		SqlUtils.closeSafe(upsertBuffer);

		super.close();
	}
}
