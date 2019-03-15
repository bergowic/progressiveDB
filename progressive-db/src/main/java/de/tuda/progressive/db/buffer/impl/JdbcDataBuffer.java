package de.tuda.progressive.db.buffer.impl;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.statement.context.impl.jdbc.JdbcSelectContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.sql.*;

public class JdbcDataBuffer extends JdbcSelectDataBuffer<JdbcSelectContext>
    implements DataBuffer<JdbcSelectContext> {

  private final PreparedStatement insertBuffer;

  private final PreparedStatement updateBuffer;

  public JdbcDataBuffer(DbDriver driver, Connection connection, JdbcSelectContext context) {
    super(driver, connection, context, context.getSelectBuffer());

    this.insertBuffer = prepare(context.getInsertBuffer());
    this.updateBuffer = prepare(context.getUpdateBuffer());
  }

  @Override
  protected void init() {
    createBuffer(context.getCreateBuffer());
  }

  private void createBuffer(SqlCreateTable create) {
    if (create == null) {
      return;
    }

    final String sql = driver.toSql(create);
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
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
    if (insertBuffer == null) {
      return;
    }

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
  public void close() throws Exception {
    SqlUtils.closeSafe(insertBuffer);
    SqlUtils.closeSafe(updateBuffer);
    super.close();
  }

  public Connection getConnection() {
    return connection;
  }
}
