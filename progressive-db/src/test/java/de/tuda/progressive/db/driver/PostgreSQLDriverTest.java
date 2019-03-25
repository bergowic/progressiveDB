package de.tuda.progressive.db.driver;

import org.junit.jupiter.api.BeforeAll;

import java.sql.DriverManager;
import java.sql.SQLException;

class PostgreSQLDriverTest extends AbstractDriverTest {
  private static final String URL = "jdbc:postgresql://localhost:5432/progressive";
  private static final String USER = "postgres";
  private static final String PASSWORD = "postgres";

  @BeforeAll
  static void init() throws SQLException {
    connection = DriverManager.getConnection(URL, USER, PASSWORD);
  }

  @Override
  protected AbstractDriver getDriver(int partitionSize) {
    return new PostgreSQLDriver(partitionSize);
  }
}
