package de.tuda.progressive.db.driver;

import org.junit.jupiter.api.BeforeAll;

import java.sql.DriverManager;
import java.sql.SQLException;

class MySQLDriverTest extends PartitionDriverTest {
  private static final String URL = "jdbc:mysql://localhost:3306/progressive";
  private static final String USER = "root";
  private static final String PASSWORD = "";

  @BeforeAll
  static void init() throws SQLException {
    connection = DriverManager.getConnection(URL, USER, PASSWORD);
  }

  @Override
  protected PartitionDriver.Builder getDriver() {
    return new MySQLDriver.Builder();
  }
}
