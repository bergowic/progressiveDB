package de.tuda.progressive.db.driver;

public class DbDriverFactory {

  private static final int PREFIX_LEN = "jdbc:".length();

  public static DbDriver create(String url) {
    final String driver = url.substring(PREFIX_LEN, url.indexOf(":", PREFIX_LEN));
    switch (driver.toUpperCase()) {
      case "POSTGRESQL":
        return new PostgreSQLDriver();
      case "SQLITE":
        return new SQLiteDriver();
      case "MYSQL":
        return new MySQLDriver();
    }

    throw new IllegalArgumentException("driver not supported: " + driver);
  }
}
