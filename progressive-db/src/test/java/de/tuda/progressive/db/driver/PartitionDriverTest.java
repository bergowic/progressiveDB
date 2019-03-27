package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.meta.MemoryMetaData;
import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public abstract class PartitionDriverTest {

  private static final String TABLE_NAME = "test";

  protected static Connection connection;

  @BeforeEach
  void beforeEach() throws SQLException {
    final PartitionDriver driver = getDriver().build();

    try (Statement statement = connection.createStatement()) {
      for (int i = 0; i < 3; i++) {
        SqlDropTable dropTable = SqlUtils.dropTable(driver.getPartitionTable(TABLE_NAME, i));
        statement.execute(driver.toSql(dropTable));
      }

      statement.execute(driver.toSql(SqlUtils.dropTable(TABLE_NAME)));
      statement.execute(String.format("create table %s (a integer, b varchar(100))", TABLE_NAME));
      statement.execute(String.format("insert into %s values (1, 'a')", TABLE_NAME));
      statement.execute(String.format("insert into %s values (2, 'b')", TABLE_NAME));
      statement.execute(String.format("insert into %s values (3, 'c')", TABLE_NAME));
    }
  }

  @AfterAll
  static void afterAll() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  protected abstract PartitionDriver.Builder<
          ? extends PartitionDriver, ? extends PartitionDriver.Builder>
      getDriver();

  @Test
  void testGetCount() {
    final AbstractDriver driver = getDriver().build();

    assertEquals(3, driver.getCount(connection, TABLE_NAME));
  }

  @Test
  void testPrepare1() {
    testPrepare1(false);
  }

  @Test
  void testPrepare1Partitions() {
    testPrepare1(true);
  }

  private void testPrepare1(boolean hasPartitions) {
    final DbDriver driver = getDriver().partitionSize(1).hasPartitions(hasPartitions).build();

    final MetaData metaData = new MemoryMetaData();
    driver.prepareTable(connection, TABLE_NAME, metaData);
    final List<Partition> partitions = metaData.getPartitions(TABLE_NAME);

    assertEquals(3, partitions.size());
    partitions.forEach(p -> assertEquals(1, p.getEntries()));

    assertColumn(metaData, "a", new Column(TABLE_NAME, "a", 1, 3));
    assertColumn(metaData, "b", null);
  }

  @Test
  void testPrepare2() {
    testPrepare2(false);
  }

  @Test
  void testPrepare2Partitions() {
    testPrepare2(true);
  }

  private void testPrepare2(boolean hasPartitions) {
    final DbDriver driver = getDriver().partitionSize(2).hasPartitions(hasPartitions).build();

    final MetaData metaData = new MemoryMetaData();
    driver.prepareTable(connection, TABLE_NAME, metaData);
    final List<Partition> partitions = metaData.getPartitions(TABLE_NAME);

    assertEquals(2, partitions.size());
    partitions.sort(Comparator.comparingInt(p -> (int) p.getEntries()));
    assertEquals(1, partitions.get(0).getEntries());
    assertEquals(2, partitions.get(1).getEntries());

    assertColumn(metaData, "a", new Column(TABLE_NAME, "a", 1, 3));
    assertColumn(metaData, "b", null);
  }

  private void assertColumn(MetaData metaData, String columnName, Column expected) {
    final Column column = metaData.getColumn(TABLE_NAME, columnName);

    if (expected == null) {
      assertNull(column);
    } else {
      assertNotNull(column);
      assertEquals(expected.getMin(), column.getMin());
      assertEquals(expected.getMax(), column.getMax());
    }
  }
}
