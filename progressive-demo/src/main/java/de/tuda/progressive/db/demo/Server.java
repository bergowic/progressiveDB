package de.tuda.progressive.db.demo;

import de.tuda.progressive.db.util.SqlUtils;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONArray;
import org.json.JSONObject;

public class Server extends WebSocketServer {

  enum DiagramType {
    ORIGIN, TIME
  }

  private static final int YEARS_START = 2000;
  private static final int YEARS_END = 2008;

  private static final String[] ORIGINS = {"ATL", "BNA", "BOS", "BWI", "CLE", "CLT", "CVG", "DCA",
      "DEN", "DFW", "DTW", "EWR", "FLL", "HOU", "IAD", "JFK", "LAS", "LAX", "LGA", "MCI", "MCO",
      "MDW", "MEM", "MIA", "MSP", "OAK", "ORD", "PDX", "PHL", "PHX", "PIT", "RDU", "SAN", "SEA",
      "SFO", "SJC", "SLC", "STL", "TPA"};

  private ExecutorService executorService = Executors.newCachedThreadPool();

  private final Map<Boolean, Map<DiagramType, Connection>> connections = new HashMap<>();
  private final Map<Boolean, Map<DiagramType, Statement>> statements = new HashMap<>();

  private Connection consoleConnection;
  private Statement consoleStatement;
  private Statement consoleViewStatement;

  private final LogWatcher logWatcher;

  private final List<String> appLogs = new ArrayList<>();

  private boolean viewCreated;

  private int queryId;

  public Server(LogWatcher logWatcher) {
    super(new InetSocketAddress(8081));

    setReuseAddr(true);
    this.logWatcher = logWatcher;
  }

  @Override
  public void onError(WebSocket webSocket, Exception e) {
    e.printStackTrace();
  }

  @Override
  public void onStart() {
    System.out.println("started");
  }

  @Override
  public synchronized void onOpen(WebSocket webSocket, ClientHandshake clientHandshake) {
    if (webSocket.getResourceDescriptor().endsWith("progressive")) {
      ensureConnection(true);
    } else if (webSocket.getResourceDescriptor().endsWith("native")) {
      ensureConnection(false);
    } else if (webSocket.getResourceDescriptor().endsWith("console")) {
      try {
        consoleConnection = DriverManager
            .getConnection("jdbc:avatica:remote:url=http://localhost:1337");
        consoleViewStatement = consoleConnection.createStatement();
        consoleViewStatement.execute(
            "create progressive view pv as select progressive_confidence(depdelay), avg(depdelay) depdelay, dayofweek future, progressive_partition(), progressive_progress() from ontime1m where (origin = 'ATL') future or (origin = 'JFK') future or (origin = 'LAX') future group by dayofweek future");
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public synchronized void onClose(WebSocket webSocket, int i, String s, boolean b) {
    if (webSocket.getResourceDescriptor().endsWith("console")) {
      dropView(consoleConnection, "pv");
      SqlUtils.closeSafe(consoleStatement);
      SqlUtils.closeSafe(consoleViewStatement);
      SqlUtils.closeSafe(consoleConnection);
    } else if (!webSocket.getResourceDescriptor().endsWith("log")) {
      boolean progressive = webSocket.getResourceDescriptor().endsWith("progressive");

      synchronized (statements) {
        if (statements.containsKey(progressive)) {
          statements.get(progressive).values().forEach(SqlUtils::closeSafe);
        }
      }

      if (progressive && viewCreated) {
        dropView(getOriginViewName(), DiagramType.ORIGIN);
        dropView(getTimeViewName(), DiagramType.TIME);
        viewCreated = false;
      }

      synchronized (connections) {
        if (connections.containsKey(progressive)) {
          connections.get(progressive).values().forEach(SqlUtils::closeSafe);
        }
      }

      connections.remove(progressive);
      statements.remove(progressive);
    }
  }

  @Override
  public synchronized void onMessage(WebSocket webSocket, String s) {
    if ("log".equals(s)) {
      logWatcher.register(
          log -> {
            sendLog(webSocket, true, log);
          });
      appLogs.forEach(log -> sendLog(webSocket, false, log));
    } else {
      JSONObject json = new JSONObject(s);

      try {
        if (json.has("sql")) {
          if (consoleStatement != null) {
            SqlUtils.closeSafe(consoleStatement);
          }
          runSql(webSocket, json.getString("sql"), json.getInt("id"));
        } else {
          final boolean progressive = json.getBoolean("progressive");
          queryId = json.getInt("queryId");
          final boolean view = json.getBoolean("view");
          final String origin = json.optString("origin", null);
          final JSONArray time = json.optJSONArray("time");
          final String start = json.optString("start");

          ensureConnection(progressive);

          if (progressive && view) {
            if (!viewCreated) {
              createView(getOriginView(), DiagramType.ORIGIN);
              createView(getTimeView(), DiagramType.TIME);
              viewCreated = true;
            }
          }

          query(webSocket, progressive, queryId, view, origin, time, start);
        }
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  private Connection getNativeConnection() throws SQLException {
    return DriverManager
        .getConnection("jdbc:postgresql://localhost:5432/progressive", "postgres", "postgres");
  }

  private Connection getProgressiveConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:1337");
  }

  private void ensureConnection(boolean progressive) {
    if (!connections.containsKey(progressive)) {
      connections.put(progressive, new HashMap<DiagramType, Connection>() {{
        try {
          put(DiagramType.ORIGIN, progressive ? getProgressiveConnection() : getNativeConnection());
          put(DiagramType.TIME, progressive ? getProgressiveConnection() : getNativeConnection());
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }});
      statements.put(progressive, new HashMap<>());
    }
  }

  private void sendLog(WebSocket socket, boolean progressive, String log) {
    if (!socket.isClosed()) {
      socket.send(new JSONObject().put("progressive", progressive).put("log", log).toString());
    }
  }

  private Statement getStatement(boolean progressive, DiagramType type) throws SQLException {
    final Connection connection = connections.get(progressive).get(type);

    synchronized (statements) {
      Statement statement = statements.get(progressive).get(type);
      if (statement != null) {
        statement.close();
      }

      statement = connection.createStatement();
      statements.get(progressive).put(type, statement);
      return statement;
    }
  }

  private String getOriginQuery(boolean progressive, int queryId, boolean view, JSONArray time) {
    String sql;
    if (view) {
      if (!progressive) {
        return getOriginQuery(false, queryId, false, time);
      }

      sql = "select progressive * from v_origin_" + queryId;
      if (time != null) {
        switch (queryId) {
          case 0:
            sql += " with future where dayofweek = " + time.getInt(0);
            break;
          case 1:
            sql += " with future where \"year\" = " + time.getInt(0);
            if (time.length() > 1) {
              sql += ", \"month\" = " + time.getInt(1);
            }
            if (time.length() > 2) {
              sql += ", dayofmonth = " + time.getInt(2);
            }
            break;
        }
      }
    } else {
      sql = "select "
          + (progressive ? "progressive " : "") + "avg(depdelay) depdelay, origin"
          + (progressive
          ? ", progressive_partition(), progressive_progress(), progressive_confidence(depdelay)"
          : "")
          + " from ontime1m where ";

      if (time != null) {
        switch (queryId) {
          case 0:
            sql += "(dayofweek = " + time.getInt(0) + ") and ";
            break;
          case 1:
            sql += "(\"year\" = " + time.getInt(0);
            if (time.length() > 1) {
              sql += " and \"month\" = " + time.getInt(1);
            }
            if (time.length() > 2) {
              sql += " and dayofmonth = " + time.getInt(2);
            }
            sql += ") and ";
            break;
        }
      }

      sql += String.format("(\"year\" >= %d and \"year\" <= %d)", YEARS_START, YEARS_END);
      sql += " group by origin";
    }

    return sql;
  }

  private String getTimeQuery(boolean progressive, int queryId, boolean view, String origin,
      JSONArray time) {
    String sql;
    String groupBy = null;

    if (view) {
      if (!progressive) {
        return getTimeQuery(false, queryId, false, origin, time);
      }

      sql = "select progressive *";

      switch (queryId) {
        case 0:
          groupBy = "dayofweek";
          break;
        case 1:
          if (time == null) {
            groupBy = "\"year\"";
          } else {
            groupBy = "\"month\"";
          }
      }

      sql += " from v_time_" + queryId;

      if (origin != null || time != null) {
        sql += " with future where ";
      }
      if (origin != null) {
        sql += "origin = '" + origin + "'";
      }
      if (time != null) {
        if (origin != null) {
          sql += ", ";
        }
        sql += "\"year\" = " + time.getInt(0);

        if (time.length() > 1) {
          sql += ", \"month\" = " + time.getInt(1);
        }
      }

      if (queryId == 1) {
        sql += " with future";
      }
    } else {
      sql = "select "
          + (progressive ? "progressive " : "") + "avg(depdelay) depdelay, "
          + (progressive
          ? "progressive_partition(), progressive_progress(), progressive_confidence(depdelay), "
          : "");

      switch (queryId) {
        case 0:
          sql += "dayofweek";
          groupBy = "dayofweek";
          break;
        case 1:
          if (time == null) {
            sql += "\"year\"";
            groupBy = "\"year\"";
          } else {
            sql += "\"month\"";
            groupBy = "\"month\"";
          }
      }

      sql += " from ontime1m where ";

      if (origin != null || time != null) {
        sql += "(";
      }

      if (origin != null) {
        sql += "origin = '" + origin + "'";
      }

      if (time != null) {
        if (origin != null) {
          sql += " and ";
        }
        sql += "\"year\" = " + time.getInt(0);
      }

      if (origin != null || time != null) {
        sql += ") and ";
      }

      sql += String.format("(\"year\" >= %d and \"year\" <= %d)", YEARS_START, YEARS_END);
    }
    sql += " group by " + groupBy;

    return sql;
  }

  private String getOriginViewName() {
    return "v_origin_" + queryId;
  }

  private String getOriginView() {
    String sql = "create progressive view " + getOriginViewName() + " as "
        + "select avg(depdelay) depdelay, progressive_partition(), progressive_progress(), progressive_confidence(depdelay), origin "
        + "from ontime1m "
        + "where ";
    if (queryId == 0) {
      sql += "(dayofweek = 1) future";
      for (int i = 2; i <= 7; i++) {
        sql += " or (dayofweek = " + i + ") future";
      }
    } else {
      sql += String.format("(((\"year\" = %d) future", YEARS_START);
      for (int i = YEARS_START + 1; i <= YEARS_END; i++) {
        sql += " or (\"year\" = " + i + ") future";
      }
      sql += ") and ((\"month\" = 1) future";
      for (int i = 2; i <= 12; i++) {
        sql += " or (\"month\" = " + i + ") future";
      }
      sql += String.format(")) and (\"year\" >= %d and \"year\" <= %d)", YEARS_START, YEARS_END);
    }
    sql += " group by origin";
    return sql;
  }

  private String getTimeViewName() {
    return "v_time_" + queryId;
  }

  private String getTimeView() {
    String sql = "create progressive view " + getTimeViewName() + " as "
        + "select progressive_confidence(depdelay), avg(depdelay) depdelay, progressive_partition(), progressive_progress(), ";

    if (queryId == 0) {
      sql += "dayofweek ";
    } else {
      sql += "\"year\" future, \"month\" future ";
    }

    sql += "from ontime1m "
        + "where ((origin = '" + ORIGINS[0] + "') future";
    for (int i = 1; i < ORIGINS.length; i++) {
      sql += " and (origin = '" + ORIGINS[i] + "') future";
    }
    sql += ")";

    if (queryId == 1) {
      sql += " and (";
      sql += String.format("(\"year\" = %d) future", YEARS_START);
      for (int i = YEARS_START + 1; i <= YEARS_END; i++) {
        sql += " or (\"year\" = " + i + ") future";
      }
      for (int i = 1; i <= 12; i++) {
        sql += " or (\"month\" = " + i + ") future";
      }
      sql += String.format(") and (\"year\" >= %d and \"year\" <= %d)", YEARS_START, YEARS_END);
    }

    if (queryId == 0) {
      sql += " group by dayofweek";
    } else {
      sql += " group by \"year\" future, \"month\" future";
    }

    return sql;
  }

  private void query(
      WebSocket socket,
      boolean progressive,
      int queryId,
      boolean view,
      String origin,
      JSONArray time,
      String start
  ) {
    try {
      if ("origin".equals(start)) {
        final String originSql = getOriginQuery(progressive, queryId, view, time);

        runQuery(socket, getStatement(progressive, DiagramType.ORIGIN), originSql, progressive,
            DiagramType.ORIGIN);
      } else if ("time".equals(start)) {
        final String timeSql = getTimeQuery(progressive, queryId, view, origin, time);

        runQuery(socket, getStatement(progressive, DiagramType.TIME), timeSql, progressive,
            DiagramType.TIME);
      } else {
        final String originSql = getOriginQuery(progressive, queryId, view, time);
        final String timeSql = getTimeQuery(progressive, queryId, view, origin, time);

        runQuery(socket, getStatement(progressive, DiagramType.ORIGIN), originSql, progressive,
            DiagramType.ORIGIN);
        runQuery(socket, getStatement(progressive, DiagramType.TIME), timeSql, progressive,
            DiagramType.TIME);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private void createView(String sql, DiagramType type) {
    appLogs.add(sql.toUpperCase());

    try (Statement statement = connections.get(true).get(type).createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private void dropView(String view, DiagramType type) {
    dropView(connections.get(true).get(type), view);
  }

  private void dropView(Connection connection, String view) {
    final String sql = String.format("drop progressive view %s", view);
    appLogs.add(sql.toUpperCase());

    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private void runSql(WebSocket socket, String sql, int id) {
    executorService.submit(() -> {
      try {
        consoleStatement = consoleConnection.createStatement();

        try (ResultSet result = consoleStatement.executeQuery(sql)) {
          while (result.next()) {
            final JSONObject json = rowToObject(result);
            socket.send(new JSONObject()
                .put("row", json)
                .put("id", id)
                .toString()
            );
          }
        }
      } catch (Throwable t) {
        t.printStackTrace();
        socket.send(new JSONObject()
            .put("error", t.getMessage())
            .put("id", id)
            .toString()
        );
      }
    });
  }

  private void runQuery(WebSocket socket, Statement statement, String sql, boolean progressive,
      DiagramType type) {
    appLogs.add(sql.toUpperCase());

    executorService.submit(
        () -> {
          try {
            try (ResultSet result = statement.executeQuery(sql)) {
              if (progressive) {
                JSONArray bulk = new JSONArray();
                final AtomicInteger partition = new AtomicInteger(-1);

                while (!statement.isClosed() && !result.isClosed() && result.next()) {
                  final JSONObject json = rowToObject(result);
                  if (json == null) {
                    break;
                  }

                  int newPartition = json.getInt("PROGRESSIVE_PARTITION");

                  if (partition.get() < 0) {
                    partition.set(newPartition);
                  } else if (partition.get() != newPartition) {
                    if (socket.isOpen()) {
                      socket.send(
                          new JSONObject()
                              .put("type", type == DiagramType.ORIGIN ? "origin" : "time")
                              .put("entries", bulk).toString());
                    }
                    bulk = new JSONArray();
                    partition.set(newPartition);
                  }

                  bulk.put(json);
                }

                if (bulk.length() > 0) {
                  if (socket.isOpen()) {
                    socket.send(
                        new JSONObject().put("type", type == DiagramType.ORIGIN ? "origin" : "time")
                            .put("entries", bulk).toString());
                  }
                }
              } else {
                final JSONArray bulk = new JSONArray();

                while (result.next()) {
                  bulk.put(rowToObject(result));
                }

                if (socket.isOpen()) {
                  socket.send(
                      new JSONObject().put("type", type == DiagramType.ORIGIN ? "origin" : "time")
                          .put("entries", bulk).toString());
                }
              }
            } catch (SQLException e) {
              if (!("This connection has been closed.".equals(e.getMessage())
                  || ("ERROR: canceling statement due to user request".equals(e.getMessage())))) {
                throw e;
              }
            }
          } catch (Throwable e) {
            System.err.println(type);
            e.printStackTrace();
          } finally {
            try {
              synchronized (statements) {
                if (!statement.isClosed()) {
                  statement.close();
                }
                if (statement == statements.get(progressive).get(type)) {
                  statements.get(progressive).remove(type);
                }
              }
            } catch (SQLException e) {
              // do nothing
            }
          }
        });
  }


  private JSONObject rowToObject(ResultSet row) throws SQLException {
    try {
      final ResultSetMetaData metaData = row.getMetaData();
      final JSONObject json = new JSONObject();

      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        final Object value = row.getObject(i);
        json.put(
            metaData.getColumnName(i),
            value instanceof BigDecimal ? ((BigDecimal) value).doubleValue() : value);
      }

      return json;
    } catch (SQLException e) {
      if (!"ResultSet closed".equals(e.getMessage())) {
        throw e;
      }
      return null;
    }
  }
}
