package de.tuda.progressive.db.demo;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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

public class Server extends WebSocketServer {

  private ExecutorService executorService = Executors.newCachedThreadPool();

  private Map<Boolean, Connection> connections =
      new HashMap<Boolean, Connection>() {
        {
          try {
            put(
                false,
                DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/progressive", "postgres", "postgres"));
            put(true, DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:1337"));
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
      };

  private final Map<String, Statement> statements = new HashMap<>();

  private final LogWatcher logWatcher;

  private final List<String> appLogs = new ArrayList<>();

  public Server(LogWatcher logWatcher) throws UnknownHostException {
    super(new InetSocketAddress(8081));

    this.logWatcher = logWatcher;
  }

  @Override
  public void onOpen(WebSocket webSocket, ClientHandshake clientHandshake) {}

  @Override
  public void onClose(WebSocket webSocket, int i, String s, boolean b) {}

  @Override
  public void onMessage(WebSocket webSocket, String s) {
    if ("log".equals(s)) {
      logWatcher.register(
          log -> {
            sendLog(webSocket, true, log);
          });
      appLogs.forEach(log -> sendLog(webSocket, false, log));
    } else {
      JSONObject json = new JSONObject(s);

      try {
        final int id = json.getInt("id");
        final boolean progressive = json.getBoolean("progressive");
        query(
            webSocket,
            getStatement(progressive, id),
            id,
            json.getString("sql"),
            json.optString("sql-init", null),
            progressive);

        final String sqlInit = json.optString("sql-init-display", null);
        final String sql = json.optString("sql-display", json.getString("sql")).toUpperCase();

        if (progressive) {
          if (sqlInit != null) {
            appLogs.add(sqlInit.toUpperCase());
            sendLog(webSocket, false, sqlInit.toUpperCase());
          }
          sendLog(webSocket, false, sql);
          appLogs.add(sql);
        }
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  private void sendLog(WebSocket socket, boolean progressive, String log) {
    if (!socket.isClosed()) {
      socket.send(new JSONObject().put("progressive", progressive).put("log", log).toString());
    }
  }

  @Override
  public void onError(WebSocket webSocket, Exception e) {}

  @Override
  public void onStart() {}

  private Statement getStatement(boolean progressive, int id) throws SQLException {
    final Connection connection = connections.get(progressive);
    final String key = getKey(progressive, id);
    synchronized (statements) {
      Statement statement = statements.get(key);
      if (statement != null) {
        statement.close();
      }

      statement = connection.createStatement();
      statements.put(key, statement);
      return statement;
    }
  }

  private void query(
      WebSocket socket,
      Statement statement,
      int id,
      String sql,
      String sqlInit,
      boolean progressive) {
    executorService.submit(
        () -> {
          try {
            if (sqlInit != null) {
              statement.execute(sqlInit);
            }

            try (ResultSet result = statement.executeQuery(sql)) {
              if (progressive) {
                JSONArray bulk = new JSONArray();
                final AtomicInteger partition = new AtomicInteger(0);

                while (result.next()) {
                  final JSONObject json = rowToObject(result);

                  bulk.put(json);

                  if ((id == 0 || id == 10) && bulk.length() == 40
                      || (id == 1 || id == 11) && bulk.length() == 7) {
                    socket.send(new JSONObject().put("id", id).put("entries", bulk).toString());
                    bulk = new JSONArray();
                    partition.set(json.getInt("PROGRESSIVE_PARTITION"));
                  }
                }

                if (bulk.length() > 0) {
                  socket.send(new JSONObject().put("id", id).put("entries", bulk).toString());
                }
              } else {
                final JSONArray bulk = new JSONArray();

                while (result.next()) {
                  bulk.put(rowToObject(result));
                }

                socket.send(new JSONObject().put("id", id).put("entries", bulk).toString());
              }
            }
          } catch (Throwable e) {
            e.printStackTrace();
          } finally {
            try {
              synchronized (statements) {
                if (!statement.isClosed()) {
                  statement.close();
                }
                final String key = getKey(progressive, id);
                if (statement == statements.get(key)) {
                  statements.remove(key);
                }
              }
            } catch (SQLException e) {
              // do nothing
            }
          }
        });
  }

  private String getKey(boolean progressive, int id) {
    return progressive + "-" + id;
  }

  private JSONObject rowToObject(ResultSet row) throws SQLException {
    final ResultSetMetaData metaData = row.getMetaData();
    final JSONObject json = new JSONObject();

    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      final Object value = row.getObject(i);
      json.put(
          metaData.getColumnName(i),
          value instanceof BigDecimal ? ((BigDecimal) value).doubleValue() : value);
    }

    return json;
  }
}
