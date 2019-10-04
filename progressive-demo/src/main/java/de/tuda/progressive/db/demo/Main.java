package de.tuda.progressive.db.demo;

import java.io.File;

import de.tuda.progressive.db.ProgressiveDbServer;
import java.util.Properties;

public class Main {

  public static void main(String[] args) throws Exception {
    final String URL = args[0];
    final String USER = args[1];
    final String PASSWORD = args[2];
    final int port = 9001;
    final Properties progressiveProps = new Properties();
    final Properties nativeProps = new Properties();

    progressiveProps.put("url","jdbc:avatica:remote:url=http://localhost:" + port);
    nativeProps.put("url", URL);
    nativeProps.put("user", USER);
    nativeProps.put("password", PASSWORD);

    final WebServer webServer = new WebServer(8000, "/");
    webServer.start();

    final ProgressiveDbServer server = new ProgressiveDbServer.Builder()
        .source(URL, USER, PASSWORD)
        .tmp("jdbc:sqlite::memory:")
        .meta(String.format("jdbc:sqlite:%s/progressivedb-demo.sqlite", System.getProperty("user.home")))
        .port(port)
        .build();

    server.start();

    final LogWatcher logWatcher = new LogWatcher(new File(System.getProperty("user.home") + "/progressive-db.log"));
    new Server(progressiveProps, nativeProps, logWatcher).start();
    logWatcher.start();
  }
}
