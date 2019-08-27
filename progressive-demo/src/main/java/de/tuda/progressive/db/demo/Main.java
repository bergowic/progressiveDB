package de.tuda.progressive.db.demo;

import de.tuda.progressive.db.ProgressiveDbServer;
import java.io.File;

public class Main {

  public static void main(String[] args) throws Exception {
    final String URL = args[0];
    final String USER = args[1];
    final String PASSWORD = args[2];

    final WebServer webServer = new WebServer(8000, "/");
    webServer.start();

    final ProgressiveDbServer server = new ProgressiveDbServer.Builder()
        .source(URL, USER, PASSWORD)
        .tmp("jdbc:sqlite::memory:")
        .meta(String.format("jdbc:sqlite:%s/progressivedb.sqlite", System.getProperty("user.home")))
        .port(1337)
        .build();

    server.start();

    final LogWatcher logWatcher = new LogWatcher(new File(System.getProperty("user.home") + "/progressive-db.log"));
    new Server(logWatcher).start();
    logWatcher.start();
  }
}
