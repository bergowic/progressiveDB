package de.tuda.progressive.db.demo;

public class Main {

  public static void main(String[] args) throws Exception {
    LogWatcher logWatcher = new LogWatcher("C:\\tmp\\progressive-db.log");
    new Server(logWatcher).start();
    logWatcher.start();
  }
}
