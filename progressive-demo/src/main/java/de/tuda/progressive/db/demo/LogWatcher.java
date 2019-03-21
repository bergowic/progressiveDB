package de.tuda.progressive.db.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashSet;
import java.util.Set;

public class LogWatcher {

  private final File file;

  private final Set<Listener> listeners = new HashSet<>();

  public LogWatcher(String path) throws IOException {
    this.file = new File(path);

    file.delete();
    file.createNewFile();
  }

  public void start() {
    new Thread(
            () -> {
              try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;

                WatchService watchService = FileSystems.getDefault().newWatchService();

                file.getParentFile()
                    .toPath()
                    .register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

                WatchKey key;
                while ((key = watchService.take()) != null) {
                  for (WatchEvent<?> event : key.pollEvents()) {
                    while ((line = br.readLine()) != null) {
                      if (line.startsWith("SELECT")) {
                        String log = line.replace(", 0 AS PARTITION", "");
                        listeners.forEach(l -> l.notify(log));
                      }
                    }
                  }
                  key.reset();
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            })
        .start();
  }

  public void register(Listener listener) {
    listeners.add(listener);
  }

  public interface Listener {
    void notify(String log);
  }
}
