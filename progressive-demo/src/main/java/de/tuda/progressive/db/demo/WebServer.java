package de.tuda.progressive.db.demo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class WebServer {

  private static final String ASSETS = "/assets/";
  private static final String WEB_JARS = "/META-INF/resources/webjars/";
  private static final String WEB = "/web";

  private final int port;

  private final String path;

  private HttpServer server;

  public WebServer(int port, String path) {
    this.port = port;
    this.path = path;
  }

  public void start() throws IOException {
    if (server != null) {
      throw new IllegalStateException("server already started");
    }

    server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(path, new Handler());
    server.setExecutor(null);
    server.start();
  }

  public void stop() {
    if (server == null) {
      throw new IllegalStateException("server is not started");
    }

    server.stop(0);
    server = null;
  }

  private static class Handler implements HttpHandler {
    private String getContent(String base, String path) {
      try {
        final InputStream input = Main.class.getResourceAsStream(base + path);
        if (input == null) {
          return null;
        }
        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(input))) {
          return buffer.lines().collect(Collectors.joining("\n"));
        }
      } catch (Throwable t) {
        t.printStackTrace();
        return null;
      }
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      final String uri = exchange.getRequestURI().getPath();

      int code = 200;
      String content;
      if (uri.startsWith(ASSETS)) {
        final String asset = uri.substring(ASSETS.length());
        content = getContent(WEB_JARS, asset);
      } else {
        content = getContent(WEB, uri);
      }

      if (content == null) {
        code = 404;
        content = "Not found";
      }

      exchange.getResponseHeaders().set("Content-Type", getContentType(uri));
      exchange.sendResponseHeaders(code, content.getBytes().length);

      try (OutputStream output = exchange.getResponseBody()) {
        output.write(content.getBytes());
      }
    }

    private String getContentType(String path) {
      if (path.endsWith(".js")) {
        return "application/javascript";
      }

      return "";
    }
  }
}
