package de.tuda.progressive.db;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.ProtobufMeta;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class PServer {

	private static final Logger log = LoggerFactory.getLogger(PServer.class);

	public static void main(String[] args) throws Exception {
		final String url = "jdbc:monetdb://localhost:50000";
		final String user = "monetdb";
		final String password = "monetdb";

		PServer server = new PServer(url, user, password);
		server.start();
	}

	private final String url;
	private final String user;
	private final String password;

	private HttpServer server;

	public PServer(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
	}

	public synchronized void start() throws SQLException {
		if (server == null) {
			log.info("starting");

			ProtobufMeta jdbcMeta = new JdbcMeta(url, user, password);
			Meta meta = new PMeta(jdbcMeta);
			Service service = new PService(meta);

			server = new HttpServer.Builder()
					.withHandler(service, Driver.Serialization.JSON)
					.withPort(1337)
					.build();
			server.start();

			Runtime.getRuntime().addShutdownHook(
					new Thread(this::stop)
			);

			new Thread(() -> {
				try {
					server.join();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}).start();
		}
	}

	public synchronized void stop() {
		if (server != null) {
			log.info("shutting down");
			server.stop();
			server = null;
		}
	}
}
