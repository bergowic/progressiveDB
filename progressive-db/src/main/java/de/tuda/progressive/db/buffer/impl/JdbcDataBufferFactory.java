package de.tuda.progressive.db.buffer.impl;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.buffer.DataBufferFactory;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.driver.DbDriverFactory;
import de.tuda.progressive.db.statement.context.impl.JdbcContext;
import de.tuda.progressive.db.statement.context.impl.JdbcUpsertContext;
import de.tuda.progressive.db.util.SqlUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcDataBufferFactory implements DataBufferFactory<JdbcContext> {

	private final String url;

	private final DataSource dataSource;

	public JdbcDataBufferFactory(String url) {
		this(url, null);
	}

	public JdbcDataBufferFactory(DataSource dataSource) {
		this(null, dataSource);
	}

	private JdbcDataBufferFactory(String url, DataSource dataSource) {
		if (url == null && dataSource == null) {
			throw new IllegalArgumentException("set either url or dataSource");
		}

		this.url = url;
		this.dataSource = dataSource;
	}

	@Override
	public DataBuffer create(JdbcContext context) {
		Connection connection = null;
		DbDriver driver = null;

		try {
			connection = getConnection();
			driver = getDriver(connection);

			return create(driver, connection, context);
		} catch (Throwable t) {
			SqlUtils.closeSafe(connection);

			throw t;
		}
	}

	private DataBuffer create(DbDriver driver, Connection connection, JdbcContext context) {
		if (context instanceof JdbcUpsertContext) {
			return new JdbcUpsertDataBuffer(driver, connection, (JdbcUpsertContext) context);
		}

		throw new IllegalArgumentException("context not supported: " + context.getClass());
	}

	private Connection getConnection() {
		try {
			if (url != null) {
				return DriverManager.getConnection(url);
			}
			if (dataSource != null) {
				return dataSource.getConnection();
			}
		} catch (SQLException e) {
			// TODO
			throw new RuntimeException(e);
		}

		// should never happen
		throw new IllegalStateException("url and dataSource are null");
	}

	private DbDriver getDriver(Connection connection) {
		String connectionUrl = url;

		if (connectionUrl == null) {
			try {
				connectionUrl = connection.getMetaData().getURL();
			} catch (SQLException e) {
				// TODO
				throw new RuntimeException(e);
			}
		}

		return DbDriverFactory.create(connectionUrl);
	}
}
