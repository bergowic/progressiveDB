package de.tuda.progressive.db;

import de.tuda.progressive.db.statement.ProgressiveStatement;
import de.tuda.progressive.db.statement.ProgressiveStatementFactory;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.jdbc.StatementInfo;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.remote.TypedValue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class ProgressiveMeta extends JdbcMeta {

	private static final String KEYWORD_PROGRESSIVE = "PROGRESSIVE";

	private final ConcurrentMap<Integer, ProgressiveStatement> statements = new ConcurrentHashMap<>();

	private final ProgressiveStatementFactory statementFactory;

	public ProgressiveMeta(String url, ProgressiveStatementFactory statementFactory) throws SQLException {
		super(url);

		this.statementFactory = statementFactory;
	}

	public ProgressiveMeta(String url, String user, String password, ProgressiveStatementFactory statementFactory) throws SQLException {
		super(url, user, password);

		this.statementFactory = statementFactory;
	}

	public ProgressiveMeta(String url, Properties info, ProgressiveStatementFactory statementFactory) throws SQLException {
		super(url, info);

		this.statementFactory = statementFactory;
	}

	public ProgressiveMeta(String url, Properties info, MetricsSystem metrics, ProgressiveStatementFactory statementFactory) throws SQLException {
		super(url, info, metrics);

		this.statementFactory = statementFactory;
	}

	@Override
	public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
		System.out.println(sql);

		return prepareProgressiveStatement(sql, statement -> {
			try {
				StatementHandle handle = new StatementHandle(
						ch.id,
						getStatementIdGenerator().getAndIncrement(),
						signature(statement.getMetaData())
				);

				statements.putIfAbsent(handle.id, statement);
				return handle;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}).orElseGet(() -> super.prepare(ch, sql, maxRowCount));
	}

	@Override
	public StatementHandle createStatement(ConnectionHandle ch) {
		return super.createStatement(ch);
	}

	@Override
	public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
		ExecuteResult result = hookPrepareAndExecute(h, sql, maxRowCount, maxRowsInFirstFrame);
		if (result != null) {
			return result;
		}

		return super.prepareAndExecute(h, sql, maxRowCount, maxRowsInFirstFrame, callback);
	}

	@Override
	public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, long maxRowCount) throws NoSuchStatementException {
		return super.execute(h, parameterValues, maxRowCount);
	}

	@Override
	public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame) throws NoSuchStatementException {
		ProgressiveStatement statement = statements.get(h.id);
		if (statement == null) {
			return super.execute(h, parameterValues, maxRowsInFirstFrame);
		}

		return execute(h, statement);
	}

	@Override
	public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) throws NoSuchStatementException, MissingResultsException {
		ProgressiveStatement statement = statements.get(h.id);
		if (statement == null) {
			return super.fetch(h, offset, fetchMaxRowCount);
		}

		try {
			return createFrame(offset, statement.isDone(), statement.getResultSet());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void closeStatement(StatementHandle h) {
		ProgressiveStatement statement = statements.get(h.id);
		if (statement == null) {
			super.closeStatement(h);
		} else {
			statement.close();
		}
	}

	private void assertStatementExists(StatementHandle h) throws NoSuchStatementException {
		final StatementInfo info = getStatementCache().getIfPresent(h.id);
		if (info == null) {
			throw new NoSuchStatementException(h);
		}
	}

	private ExecuteResult hookPrepareAndExecute(StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame) throws NoSuchStatementException {
		assertStatementExists(h);

		return prepareProgressiveStatement(sql, statement -> {
			statements.putIfAbsent(h.id, statement);
			return execute(h, statement);
		}).orElse(null);
	}

	private ExecuteResult execute(StatementHandle h, ProgressiveStatement statement) {
		ResultSet resultSet = statement.getResultSet();

		try {
			Frame frame = createFrame(0, statement.isDone(), resultSet);
			MetaResultSet result = MetaResultSet.create(h.connectionId, h.id, false, signature(statement.getMetaData()), frame);
			return new ExecuteResult(Collections.singletonList(result));
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private Frame createFrame(long offset, boolean done, ResultSet resultSet) throws SQLException {
		if (resultSet == null) {
			return Frame.create(offset, true, Collections.emptyList());
		}

		final int columnCount = resultSet.getMetaData().getColumnCount();
		final List<Object> rows = new ArrayList<>();

		while (resultSet.next()) {
			Object[] columns = new Object[columnCount];
			for (int i = 0; i < columnCount; i++) {
				columns[i] = resultSet.getObject(i + 1);
			}
			rows.add(columns);
			offset++;
		}

		return Frame.create(offset, done, rows);
	}

	private <T> Optional<T> prepareProgressiveStatement(String sql, Function<ProgressiveStatement, T> success) {
		if (sql.toUpperCase().matches("^\\s*SELECT\\sPROGRESSIVE[\\s\\S]*")) {
			ProgressiveStatement statement = statementFactory.prepare(removeProgressiveKeyword(sql));
			return Optional.of(success.apply(statement));
		}

		return Optional.empty();
	}

	private String removeProgressiveKeyword(String sql) {
		final int startProgressive = sql.toUpperCase().indexOf(KEYWORD_PROGRESSIVE);
		final int endProgressive = startProgressive + KEYWORD_PROGRESSIVE.length();

		return sql.substring(0, startProgressive) + sql.substring(endProgressive);
	}
}
