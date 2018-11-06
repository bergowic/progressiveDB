package de.tuda.progressive.db.benchmark.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

public class IOUtils {

	private IOUtils() {
	}

	public static String read(InputStream input) {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		byte[] buffer = new byte[4096];
		int length;
		try {
			while ((length = input.read(buffer)) != -1) {
				result.write(buffer, 0, length);
			}
			return result.toString(StandardCharsets.UTF_8.name());
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static String read(File file) {
		try (InputStream input = new FileInputStream(file)) {
			return read(input);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static void closeSafe(AutoCloseable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception e) {
				// do nothing
			}
		}
	}

	public static Properties loadProperties(File file) {
		try (InputStream input = new FileInputStream(file)) {
			Properties properties = new Properties();
			properties.load(input);
			return properties;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static void writeCSVRow(OutputStream output, List<String> data) {
		final String row = String.join(";", data);
		try {
			output.write(row.getBytes(StandardCharsets.UTF_8));
			output.write('\n');
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
