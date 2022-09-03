package org.lockss.laaws.rs.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.lockss.log.L4JLogger;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Iterable over a sequence of objects
 *
 * @param <T>
 */
public class JsonSequenceIterable<T> implements Iterable<T>, Closeable {
  private static L4JLogger log = L4JLogger.getLogger();
  private static ObjectMapper objMapper = new ObjectMapper();

  private InputStream input;
  private Class<T> type;
  private boolean isUsed;

  public JsonSequenceIterable(InputStream input, Class<T> type) {
    this.input = input;
    this.type = type;
  }

  @NotNull
  @Override
  public Iterator<T> iterator() {
    synchronized (this) {
      if (isUsed) throw new IllegalStateException("Cannot use this iterator more than once");
      isUsed = true;
    }

    try {
      ObjectReader objReader = objMapper.readerFor(type);
      return objReader.readValues(new BufferedInputStream(input));
    } catch (IOException e) {
      log.error("Could not read JSON array", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    close();
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(this.input);
  }
}
