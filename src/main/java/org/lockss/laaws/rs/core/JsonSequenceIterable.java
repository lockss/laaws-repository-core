/*

Copyright (c) 2000-2022, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

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
