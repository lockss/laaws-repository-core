/*
 * Copyright (c) 2017-2019, Board of Trustees of Leland Stanford Jr. University,
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.lockss.laaws.rs.io.storage.warc;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.VolatileArtifactIndex;
import org.lockss.log.L4JLogger;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A volatile ("in-memory") implementation of WarcArtifactDataStore.
 */
public class VolatileWarcArtifactDataStore extends WarcArtifactDataStore {
  private final static L4JLogger log = L4JLogger.getLogger();
  private final static long DEFAULT_BLOCKSIZE = FileUtils.ONE_MB;
  private final static String DEFAULT_TMPWARCBASEPATH = "/tmp";
  private final static String DEFAULT_BASEPATH = "/";

  protected final Map<String, ByteArrayOutputStream> warcs;

  /**
   * Constructor.
   */
  public VolatileWarcArtifactDataStore() {
    this(new VolatileArtifactIndex(), DEFAULT_BASEPATH);
  }

  public VolatileWarcArtifactDataStore(ArtifactIndex index) {
    this(index, DEFAULT_BASEPATH);
  }

  /**
   * For testing; this kind of data store ignores the base path.
   */
  private VolatileWarcArtifactDataStore(ArtifactIndex index, String basePath) {
    super(index, basePath);

    this.warcs = new HashMap<>();

    this.storageUrlPattern =
        Pattern.compile("(volatile://)(" + (getBasePath().equals("/") ? "" : getBasePath()) + ")([^?]+)\\?offset=(\\d+)&length=(\\d+)");
  }

  @Override
  public void initArtifactDataStore() throws IOException {
    reloadDataStoreState();
    dataStoreState = DataStoreState.INITIALIZED;
  }

  @Override
  public void initCollection(String collectionId) {
    // NOP
  }

  @Override
  public void initAu(String collectionId, String auid) {
    // NOP
  }

  @Override
  public void initWarc(String path) {
    synchronized (warcs) {
      warcs.putIfAbsent(path, new ByteArrayOutputStream());
    }
  }

  @Override
  public boolean removeWarc(String path) {
    synchronized (warcs) {
      warcs.remove(path);
      return true;
    }
  }

  @Override
  protected String getTmpWarcBasePath() {
    return getBasePath() + DEFAULT_TMPWARCBASEPATH;
  }

  @Override
  protected long getBlockSize() {
    return DEFAULT_BLOCKSIZE;
  }

  @Override
  public long getWarcLength(String warcPath) {
    synchronized (warcs) {
      ByteArrayOutputStream warc = warcs.get(warcPath);
      if (warc != null) {
        return warc.size();
      }
    }

    return 0L;
  }

  @Override
  public String makeStorageUrl(String filePath, MultiValueMap<String, String> params) {
    UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("volatile://" + getBasePath() + filePath);
    uriBuilder.queryParams(params);

    return uriBuilder.toUriString();
  }

  @Override
  public OutputStream getAppendableOutputStream(String path) {
    synchronized (warcs) {
      return warcs.get(path);
    }
  }

  @Override
  public InputStream getInputStreamAndSeek(String path, long seek) throws IOException {
    synchronized (warcs) {
      ByteArrayOutputStream warc = warcs.get(path);
      if (warc != null) {
        InputStream is = warc.toInputStream();
        long skipped = is.skip(seek);
        assert(skipped == seek);
        return is;
      }
    }

    return null;
  }

  @Override
  public Collection<String> findWarcs(String basePath) {
    synchronized (warcs) {
      log.debug("basePath = {}", basePath);
      log.debug("warcs.keySet() = {}", warcs.keySet());

      return warcs.keySet().stream()
          .filter(path -> path.startsWith(basePath))
          .filter(path -> path.endsWith(WARC_FILE_EXTENSION))
          .collect(Collectors.toList());
    }
  }

  /**
   * Returns a boolean indicating whether this artifact store is ready.
   *
   * Always true in volatile implementation.
   *
   * @return {@code true}
   */
  @Override
  public boolean isReady() {
    return dataStoreState == DataStoreState.INITIALIZED;
  }

  @Override
  protected String getAbsolutePath(String path) {
    return path;
  }
}
