/*
 * Copyright (c) 2017, Board of Trustees of Leland Stanford Jr. University,
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
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.DeferredFileOutputStream;
import org.lockss.laaws.rs.model.*;
import org.lockss.log.L4JLogger;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URLEncoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A volatile ("in-memory") implementation of WarcArtifactDataStore.
 */
public class VolatileWarcArtifactDataStore extends WarcArtifactDataStore {
  private final static L4JLogger log = L4JLogger.getLogger();
  private final static long DEFAULT_BLOCKSIZE = FileUtils.ONE_MB;
  private final static String DEFAULT_TMPWARCBASEPATH = "/tmp";

  protected Map<String, Map<String, Map<String, byte[]>>> repository;
  protected Map<String, byte[]> tempFiles;
  protected Map<String, RepositoryArtifactMetadata> repositoryMetadata;

  /**
   * Constructor.
   */
  public VolatileWarcArtifactDataStore() {
    this("volatile:///");
  }

  /**
   * For testing; this kind of data store ignores the base path.
   */
  protected VolatileWarcArtifactDataStore(String basePath) {
      super(basePath);
  }

  @Override
  public void initArtifactDataStore() {
    this.repository = new HashMap<>();
    this.repositoryMetadata = new HashMap<>();
    this.tempFiles = new HashMap<>();
  }

  @Override
  public void initCollection(String collectionId) {
    synchronized (repository) {
      repository.putIfAbsent(collectionId, new HashMap<>());
    }
  }

  @Override
  public void initAu(String collectionId, String auid) {
    synchronized (repository) {
      initCollection(collectionId);
      Map<String, Map<String, byte[]>> collection = repository.get(collectionId);
      collection.putIfAbsent(auid, new HashMap<>());
    }
  }

  @Override
  public void initWarc(String path) throws IOException {
    log.info("initWarc(String path): path = {}", path);

    VolatilePath vp = new VolatilePath(path);
    vp.storeByteArray(new byte[0]);
  }

  private class VolatilePath {
    private String path;

    public VolatilePath(String path) {
      this.path = path;
    }

    public String getPath() {
      return path;
    }

    public byte[] getByteArray() {
      if (inTmp()) {
        synchronized (tempFiles) {
          return tempFiles.get(getTmpFileName());
        }
      }

      Pattern p = Pattern.compile("^/collections/(.*)/(.*)/(.*)$");
      Matcher m = p.matcher(getPath());

      if (m.matches()) {
        String collection = m.group(1);
        String auid = m.group(2);
        String file = m.group(3);

        log.info("collection = {}", collection);
        log.info("auid = {}", auid);
        log.info("file = {}", file);

        synchronized (repository) {
          Map<String, Map<String, byte[]>> aus = repository.get(collection);
          Map<String, byte[]> au = aus.get(auid);
          return au.get(file);
        }
      }

      return null;
    }

    public OutputStream getAppendableOutputStream() throws IOException {
      synchronized (repository) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(getInputStream(), baos);

        return baos;
      }
    }

    public InputStream getInputStream() {
      synchronized (repository) {
        byte[] bytes = getByteArray();

        log.info("bytes.length = {}", bytes.length);

        return new ByteArrayInputStream(bytes);
      }
    }

    public void storeByteArray(byte[] bytes) throws IOException {
      if (inTmp()) {
        synchronized (tempFiles) {
          tempFiles.put(getTmpFileName(), bytes);
        }
      }

      Pattern p = Pattern.compile("^/collections/(.*)/(.*)/(.*)$");
      Matcher m = p.matcher(getPath());

      if (m.matches()) {
        String collection = m.group(1);
        String auid = m.group(2);
        String file = m.group(3);

        log.info("collection = {}", collection);
        log.info("auid = {}", auid);
        log.info("file = {}", file);

        initAu(collection, auid);

        synchronized (repository) {
          Map<String, Map<String, byte[]>> aus = repository.get(collection);
          Map<String, byte[]> au = aus.get(auid);
          au.put(file, bytes);
        }
      }
    }

    private String getTmpFileName() {
      if (inTmp()) {
        Pattern p = Pattern.compile("^" + getTmpWarcBasePath() + "/(.*)$");
        Matcher m = p.matcher(getPath());

        if (m.matches()) {
          return m.group(1);
        }
      }

      return null;
    }

    private boolean inTmp() {
      return path.startsWith(getTmpWarcBasePath());
    }
  }

  @Override
  public boolean removeWarc(String warcPath) {
    synchronized (repository) {
      VolatilePath vp = new VolatilePath(warcPath);
    }

    return false;
  }

  @Override
  public Artifact addArtifactData(ArtifactData artifactData) throws IOException {
    if (artifactData == null) {
      throw new IllegalArgumentException("Null artifact data");
    }

    // Get the artifact identifier
    ArtifactIdentifier artifactId = artifactData.getIdentifier();

    if (artifactId == null) {
      throw new IllegalArgumentException("Artifact data has null identifier");
    }

    log.info(String.format(
        "Adding artifact (%s, %s, %s, %s, %s)",
        artifactId.getId(),
        artifactId.getCollection(),
        artifactId.getAuid(),
        artifactId.getUri(),
        artifactId.getVersion()
    ));

    // Serialize artifact to WARC record and get the number of bytes in the serialization
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    long bytesWritten = writeArtifactData(artifactData, baos);
    baos.close();

    // Get an available temporary WARC from the temporary WARC pool
    WarcFile tmpWarcFile = tmpWarcPool.findWarcFile(bytesWritten);
    String tmpWarcFilePath = tmpWarcFile.getPath();

    // Initialize the WARC
    initWarc(tmpWarcFilePath);

    // The offset for the record to be appended to this WARC is the length of the WARC file (i.e., its end)
    long offset = getFileLength(tmpWarcFilePath);

    log.info("tmpWarcFilePath = {}, offset = {}", tmpWarcFilePath, offset);

    try (
        // Get an (appending) OutputStream to the temporary WARC file
        OutputStream output = getAppendableOutputStream(tmpWarcFilePath)
    ) {
      // Write serialized artifact to temporary WARC file
      baos.writeTo(output);

      // Save byte array
      VolatilePath tmpWarcPath = new VolatilePath(tmpWarcFilePath);
      tmpWarcPath.storeByteArray(((ByteArrayOutputStream)output).toByteArray());

      log.info(String.format(
          "Wrote %d bytes starting at byte offset %d to %s; size is now %d",
          bytesWritten,
          offset,
          tmpWarcFilePath,
          offset + bytesWritten
      ));
    }

    // Update temporary WARC stats and return to pool
    tmpWarcFile.setLength(offset + bytesWritten);
    tmpWarcPool.returnWarcFile(tmpWarcFile);

    // Set artifact data storage URL
    artifactData.setStorageUrl(makeStorageUrl(tmpWarcFilePath, offset));

    // Write artifact data metadata - TODO: Generalize this to write all of an artifact's metadata
    artifactData.setRepositoryMetadata(new RepositoryArtifactMetadata(artifactId, false, false));
    initWarc(getAuMetadataWarcPath(artifactId, artifactData.getRepositoryMetadata()));
    updateArtifactMetadata(artifactId, artifactData.getRepositoryMetadata());

    // Create a new Artifact object to return; should reflect artifact data as it is in the data store
    Artifact artifact = new Artifact(
        artifactId,
        false,
        artifactData.getStorageUrl(),
        artifactData.getContentLength(),
        artifactData.getContentDigest()
    );

    return artifact;
  }

  /**
   * Updates and writes associated metadata of an artifact to this store.
   *
   * @param artifactId
   *          A (@code ArtifactIdentifier) that identifies the artifact to update.
   * @param artifactMetadata
   *          RepositoryArtifactMetadata update the artifact with, and write to the store.
   * @return A representation of the RepositoryArtifactMetadata as it is now stored.
   */
//  @Override
  public RepositoryArtifactMetadata updateArtifactMetadata2(ArtifactIdentifier artifactId, RepositoryArtifactMetadata artifactMetadata) {
    if (artifactId == null) {
      throw new NullPointerException("artifactId is null");
    }

    if (artifactMetadata == null) {
      throw new NullPointerException("artifactMetadata is null");
    }

    repositoryMetadata.replace(artifactId.getId(), artifactMetadata);
    return repositoryMetadata.get(artifactId.getId());
  }

  @Override
  protected String getTmpWarcBasePath() {
    return DEFAULT_TMPWARCBASEPATH;
  }

  @Override
  protected long getBlockSize() {
    return DEFAULT_BLOCKSIZE;
  }

  /**
   * <p>
   * Given a map, either returns the value associated with the given key (if the
   * key is associated with a non-null value), or sets a mapping from the given
   * initial value and returns it (if the key is not associated with a value or
   * is associated with {@code null}).
   * </p>
   * <p>
   * This is slightly different from {@link Map#putIfAbsent(Object, Object)},
   * which returns {@code null} if the key is not associated with a value or is
   * associated with {@code null}, which does not enable the caller to use the
   * returned value immediately.
   * </p>
   * 
   * @param map
   *          A map.
   * @param key
   *          A key in the map.
   * @param initial
   *          An initial value used to create a mapping with the key if the key
   *          is not associated with a value or is associated with {@code null}
   *          in the map.
   * @param <K>
   *          The key type in the map.
   * @param <T>
   *          The value type in the map.
   * @return The value associated with the key after the side effect: either the
   *         original value if the key was associated with one, or the initial
   *         value used to initialize a mapping with the key if they key was not
   *         associated with a value or was associated with {@code null}.
   */
  protected static <K, T> T getInitialize(Map<K, T> map, K key, T initial) {
    T current = map.putIfAbsent(key, initial);
    return current == null ? initial : current;
  }
    
  @Override
  public long getFileLength(String filePath) throws IOException {
      log.info("getFileLength().filePath = {}", filePath);
      VolatilePath vp = new VolatilePath(filePath);
      return vp.getByteArray().length;
  }

  public String makeStorageUrl(ArtifactIdentifier ident) {
    try {
      return makeStorageUrl(String.format("/%s/%s/%s/%s/%s",
                                          ident.getCollection(),
                                          ident.getAuid(),
                                          URLEncoder.encode(ident.getUri(), "UTF-8"),
                                          ident.getVersion(),
                                          ident.getId()),
                            0L);
    }
    catch (UnsupportedEncodingException e) {
      log.error("Internal error", e);
      throw new UncheckedIOException(e);
    }
  }
  
  @Override
  public String makeStorageUrl(String filePath, String offset) {
    MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
    params.add("offset", offset);
    return makeStorageUrl(filePath, params);
  }

  @Override
  public String makeStorageUrl(String filePath, MultiValueMap<String, String> params) {
    UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("volatile://" + getBasePath() + filePath);
    uriBuilder.queryParams(params);
    return uriBuilder.toUriString();
  }

  @Override
  public OutputStream getAppendableOutputStream(String filePath) throws IOException {
      VolatilePath vp = new VolatilePath(filePath);
      return vp.getAppendableOutputStream();
  }

  @Override
  public InputStream getInputStream(String filePath) throws IOException {
    throw new UnsupportedOperationException();
  }  

  @Override
  public InputStream getInputStreamAndSeek(String filePath, long seek) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public InputStream getWarcRecordInputStream(String storageUrl) throws IOException {
    log.info("storageUrl = {}", storageUrl);
    VolatileStorageUrl vsu = new VolatileStorageUrl(storageUrl);
    return vsu.getInputStream();
  }

  private class VolatileStorageUrl {
    private String path;
    private long offset;

    public VolatileStorageUrl(String storageUrl) {
      Pattern p = Pattern.compile("volatile://volatile(/.*)\\?offset=(\\d+)$");
      Matcher m = p.matcher(storageUrl);

      if (m.matches()) {
        this.path = m.group(1);
        this.offset = Long.parseUnsignedLong(m.group(2));

        log.info("path = {}", this.path);
        log.info("offset = {}", this.offset);
      } else {
        throw new IllegalArgumentException("Malformed storage URL");
      }
    }

    public InputStream getInputStream() throws IOException {
      VolatilePath vp = new VolatilePath(getPath());
      InputStream input = vp.getInputStream();
      input.skip(getOffset());
      return input;
    }

    public String getPath() {
      return this.path;
    }

    public long getOffset() {
      return this.offset;
    }
  }

  @Override
  protected Artifact moveToPermanentStorage(Artifact artifact) throws IOException {
    // Read artifact data from current WARC file
    ArtifactData artifactData = getArtifactData(artifact);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Serialize artifact to WARC record and get the number of bytes in the serialization
    long bytesWritten = writeArtifactData(artifactData, baos);
    baos.close();

    // Get the current active permanent WARC for this AU
    String dst = getActiveWarcPath(artifact.getCollection(), artifact.getAuid());
    initWarc(dst);

    // Artifact will be appended as a WARC record to this WARC file so its offset is the current length of the file
    long offset = getFileLength(dst);

    if (bytesWritten + offset > getThresholdWarcSize()) {
      // If the WARC record alone is over the size threshold, then we're going to be writing a WARC that goes past the
      // threshold anyway. So we have two options: 1) write to a new WARC or, 2) the current one. Which one? The one
      // that maximizes the last block.
      if (!((bytesWritten > getThresholdWarcSize()) &&
          (getBytesUsedLastBlock(bytesWritten + offset) > getBytesUsedLastBlock(bytesWritten) + getBytesUsedLastBlock(offset)))) {

        // Seal the active WARC
        sealActiveWarc(artifact.getCollection(), artifact.getAuid());

        // Initialize the new active WARC and retrieve its size
        dst = getActiveWarcPath(artifact.getCollection(), artifact.getAuid());
        initWarc(dst);
        offset = getFileLength(dst);
      }
    }

    try (
        // Get an (appending) OutputStream to the WARC file
        OutputStream output = getAppendableOutputStream(dst)
    ) {
      // Write serialized artifact to permanent WARC file
      baos.writeTo(output);
      output.flush();

      // Save byte array
      VolatilePath dst_vp = new VolatilePath(dst);
      dst_vp.storeByteArray(((ByteArrayOutputStream)output).toByteArray());

      log.info(String.format(
          "Committed: %s: Wrote %d bytes starting at byte offset %d to %s; size is now %d",
          artifact.getIdentifier().getId(),
          bytesWritten,
          offset,
          dst,
          offset + bytesWritten
      ));
    }

    // Set the artifact's new storage URL and update the index
    artifact.setStorageUrl(makeStorageUrl(dst, offset));
    artifactIndex.updateStorageUrl(artifact.getId(), artifact.getStorageUrl());

    // Immediately seal active WARC if size threshold has been met or exceeded
    if (offset + bytesWritten >= getThresholdWarcSize()) {
      sealActiveWarc(artifact.getCollection(), artifact.getAuid());
    }

    return artifact;
  }

  @Override
  public void moveWarc(String srcPath, String dstPath) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<String> findWarcs(String basePath) throws IOException {
      throw new UnsupportedOperationException();
  }

  /**
   * Returns a boolean indicating whether this artifact store is ready.
   *
   * Always true in volatile implementation.
   *
   * @return
   */
  @Override
  public boolean isReady() {
    return true;
  }
}
