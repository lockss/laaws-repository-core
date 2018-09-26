/*
 * Copyright (c) 2017-2018, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.io.storage.hdfs;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Apache Hadoop Distributed File System (HDFS) implementation of WarcArtifactDataStore.
 */
public class HdfsWarcArtifactDataStore extends WarcArtifactDataStore {
  private final static Log log = LogFactory.getLog(HdfsWarcArtifactDataStore.class);
  public final static String DEFAULT_REPO_BASEDIR = "/";

  protected FileSystem fs;
  private boolean initialized = false;

  public HdfsWarcArtifactDataStore(Configuration config) throws IOException {
    this(config, DEFAULT_REPO_BASEDIR);
  }

  /**
   * Constructor that takes a Hadoop {@code Configuration} and base path.
   *
   * @param config   An Apache Hadoop {@code Configuration}.
   * @param basePath A {@code String} containing the base path of the LOCKSS repository under HDFS.
   */
  public HdfsWarcArtifactDataStore(Configuration config, String basePath) throws IOException {
    this(FileSystem.get(config), basePath);
  }

  /**
   * Constructor that takes a Hadoop {@code FileSystem} and uses the default repository base path.
   *
   * @param fs An Apache Hadoop {@code FileSystem}.
   * @throws IOException
   */
  public HdfsWarcArtifactDataStore(FileSystem fs) throws IOException {
    this(fs, DEFAULT_REPO_BASEDIR);
  }

  /**
   * Constructor that takes a Hadoop {@code FileSystem} and base path.
   *
   * @param fs       An Apache Hadoop {@code FileSystem}.
   * @param basePath A {@code String} containing the base path of the LOCKSS repository under HDFS.
   * @throws IOException
   */
  public HdfsWarcArtifactDataStore(FileSystem fs, String basePath) throws IOException {
    super(basePath);

    log.info(String.format(
        "Instantiating a HDFS artifact data store under %s%s",
        fs.getUri(),
        this.basePath
    ));

    this.fs = fs;
    this.fileAndOffsetStorageUrlPat =
        Pattern.compile("(" + fs.getUri() + ")(" + (getBasePath().equals("/") ? "" : getBasePath()) + ")([^?]+)\\?offset=(\\d+)");

    // Initialize the base path with a LOCKSS repository structure
    initRepository();
  }

  /**
   * Checks whether the HDFS cluster is available by getting its status.
   *
   * @return
   */
  private boolean checkAlive() {
    try {
      fs.getStatus();
      return true;
    } catch (IOException e) {
      log.warn(String.format("Could not get HDFS status: %s", e));
    }

    return false;
  }

  /**
   * Returns a boolean indicating whether this artifact store is ready.
   *
   * @return
   */
  @Override
  public boolean isReady() {
    initRepository();
    return initialized && checkAlive();
  }

  /**
   * Recursively finds artifact WARC files under a given base path.
   *
   * @param basePath The base path to scan recursively for WARC files.
   * @return A collection of paths to WARC files under the given base path.
   * @throws IOException
   */
  @Override
  public Collection<String> scanDirectories(String basePath) throws IOException {
    Collection<String> warcFiles = new ArrayList<>();

    RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(basePath), true);

    while (files.hasNext()) {
      // Get located file status and name
      LocatedFileStatus status = files.next();
      String fileName = status.getPath().getName();

      // Add this file to the list of WARC files found
      if (status.isFile() && fileName.toLowerCase().endsWith(WARC_FILE_EXTENSION)) {
        warcFiles.add(status.getPath().toString().substring((fs.getUri() + getBasePath()).length()));
      }
    }

    // Return WARC files
    return warcFiles;
  }

  /**
   * Ensures a directory exists at the given path by creating one if nothing exists there. Throws RunTimeExceptionError
   * if something exists at the path but is not a directory (there is no way to safely handle this situation).
   *
   * @param dirPath Path to the directory to create, if it doesn't exist yet.
   */
  public void mkdirs(String dirPath) throws IOException {
    Path fullPath = new Path(getBasePath() + dirPath);

    if (fs.isDirectory(fullPath)) {
      return;
    }

    if (fs.mkdirs(fullPath)) {
      log.debug(String.format("Created directory: %s", fullPath));
    } else {
      throw new IOException(String.format("Error creating directory: %s", fullPath));
    }
  }

  @Override
  public long getFileLength(String filePath) throws IOException {
    // Acquire lock to avoid returning a file length while writing an artifact
    Lock warcLock = warcLockMap.getLock(filePath);
    warcLock.lock();

    try {
      return fs.getFileStatus(new Path(getBasePath() + filePath)).getLen();
    } catch (FileNotFoundException e) {
      return 0L;
    } finally {
      warcLock.unlock();
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
    UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString(fs.getUri() + getBasePath() + filePath);
    uriBuilder.queryParams(params);
    return uriBuilder.toUriString();
  }

  /**
   * Initializes a new LOCKSS repository structure under the configured base path.
   *
   * @throws IOException
   */
  public synchronized void initRepository() {
    if (!initialized) {
      try {
        mkdirs("/");
        mkdirs(getSealedWarcPath());
        initialized = true;
      } catch (IOException e) {
        log.warn(String.format("Could not initialize HDFS artifact store: %s", e));
      }
    }
  }

  /**
   * Initializes a new AU collection under this LOCKSS repository.
   *
   * @param collectionId
   *          A {@code String} containing the collection ID.
   * @throws IOException
   */
  @Override
  public void initCollection(String collectionId) throws IOException {
    mkdirs(getCollectionPath(collectionId));
    mkdirs(getCollectionTmpPath(collectionId));
  }

  /**
   * Initializes an AU in the specified AU collection.
   *
   * @param collectionId
   *          A {@code String} containing the collection ID of this AU.
   * @param auid
   *          A {@code String} containing the AUID of this AU.
   * @throws IOException
   */
  @Override
  public void initAu(String collectionId, String auid) throws IOException {
    initCollection(collectionId);
    mkdirs(getAuPath(collectionId, auid));
  }

  /**
   * Initializes a new WARC file at the provided path.
   *
   * @param warcPath
   *          A {@code String} containing the path of the WARC file to be initialized.
   * @throws IOException
   */
  @Override
  public void initWarc(String warcPath) throws IOException {
    Path fullPath = new Path(getBasePath() + warcPath);

    if (fs.createNewFile(fullPath)) {
      log.info(String.format("Created new WARC file under HDFS: %s", fullPath));
    }
  }

  @Override
  public OutputStream getAppendableOutputStream(String filePath) throws IOException {
    Path extPath = new Path(getBasePath() + filePath);
    log.info(String.format("Opening %s for appendable OutputStream", extPath));
    return fs.append(extPath);
  }

  @Override
  public InputStream getInputStream(String filePath) throws IOException {
    return fs.open(new Path(getBasePath() + filePath));
  }

  @Override
  public InputStream getInputStreamAndSeek(String filePath, long seek) throws IOException {
    FSDataInputStream fsDataInputStream = fs.open(new Path(getBasePath() + filePath));
    fsDataInputStream.seek(seek);
    return fsDataInputStream;
  }

  @Override
  public InputStream getWarcRecordInputStream(String storageUrl) throws IOException {
    return getFileAndOffsetWarcRecordInputStream(storageUrl);
  }

  @Override
  public void moveWarc(String srcPath, String dstPath) throws IOException {
    if (!fs.rename(new Path(getBasePath() + srcPath), new Path(getBasePath() + dstPath))) {
      throw new IOException(String.format("Error renaming %s to %s", srcPath, dstPath));
    }
  }

  @Override
  public String makeNewStorageUrl(String newPath, Artifact artifact) {
    return makeNewFileAndOffsetStorageUrl(newPath, artifact);
  }

  /**
   * Creates a new WARC file, and begins it with a warcinfo WARC record.
   *
   * @param warcFilePath A {@code Path} to the new WARC file to create.
   * @throws IOException
   */
  public void createWarcFile(Path warcFilePath) throws IOException {
    if (!fs.exists(warcFilePath)) {
      // Create a new WARC file
      fs.createNewFile(warcFilePath);

      // TODO: Write a warcinfo WARC record

    } else {
      if (!fs.isFile(warcFilePath)) {
        log.warn(String.format("%s is not a file", warcFilePath));
      }
    }
  }
}
