/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.io.storage.warc.WarcFilePool;
import org.lockss.log.L4JLogger;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Apache Hadoop Distributed File System (HDFS) implementation of WarcArtifactDataStore.
 */
public class HdfsWarcArtifactDataStore extends WarcArtifactDataStore {
  private final static L4JLogger log = L4JLogger.getLogger();

  public final static long DEFAULT_BLOCKSIZE = FileUtils.ONE_MB * 128;

  protected FileSystem fs;
//  protected Path basePath;

  /**
   * Constructor that takes a Hadoop {@code Configuration}. Uses a default LOCKSS repository base path.
   *
   * @param config A Hadoop {@code Configuration} instance.
   * @throws IOException
   */
  public HdfsWarcArtifactDataStore(ArtifactIndex index, Configuration config) throws IOException {
    this(index, config, DEFAULT_BASEPATH);
  }

  /**
   * Constructor that takes a Hadoop {@code Configuration} and base path.
   *
   * @param config   An Apache Hadoop {@code Configuration}.
   * @param basePath A {@code String} containing the base path of the LOCKSS repository under HDFS.
   */
  public HdfsWarcArtifactDataStore(ArtifactIndex index, Configuration config, Path basePath) throws IOException {
    this(index, FileSystem.get(config), basePath);
  }

  /**
   * Constructor that takes a Hadoop {@code FileSystem} and uses the default repository base path.
   *
   * @param fs An Apache Hadoop {@code FileSystem}.
   * @throws IOException
   */
  public HdfsWarcArtifactDataStore(ArtifactIndex index, FileSystem fs) throws IOException {
    this(index, fs, DEFAULT_BASEPATH);
  }

  /**
   * Constructor that takes a Hadoop {@code FileSystem} and base path.
   *
   * @param fs       An Apache Hadoop {@code FileSystem}.
   * @param basePath A {@code String} containing the base path of the LOCKSS repository under HDFS.
   * @throws IOException
   */
  public HdfsWarcArtifactDataStore(ArtifactIndex index, FileSystem fs, Path basePath) throws IOException {
    super(index);

    log.info("Instantiating a HDFS artifact data store under {}{}", fs.getUri(), getBasePaths());

    this.fs = fs;
//    this.basePath = basePath;
    this.basePaths = new Path[]{basePath};
    Path[] tmpWarcBasePaths = getTmpWarcBasePaths();
    this.tmpWarcPool = new WarcFilePool(tmpWarcBasePaths);

    mkdirs(getBasePaths());
    mkdirs(getTmpWarcBasePaths());
  }

//  protected Path getBasePath() {
//    return this.basePath;
//  }

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
      log.warn("Could not get HDFS status: {}", e);
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
    return dataStoreState == DataStoreState.INITIALIZED && checkAlive();
  }

  /**
   * Recursively finds WARC files under a given base path.
   *
   * @param path A {@code String} containing the base path to scan recursively for WARC files.
   * @return A {@code Collection<String>} containing paths to WARC files under the base path.
   * @throws IOException
   */
  @Override
  public Collection<Path> findWarcs(Path path) throws IOException {
    Collection<Path> warcFiles = new ArrayList<>();

    org.apache.hadoop.fs.Path basePath = new org.apache.hadoop.fs.Path(path.toString());

    if (fs.exists(basePath) && fs.getFileStatus(basePath).isDirectory()) {
      RemoteIterator<LocatedFileStatus> files = fs.listFiles(basePath, true);

      while (files.hasNext()) {
        // Get located file status and name
        LocatedFileStatus status = files.next();
        String fileName = status.getPath().getName();

        // Add this file to the list of WARC files found
        if (status.isFile() && fileName.toLowerCase().endsWith(WARC_FILE_EXTENSION)) {
          warcFiles.add(Paths.get(status.getPath().toString().substring(fs.getUri().toString().length())));
        }
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
  public void mkdirs(Path dirPath) throws IOException {
    org.apache.hadoop.fs.Path fullPath = new org.apache.hadoop.fs.Path(dirPath.toString());

//    if (fs.getFileStatus(fullPath).isDirectory()) {
//      return;
//    }

    if (fs.mkdirs(fullPath)) {
      log.debug2("Created directory [fullPath: {}]", fullPath);
    } else {
      throw new IOException(String.format("Error creating directory: %s", fullPath));
    }
  }

  public void mkdirs(Path[] dirs) throws IOException {
    for (Path dirPath : dirs) {
      mkdirs(dirPath);
    }
  }

  @Override
  public long getWarcLength(Path warcPath) throws IOException {
    try {
      return fs.getFileStatus(new org.apache.hadoop.fs.Path(warcPath.toString())).getLen();
    } catch (FileNotFoundException e) {
      return 0L;
    }
  }

//  protected Path getTmpWarcBasePath() {
//    return basePath.resolve(TMP_WARCS_DIR);
//  }

  @Override
  protected long getBlockSize() {
    return DEFAULT_BLOCKSIZE;
  }

  @Override
  public String makeStorageUrl(Path filePath, MultiValueMap<String, String> params) {
    UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString(fs.getUri() + filePath.toString());
    uriBuilder.queryParams(params);
    return uriBuilder.toUriString();
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
    if (collectionId == null || collectionId.isEmpty()) {
      throw new IllegalArgumentException("Collection ID is null or empty");
    }

    mkdirs(getCollectionPath(getBasePaths()[0], collectionId));
    mkdirs(getCollectionPath(getBasePaths()[0], collectionId).resolve(TMP_WARCS_DIR));
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

    if (auid == null || auid.isEmpty()) {
      throw new IllegalArgumentException("AUID is null or empty");
    }

    mkdirs(getAuPath(getCollectionPath(getBasePaths()[0], collectionId), auid));
  }

  public Path getAuPath(Path collectionBase, String auid) {
    return collectionBase.resolve(AU_DIR_PREFIX + DigestUtils.md5Hex(auid));
  }

  public Path getCollectionPath(Path basePath, String collectionId) {
    return getCollectionsBase(basePath).resolve(collectionId);
  }

  public Path getCollectionsBase(Path basePath) {
    return basePath.resolve(COLLECTIONS_DIR);
  }

  /**
   * Initializes a new WARC file at the provided path.
   *
   * @param warcPath
   *          A {@code String} containing the path of the WARC file to be initialized.
   * @throws IOException
   */
  @Override
  public void initWarc(Path warcPath) throws IOException {
    org.apache.hadoop.fs.Path fullPath = new org.apache.hadoop.fs.Path(warcPath.toString());

    if (fs.createNewFile(fullPath)) {
      log.debug2("Created new WARC file under HDFS [fullPath: {}]", fullPath);
    }

    try (OutputStream output = getAppendableOutputStream(warcPath)) {
      writeWarcInfoRecord(output);
    }
  }

  @Override
  public OutputStream getAppendableOutputStream(Path filePath) throws IOException {
    log.debug2("Opening appendable OutputStream [filePath: {}]", filePath);

    org.apache.hadoop.fs.Path extPath = new org.apache.hadoop.fs.Path(filePath.toString());
    return fs.append(extPath);
  }

  @Override
  public InputStream getInputStreamAndSeek(Path filePath, long seek) throws IOException {
    log.debug2("filePath = {}", filePath);
    log.debug2("seek = {}", seek);

    FSDataInputStream fsDataInputStream = fs.open(new org.apache.hadoop.fs.Path(filePath.toString()));
    fsDataInputStream.seek(seek);
    return fsDataInputStream;
  }

  @Override
  public boolean removeWarc(Path path) throws IOException {
    return fs.delete(new org.apache.hadoop.fs.Path(path.toString()), false);
  }
}
