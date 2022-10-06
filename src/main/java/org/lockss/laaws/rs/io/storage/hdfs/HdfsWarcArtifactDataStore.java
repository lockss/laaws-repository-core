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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.archive.format.warc.WARCConstants;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.io.storage.warc.WarcFilePool;
import org.lockss.laaws.rs.model.NamespacedAuid;
import org.lockss.log.L4JLogger;
import org.lockss.util.storage.StorageInfo;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Apache Hadoop Distributed File System (HDFS) implementation of {@link WarcArtifactDataStore}.
 */
public class HdfsWarcArtifactDataStore extends WarcArtifactDataStore {
  private final static L4JLogger log = L4JLogger.getLogger();

  /** Label to describe type of HdfsWarcArtifactDataStore */
  public static String ARTIFACT_DATASTORE_TYPE = "Hdfs";

  public final static long DEFAULT_BLOCKSIZE = FileUtils.ONE_MB * 128;

  protected FileSystem fs;

  // *******************************************************************************************************************
  // * CONSTRUCTORS
  // *******************************************************************************************************************

  /**
   * Constructor that takes a Hadoop {@code Configuration}. Uses a default LOCKSS repository base path.
   *
   * @param config A Hadoop {@code Configuration} instance.
   * @throws IOException
   */
  public HdfsWarcArtifactDataStore(Configuration config) throws IOException {
    this(config, DEFAULT_BASEPATH);
  }

  /**
   * Constructor that takes a Hadoop {@code Configuration} and base path.
   *
   * @param config   An Apache Hadoop {@code Configuration}.
   * @param basePath A {@code String} containing the base path of the LOCKSS repository under HDFS.
   */
  public HdfsWarcArtifactDataStore(Configuration config, Path basePath) throws IOException {
    this(FileSystem.get(config), basePath);
  }

  /**
   * Constructor that takes a Hadoop {@code FileSystem} and uses the default repository base path.
   *
   * @param fs An Apache Hadoop {@code FileSystem}.
   * @throws IOException
   */
  public HdfsWarcArtifactDataStore(FileSystem fs) throws IOException {
    this(fs, DEFAULT_BASEPATH);
  }

  /**
   * Constructor that takes a Hadoop {@code FileSystem} and base path.
   *
   * @param fs       An Apache Hadoop {@code FileSystem}.
   * @param basePath A {@code String} containing the base path of the LOCKSS repository under HDFS.
   * @throws IOException
   */
  public HdfsWarcArtifactDataStore(FileSystem fs, Path basePath) throws IOException {
    log.info("Instantiating a HDFS artifact data store under {}", fs.getUri());

    this.fs = fs;
    this.basePaths = new Path[]{basePath};
    this.tmpWarcPool = new WarcFilePool(this);

    mkdirs(getBasePaths());
    mkdirs(getTmpWarcBasePaths());
  }

  // *******************************************************************************************************************
  // * IMPLEMENTATION UTILITY METHODS
  // *******************************************************************************************************************

  /**
   * Ensures a directory exists at the given path by creating one if nothing exists there. Throws RunTimeExceptionError
   * if something exists at the path but is not a directory (there is no way to safely handle this situation).
   *
   * @param dirPath Path to the directory to create, if it doesn't exist yet.
   */
  public void mkdirs(Path dirPath) throws IOException {
    org.apache.hadoop.fs.Path fullPath = new org.apache.hadoop.fs.Path(dirPath.toString());

    if (fs.mkdirs(fullPath)) {
      log.debug2("Created directory [fullPath: {}]", fullPath);
      return;
    }

    throw new IOException(String.format("Error creating directory: %s", fullPath));
  }

  public void mkdirs(Path[] dirs) throws IOException {
    for (Path dirPath : dirs) {
      mkdirs(dirPath);
    }
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
      log.warn("Could not get HDFS status: {}", e);
    }

    return false;
  }

  /**
   * Only used to enable testing!
   *
   */
  // FIXME
  protected void clearAuMaps() {
    log.debug("Cleared internal AU maps");

    // Reset maps
    auPathsMap = new HashMap<>();
    auActiveWarcsMap = new HashMap<>();
  }

  // *******************************************************************************************************************
  // * ABSTRACT METHOD IMPLEMENTATION
  // *******************************************************************************************************************

  /**
   * Returns a boolean indicating whether this artifact store is ready.
   *
   * @return
   */
  @Override
  public boolean isReady() {
    return dataStoreState != DataStoreState.STOPPED && checkAlive();
  }

  /**
   * Recursively finds WARC files under a given base path.
   *
   * @param basePath A {@link String} containing the base path to scan recursively for WARC files.
   * @return A {@link Collection<String>} containing paths to WARC files under the base path.
   * @throws IOException
   */
  @Override
  public Collection<Path> findWarcs(Path basePath) throws IOException {
    Collection<Path> warcFiles = new ArrayList<>();

    org.apache.hadoop.fs.Path fsBasePath = new org.apache.hadoop.fs.Path(basePath.toString());

    boolean fsBasePathExists = fs.exists(fsBasePath);
    boolean fsBasePathIsDir = fs.getFileStatus(fsBasePath).isDirectory();

    if (fsBasePathExists && fsBasePathIsDir) {
      // Recursively build a list of all files under this path
      RemoteIterator<LocatedFileStatus> files = fs.listFiles(fsBasePath, true);

      while (files.hasNext()) {
        // Get located file status and name
        LocatedFileStatus status = files.next();
        String fileName = status.getPath().getName();

        // Add file to set of WARC files if it is a WARC file
        if (status.isFile() &&
            (fileName.toLowerCase().endsWith(WARCConstants.DOT_WARC_FILE_EXTENSION) ||
                fileName.toLowerCase().endsWith(WARCConstants.DOT_COMPRESSED_WARC_FILE_EXTENSION))) {
          warcFiles.add(Paths.get(status.getPath().toUri().getPath())); // what?
        }
      }
    } else if (fsBasePathExists && !fsBasePathIsDir) {
      log.error("Base path is not a directory! [basePath: {}]", basePath);
      throw new IllegalStateException("Base path is not a directory!");
    }

    // Return WARC files
    return warcFiles;
  }

  @Override
  public long getWarcLength(Path warcPath) throws IOException {
    try {
      return fs.getFileStatus(new org.apache.hadoop.fs.Path(warcPath.toString())).getLen();
    } catch (FileNotFoundException e) {
      return 0L;
    }
  }

  @Override
  protected long getBlockSize() {
    return DEFAULT_BLOCKSIZE;
  }

  @Override
  protected long getFreeSpace(Path fsPath) {
    try {
      return fs.getStatus(new org.apache.hadoop.fs.Path(fsPath.toString())).getRemaining();
    } catch (IOException e) {
      // XXX Should we rethrow IOException?
      return 0L;
    }
  }

  @Override
  public URI makeStorageUrl(Path filePath, MultiValueMap<String, String> params) {
    UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUri(fs.getUri().resolve(filePath.toString()).normalize());
    uriBuilder.queryParams(params);
    return uriBuilder.build().toUri();
  }

  /**
   * Initializes a new namespace under this LOCKSS repository.
   *
   * @param namespace A {@code String} containing the namespace.
   * @throws IOException
   */
  @Override
  public void initNamespace(String namespace) throws IOException {
    if (namespace == null || namespace.isEmpty()) {
      throw new IllegalArgumentException("Namespace is null or empty");
    }

    mkdirs(getNamespacePaths(namespace));
  }

  /**
   * Initializes an AU in the specified namespace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @param auid         A {@code String} containing the AUID of this AU.
   * @throws IOException
   */
  @Override
  public List<Path> initAu(String namespace, String auid) throws IOException {
    //// Initialize the namespace on each filesystem
    initNamespace(namespace);

    //// Reload any existing AU base paths

    // Get base paths of the repository
    Path[] baseDirs = getBasePaths();

    if (baseDirs == null || baseDirs.length < 1) {
      throw new IllegalStateException("Null or empty baseDirs");
    }

    // Find existing base directories of this AU
    List<Path> auPathsFound = Arrays.stream(baseDirs)
        .map(basePath -> getAuPath(basePath, namespace, auid))
        .filter(auPath -> {
          org.apache.hadoop.fs.Path hdfsAuPath = new org.apache.hadoop.fs.Path(auPath.toString());
          try {
            FileStatus status = fs.getFileStatus(hdfsAuPath);
            return status.isDirectory();
          } catch (IOException e) {
            // Cannot determine whether AU path exists
            log.warn("Cannot determine whether AU path exists [hdfsAuPath: {}]", hdfsAuPath, e);
            return false;
          }
        })
        .collect(Collectors.toList());

    if (auPathsFound.isEmpty()) {
      // No existing directories for this AU: Initialize a new AU directory
      auPathsFound.add(initAuDir(namespace, auid));
    }

    // Track AU directories in internal AU paths map
    NamespacedAuid key = new NamespacedAuid(namespace, auid);
    auPathsMap.put(key, auPathsFound);

    return auPathsFound;
  }

  /**
   * Creates a new AU directory in HDFS.
   *
   * @param namespace A {@link String} containing the namespace.
   * @param auid A {@link String} containing the AUID.
   * @return A {@link Path} to a directory of this AU.
   * @throws IOException
   */
  @Override
  protected Path initAuDir(String namespace, String auid) throws IOException {
    Path[] basePaths = getBasePaths();

    if (basePaths == null || basePaths.length < 1) {
      throw new IllegalStateException("Data store is misconfigured");
    }

    // Determine which base path to use based on current available space
    Path basePath = Arrays.stream(basePaths)
        .sorted((a, b) -> (int) (getFreeSpace(b.getParent()) - getFreeSpace(a.getParent())))
        .findFirst()
        .get();

    // Generate an AU path under this base path and create it on disk
    Path auPath = getAuPath(basePath, namespace, auid);

    // Get FileStatus of AU path in HDFS
    org.apache.hadoop.fs.Path hdfsAuPath = hdfsPathFromPath(auPath);

    // Create the AU directory if necessary
    if (!fs.exists(hdfsAuPath) || !fs.getFileStatus(hdfsAuPath).isDirectory()) {
      mkdirs(auPath);
    }

    return auPath;
  }

  protected org.apache.hadoop.fs.Path hdfsPathFromPath(Path path) {
    return new org.apache.hadoop.fs.Path(path.toString());
  }

  /**
   * Initializes a new WARC file at the provided path.
   *
   * @param warcPath A {@code String} containing the path of the WARC file to be initialized.
   * @throws IOException
   */
  @Override
  public void initWarc(Path warcPath) throws IOException {
    initFile(warcPath);

    try (OutputStream output = getAppendableOutputStream(warcPath)) {
      writeWarcInfoRecord(output);
    }
  }

  protected void initFile(Path filePath) throws IOException {
    org.apache.hadoop.fs.Path fullPath =
        new org.apache.hadoop.fs.Path(filePath.toString());

    if (fs.createNewFile(fullPath)) {
      log.debug2("Created new WARC file under HDFS [fullPath: {}]", fullPath);
    }
  }

  @Override
  public OutputStream getAppendableOutputStream(Path filePath) throws IOException {
    log.debug2("Opening appendable OutputStream [filePath: {}]", filePath);

    org.apache.hadoop.fs.Path fsPath = new org.apache.hadoop.fs.Path(filePath.toString());
    return fs.append(fsPath);
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

  @Override
  public StorageInfo getStorageInfo() {
    try {
      // Build a StorageInfo
      StorageInfo sum = new StorageInfo(ARTIFACT_DATASTORE_TYPE);
      List<URI> uris = new ArrayList<>();

      // Compute sum of DFs
      for (Path basePath : getBasePaths()) {
        FsStatus status = fs.getStatus(new org.apache.hadoop.fs.Path(basePath.toString()));

        uris.add(fs.getUri().resolve(basePath.toUri()));
        sum.setSizeKB(sum.getSizeKB() + StorageInfo.toKBRounded(status.getCapacity()));
        sum.setUsedKB(sum.getUsedKB() + StorageInfo.toKBRounded(status.getUsed()));
        sum.setAvailKB(sum.getAvailKB() + StorageInfo.toKBRounded(status.getRemaining()));
      }

      // Set one-time StorageInfo fields
      sum.setName(fs.getUri().toString());
//      sum.setName(String.join(",", uris));
      sum.setPercentUsed((double)sum.getUsedKB() / (double)sum.getSizeKB());
      sum.setPercentUsedString(String.valueOf(100 * Math.round(sum.getPercentUsed())) + "%");

      // Return the sum
      return sum;

    } catch (IOException e) {
      throw new UnsupportedOperationException("Can't get WarcArtifactDataStore info", e);
    }
  }
}
