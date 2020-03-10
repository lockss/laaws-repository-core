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

package org.lockss.laaws.rs.io.storage.local;

import org.apache.commons.io.FileUtils;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.io.storage.warc.WarcFilePool;
import org.lockss.log.L4JLogger;
import org.lockss.util.io.FileUtil;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Local filesystem implementation of WarcArtifactDataStore.
 */
public class LocalWarcArtifactDataStore extends WarcArtifactDataStore {
  private final static L4JLogger log = L4JLogger.getLogger();

  private final static long DEFAULT_BLOCKSIZE = FileUtils.ONE_KB * 4;

  // *******************************************************************************************************************
  // * CONSTRUCTORS
  // *******************************************************************************************************************

  public LocalWarcArtifactDataStore(ArtifactIndex index, File[] basePath) throws IOException {
    this(index, Arrays.stream(basePath).map(File::toPath).toArray(Path[]::new));
  }

  /**
   * Constructor. Rebuilds the index on start-up from a given repository base path, if using a volatile index.
   */
  public LocalWarcArtifactDataStore(ArtifactIndex index, Path[] basePaths) throws IOException {
    super(index);

    log.debug2("Starting local WARC artifact data store [basePaths: {}]", basePaths);

    // Set local base paths
    this.basePaths = basePaths;

    // Start temporary WARC file pool
    this.tmpWarcPool = new WarcFilePool(this);

    // Initialize LOCKSS repository structure under base paths
    for (Path basePath : basePaths) {
      mkdirs(basePath);
      mkdirs(getTmpWarcBasePaths());
    }
  }

  // *******************************************************************************************************************
  // * UTILITY METHODS
  // *******************************************************************************************************************

  public void mkdirs(Path dirPath) throws IOException {
    log.trace("dirPath = {}", dirPath);
    if (!FileUtil.ensureDirExists(dirPath.toFile())) {
      throw new IOException(String.format("Could not create directory [dirPath: %s]", dirPath));
    }
  }

  public void mkdirs(Path[] dirs) throws IOException {
    for (Path dirPath : dirs) {
      mkdirs(dirPath);
    }
  }

  // *******************************************************************************************************************
  // * ABSTRACT METHOD IMPLEMENTATION
  // *******************************************************************************************************************

  @Override
  protected long getBlockSize() {
    return DEFAULT_BLOCKSIZE;
  }

  @Override
  protected long getFreeSpace(Path fsPath) {
    return fsPath.toFile().getFreeSpace();
  }

  @Override
  public void initCollection(String collectionId) throws IOException {
    mkdirs(getCollectionPaths(collectionId));
    mkdirs(getCollectionTmpWarcsPaths(collectionId));
  }

  @Override
  public void initAu(String collectionId, String auid) throws IOException {
    // Initialize collection on each filesystem
    initCollection(collectionId);

    // Iterate over AU's paths on each filesystem and create AU directory structure
    for (Path auBasePath : getAuPaths(collectionId, auid)) {
      mkdirs(auBasePath);
      mkdirs(auBasePath.resolve("artifacts"));
      mkdirs(auBasePath.resolve("journals"));
    }
  }

  /**
   * Returns a boolean indicating whether this artifact store is ready.
   *
   * @return
   */
  @Override
  public boolean isReady() {
    return dataStoreState == DataStoreState.INITIALIZED;
  }

  /**
   * Recursively finds artifact WARC files under a given base path.
   *
   * @param basePath The base path to scan recursively for WARC files.
   * @return A collection of paths to WARC files under the given base path.
   */
  @Override
  public Collection<Path> findWarcs(Path basePath) throws IOException {
    log.trace("basePath = {}", basePath);

    File basePathFile = basePath.toFile();

    if (basePathFile.exists() && basePathFile.isDirectory()) {

      File[] dirObjs = basePathFile.listFiles();

      if (dirObjs == null) {
        // File#listFiles() can return null if the path doesn't exist or if there was an I/O error; we checked that
        // the path exists and is a directory earlier so it must be the former
        log.error("Unable to list directory contents [basePath = {}]", basePath);
        throw new IOException(String.format("Unable to list directory contents [basePath = %s]", basePath));
      }

      Collection<Path> warcFiles = new ArrayList<>();

      // Recursively look for WARCs
      // FIXME: Potential stack overflow here with sufficiently deep tree
      // Arrays.stream(dirObjs).map(x -> findWarcs(x.toPath())).forEach(warcFiles::addAll);
      for (File dir : Arrays.stream(dirObjs).filter(File::isDirectory).toArray(File[]::new)) {
        warcFiles.addAll(findWarcs(dir.toPath()));
      }

      // Add WARC files from this directory
      warcFiles.addAll(
          Arrays.stream(dirObjs)
              .filter(x -> x.isFile() && x.getName().toLowerCase().endsWith(WARC_FILE_EXTENSION))
              .map(x -> x.toPath())
              .collect(Collectors.toSet())
      );

      // Return WARC files at this level
      return warcFiles;
    }

    log.warn("Path doesn't exist or was not a directory [basePath = {}]", basePath);

    return Collections.EMPTY_SET;
  }

  @Override
  public long getWarcLength(Path warcPath) {
    return warcPath.toFile().length();
  }

  @Override
  public URI makeStorageUrl(Path filePath, MultiValueMap<String, String> params) {
    UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("file://" + filePath);
    uriBuilder.queryParams(params);
    return uriBuilder.build().toUri();
  }

  @Override
  public OutputStream getAppendableOutputStream(Path filePath) throws IOException {
    return new FileOutputStream(filePath.toFile(), true);
  }

  @Override
  public InputStream getInputStreamAndSeek(Path filePath, long seek) throws IOException {
    log.trace("filePath = {}", filePath);
    log.trace("seek = {}", seek);

    InputStream inputStream = new FileInputStream(filePath.toFile());
    inputStream.skip(seek);

    return inputStream;
  }

  @Override
  public void initWarc(Path warcPath) throws IOException {
    File warcFile = warcPath.toFile();

    if (!warcFile.exists()) {
      mkdirs(warcPath.getParent());
      FileUtils.touch(warcFile);
    }

    try (OutputStream output = getAppendableOutputStream(warcPath)) {
      writeWarcInfoRecord(output);
    }
  }

  @Override
  public boolean removeWarc(Path filePath) {
    return filePath.toFile().delete();
  }
}
