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

package org.lockss.laaws.rs.io.storage.local;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.*;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.lockss.log.L4JLogger;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Local filesystem implementation of WarcArtifactDataStore.
 */
public class LocalWarcArtifactDataStore extends WarcArtifactDataStore {
  private final static L4JLogger log = L4JLogger.getLogger();
  private final static long DEFAULT_BLOCKSIZE = FileUtils.ONE_KB * 4;

  public LocalWarcArtifactDataStore(ArtifactIndex index, File basePath) throws IOException {
    this(index, basePath.getAbsolutePath());
  }

  /**
   * Constructor. Rebuilds the index on start-up from a given repository base path, if using a volatile index.
   *
   * @param basePath The base path of the local repository.
   */
  public LocalWarcArtifactDataStore(ArtifactIndex index, String basePath) throws IOException {
    super(index, basePath);

    log.info(String.format("Instantiating a local data store under %s", basePath));

    // Initialize LOCKSS repository structure
    mkdirs(getBasePath());
    mkdirs(getTmpWarcBasePath());
    mkdirs(getSealedWarcsPath());
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
  public synchronized void initDataStore() {
    // Reload temporary WARCs
    reloadDataStoreState();
    dataStoreState = DataStoreState.INITIALIZED;
  }

  @Override
  public void initCollection(String collectionId) throws IOException {
    mkdirs(getCollectionPath(collectionId));
    mkdirs(getCollectionTmpPath(collectionId));
  }

  @Override
  public void initAu(String collectionId, String auid) throws IOException {
    initCollection(collectionId);
    mkdirs(getAuPath(collectionId, auid));
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
    public Collection<String> findWarcs(String basePath) {
        Collection<String> warcFiles = new ArrayList<>();
        File basePathFile = new File(basePath);

        if (basePathFile.exists() && basePathFile.isDirectory()) {
          // DFS recursion through directories
          Arrays.stream(basePathFile.listFiles(x -> x.isDirectory()))
              .map(x -> findWarcs(x.getPath()))
              .forEach(warcFiles::addAll);

          // Add WARC files from this directory
          warcFiles.addAll(
              Arrays.asList(
                  basePathFile.listFiles(
                      x -> x.isFile() && x.getName().toLowerCase().endsWith(WARC_FILE_EXTENSION)
                  )
              ).stream().map(x -> x.getPath()).collect(Collectors.toSet()) // TODO: clean up
          );
        }

        // Return WARC files at this level
        return warcFiles;
    }

    public void mkdirs(String dirPath) throws IOException {
        File dir = new File(dirPath);

        if (dir.isDirectory()) {
            return;
        }

        if (!dir.mkdirs()) {
            throw new IOException(String.format("Error creating %s: mkdirs did not succeed", dir.getAbsolutePath()));
        }
    }

    @Override
    public long getWarcLength(String warcPath) {
      return new File(warcPath).length();
    }

    @Override
    public String makeStorageUrl(String filePath, MultiValueMap<String, String> params) {
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("file://" + filePath);
        uriBuilder.queryParams(params);
        return uriBuilder.toUriString();
    }

    @Override
    public OutputStream getAppendableOutputStream(String filePath) throws IOException {
        return new FileOutputStream(filePath, true);
    }

    @Override
    public InputStream getInputStreamAndSeek(String filePath, long seek) throws IOException {
        log.info("filePath = {}", filePath);
        log.info("seek = {}", seek);

        InputStream inputStream = new FileInputStream(filePath);
        inputStream.skip(seek);
        return inputStream;
    }

    @Override
    public void initWarc(String storageUrl) throws IOException {
        File file = new File(storageUrl);
        if (!file.exists()) {
            mkdirs(new File(storageUrl).getParent());
            FileUtils.touch(file);
        }
    }

    @Override
    public boolean removeWarc(String path) {
      return new File(path).delete();
    }

    /**
     * Returns a boolean indicating whether an artifact is marked as deleted in the repository storage.
     *
     * @param indexData The artifact identifier of the aritfact to check.
     * @return A boolean indicating whether the artifact is marked as deleted.
     * @throws IOException
     * @throws URISyntaxException
     */
    public boolean isDeleted(Artifact indexData)
            throws IOException, URISyntaxException {
        ArtifactData artifact = getArtifactData(indexData);
        RepositoryArtifactMetadata metadata = artifact.getRepositoryMetadata();
        return metadata.isDeleted();
    }

    /**
     * Returns a boolean indicating whether an artifact is marked as committed in the repository storage.
     *
     * @param indexData The artifact identifier of the artifact to check.
     * @return A boolean indicating whether the artifact is marked as committed.
     * @throws IOException
     * @throws URISyntaxException
     */
    // TODO this isn't used by anything?
    public boolean isCommitted(Artifact indexData)
            throws IOException, URISyntaxException {
        ArtifactData artifact = getArtifactData(indexData);
        RepositoryArtifactMetadata metadata = artifact.getRepositoryMetadata();
        return metadata.isCommitted();
    }

    @Override
    protected String getAbsolutePath(String path) {
      File pathDir = new File(getBasePath(), path);
      return pathDir.toString();
    }

}