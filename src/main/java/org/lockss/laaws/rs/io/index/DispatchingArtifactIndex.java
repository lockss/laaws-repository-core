/*
 * Copyright (c) 2017-2022, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.io.index;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;

import org.lockss.laaws.rs.core.BaseLockssRepository;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.lockss.log.L4JLogger;
import org.lockss.util.storage.StorageInfo;
import org.lockss.util.concurrent.CopyOnWriteMap;

/**
 * ArtifactIndex that dispatches all operations to either a permanent
 * SolrArtifactIndex, or a transient VolatileArtifactIndex.  Allows
 * substantially faster bulk artifact storage by firat storing the
 * Artifacts in a VolatileArtifactIndex, then transferring them into
 * the SolrArtifactIndex in a batch.
 */
public class DispatchingArtifactIndex implements ArtifactIndex {
  private final static L4JLogger log = L4JLogger.getLogger();

  private ArtifactIndex masterIndex;
  private Map<String,ArtifactIndex> tempIndexMap = new CopyOnWriteMap<>();
  private BaseLockssRepository repository;

  public DispatchingArtifactIndex(ArtifactIndex master) {
    this.masterIndex = master;
  }

  /** Return true if this {collection,auid} is currently in the temp index */
  private ArtifactIndex findIndexHolding(String collection, String auid) {
    ArtifactIndex res = tempIndexMap.get(key(collection, auid));
    return res != null ? res : masterIndex;
  }

  /** Return true if the artifactId's {collection,auid} is declared to
   * be in the temp index */
  private ArtifactIndex findIndexHolding(ArtifactIdentifier artifactId) {
    return findIndexHolding(artifactId.getNamespace(), artifactId.getAuid());
  }

  /** Return true if the stem's {collection,auid} is declared to be in
   * the temp index */
  private ArtifactIndex findIndexHolding(ArtifactIdentifier.ArtifactStem stem) {
    return findIndexHolding(stem.getNamespace(), stem.getAuid());
  }

  /** Return true if the ArtifactData's {collection,auid} is declared
   * to be in the temp index */
  private ArtifactIndex findIndexHolding(ArtifactData ad) {
    ArtifactIdentifier id = ad.getIdentifier();
    return findIndexHolding(id.getNamespace(), id.getAuid());
  }

  /** Return the temp index in which the artifactId is found, or the
   * masterIndex */
  private ArtifactIndex findIndexHolding(String artifactId) throws IOException{
    for (ArtifactIndex ind : tempIndexMap.values()) {
      if (ind.getArtifact(artifactId) != null) {
        return ind;
      }
    }
    return masterIndex;
  }

  /** Return true if the artifactId is found in the temp index.  (This
   * is a heuristic - checks whether the artifactId is known to the
   * temp index*/
  private ArtifactIndex findIndexHolding(UUID artifactId) throws IOException {
    return findIndexHolding(artifactId.toString());
  }

  @Override
  public void init() {
    masterIndex.init();
  }

  @Override
  public void setLockssRepository(BaseLockssRepository repository) {
    this.repository = repository;
    masterIndex.setLockssRepository(repository);
    for (ArtifactIndex ind : tempIndexMap.values()) {
      ind.setLockssRepository(repository);
    }
  }

  @Override
  public void start() {
    masterIndex.start();
  }

  @Override
  public void stop() {
    masterIndex.stop();
  }

  @Override
  public boolean isReady() {
    return masterIndex.isReady();
  }

  @Override
  public void acquireVersionLock(ArtifactIdentifier.ArtifactStem stem)
      throws IOException {
    findIndexHolding(stem).acquireVersionLock(stem);
  }

  @Override
  public void releaseVersionLock(ArtifactIdentifier.ArtifactStem stem) {
    findIndexHolding(stem).releaseVersionLock(stem);
  }

  @Override
  public Artifact indexArtifact(ArtifactData artifactData) throws IOException {
    return findIndexHolding(artifactData).indexArtifact(artifactData);
  }

  @Override
  public Artifact getArtifact(String artifactId) throws IOException {
    return findIndexHolding(artifactId).getArtifact(artifactId);
  }

  @Override
  public Artifact getArtifact(UUID artifactId) throws IOException {
    return findIndexHolding(artifactId).getArtifact(artifactId);
  }

  @Override
  public Artifact commitArtifact(String artifactId) throws IOException {
    return findIndexHolding(artifactId).commitArtifact(artifactId);
  }

  @Override
  public Artifact commitArtifact(UUID artifactId) throws IOException {
    return findIndexHolding(artifactId).commitArtifact(artifactId);
  }

  @Override
  public boolean deleteArtifact(String artifactId) throws IOException {
    return findIndexHolding(artifactId).deleteArtifact(artifactId);
  }

  @Override
  public boolean deleteArtifact(UUID artifactId) throws IOException {
    return findIndexHolding(artifactId).deleteArtifact(artifactId);
  }

  @Override
  public boolean artifactExists(String artifactId) throws IOException {
    return findIndexHolding(artifactId).artifactExists(artifactId);
  }

  @Override
  public Artifact updateStorageUrl(String artifactId, String storageUrl)
      throws IOException {
    return findIndexHolding(artifactId).updateStorageUrl(artifactId, storageUrl);
  }

  @Override
  public Iterable<String> getCollectionIds() throws IOException {
    return masterIndex.getCollectionIds();
  }

  @Override
  public Iterable<String> getAuIds(String collection) throws IOException {
    return masterIndex.getAuIds(collection);
  }

  @Override
  public Iterable<Artifact> getArtifacts(String collection, String auid,
                                         boolean includeUncommitted)
      throws IOException {
    return findIndexHolding(collection, auid).getArtifacts(collection, auid, includeUncommitted);
  }

  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String collection,
                                                    String auid,
                                                    boolean includeUncommitted)
      throws IOException {
    return findIndexHolding(collection, auid).getArtifactsAllVersions(collection, auid,
                                                                      includeUncommitted);
  }

  @Override
  public Iterable<Artifact> getArtifactsWithPrefix(String collection,
                                                   String auid, String prefix)
      throws IOException {
    return findIndexHolding(collection, auid).getArtifactsWithPrefix(collection, auid, prefix);
  }

  @Override
  public Iterable<Artifact> getArtifactsWithPrefixAllVersions(String collection,
                                                              String auid,
                                                              String prefix)
      throws IOException {
    return findIndexHolding(collection, auid).getArtifactsWithPrefixAllVersions(collection, auid,
                                                                                prefix);
  }

  @Override
  public Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String collection,
                                                                String prefix,
                                                                ArtifactVersions versions)
      throws IOException {
    return masterIndex.getArtifactsWithUrlPrefixFromAllAus(collection,
                                                           prefix,
                                                           versions);
  }

  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String collection,
                                                    String auid,
                                                    String url)
      throws IOException {
    return findIndexHolding(collection, auid).getArtifactsAllVersions(collection, auid, url);
  }

  @Override
  public Iterable<Artifact> getArtifactsWithUrlFromAllAus(String collection,
                                                          String url,
                                                          ArtifactVersions versions)
      throws IOException {
    return masterIndex.getArtifactsWithUrlFromAllAus(collection, url, versions);
  }

  @Override
  public Artifact getArtifact(String collection, String auid,
                              String url, boolean includeUncommitted)
      throws IOException {
    return findIndexHolding(collection, auid).getArtifact(collection, auid, url, includeUncommitted);
  }

  @Override
  public Artifact getArtifactVersion(String collection,
                                     String auid,
                                     String url,
                                     Integer version,
                                     boolean includeUncommitted)
      throws IOException {
    return findIndexHolding(collection, auid).getArtifactVersion(collection, auid, url,
                                                                 version, includeUncommitted);
  }

  @Override
  public AuSize auSize(String collection, String auid) throws IOException {
    return findIndexHolding(collection, auid).auSize(collection, auid);
  }

  /**
   * Returns information about the storage size and free space
   * @return A {@code StorageInfo}
   */
  @Override
  public StorageInfo getStorageInfo() {
    return masterIndex.getStorageInfo();
  }

  @Override
  public void startBulkStore(String collection, String auid) {
    VolatileArtifactIndex volInd = new VolatileArtifactIndex();
    volInd.init();
    volInd.start();
    volInd.setLockssRepository(repository);
    tempIndexMap.put(key(collection, auid), volInd);
  }

  @Override
  public void finishBulkStore(String collection, String auid,
                              int copyBatchSize) throws IOException {
    // Wait for all background commits for this AU to finish.  (They
    // call updateStorageUrl(), but index reads or writes are not
    // permitted and likely won't work correctly while the Artifacts
    // are being copied into Solr
    ArtifactDataStore store = repository.getArtifactDataStore();
    if (store instanceof WarcArtifactDataStore) {
      WarcArtifactDataStore warcStore = (WarcArtifactDataStore)store;
      if (!warcStore.waitForCommitTasks(collection, auid)) {
        log.warn("waitForCommitTasks() was interrupted");
        throw new InterruptedIOException("finishBulk interrupted");
      }
    }
    // copy Artifacts to master index
    ArtifactIndex volInd;
    volInd = tempIndexMap.remove(key(collection, auid));
    if (volInd == null) {
      throw new IllegalStateException("Attempt to finishBulkStore of AU not in bulk store mode: " + collection + ", " + auid);
    }
    volInd.stop();
    try {
      Iterable<Artifact> artifacts = volInd.getArtifactsAllVersions(collection, auid, true);
      ((SolrArtifactIndex)masterIndex).indexArtifacts(artifacts, copyBatchSize);
    } catch (IOException e) {
      log.error("Failed to retrieve and bulk add artifacts", e);
      throw e;
    }
  }

  private String key(String collection, String auid) {
    return collection + auid;
  }

}
