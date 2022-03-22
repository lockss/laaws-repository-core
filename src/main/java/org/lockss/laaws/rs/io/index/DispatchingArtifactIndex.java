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
import java.util.*;

import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.laaws.rs.model.*;
import org.lockss.log.L4JLogger;
import org.lockss.util.storage.StorageInfo;

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
  private ArtifactIndex tempIndex;
  private Set<String> bulkStoreAuids = new HashSet<>();
  private LockssRepository repository;

  public DispatchingArtifactIndex(ArtifactIndex master, ArtifactIndex temp) {
    this.masterIndex = master;
    this.tempIndex = temp;
  }

  /** Return true if this {collection,auid} is currently in the temp index */
  private boolean useTempIndex(String collection, String auid) {
    return bulkStoreAuids.contains(key(collection, auid));
  }

  /** Return true if the artifactId's {collection,auid} is declared to
   * be in the temp index */
  private boolean useTempIndex(ArtifactIdentifier artifactId) {
    return useTempIndex(artifactId.getCollection(), artifactId.getAuid());
  }

  /** Return true if the stem's {collection,auid} is declared to be in
   * the temp index */
  private boolean useTempIndex(ArtifactIdentifier.ArtifactStem stem) {
    return useTempIndex(stem.getCollection(), stem.getAuid());
  }

  /** Return true if the ArtifactData's {collection,auid} is declared
   * to be in the temp index */
  private boolean useTempIndex(ArtifactData ad) {
    ArtifactIdentifier id = ad.getIdentifier();
    return useTempIndex(id.getCollection(), id.getAuid());
  }

  /** Return true if the artifactId is found in the temp index.  (This
   * is a heuristic - checks whether the artifactId is known to the
   * temp index*/
  private boolean useTempIndex(String artifactId) throws IOException{
    return tempIndex.getArtifact(artifactId) != null;
  }

  /** Return true if the artifactId is found in the temp index.  (This
   * is a heuristic - checks whether the artifactId is known to the
   * temp index*/
  private boolean useTempIndex(UUID artifactId) throws IOException {
    return useTempIndex(artifactId.toString());
  }

  @Override
  public void init() {
    masterIndex.init();
    tempIndex.init();
  }

  @Override
  public void setLockssRepository(LockssRepository repository) {
    masterIndex.setLockssRepository(repository);
    tempIndex.setLockssRepository(repository);
  }

  @Override
  public void start() {
    masterIndex.start();
    tempIndex.start();
  }

  @Override
  public void stop() {
    masterIndex.stop();
    tempIndex.stop();
  }

  @Override
  public boolean isReady() {
    return masterIndex.isReady() && tempIndex.isReady();
  }

  @Override
  public void acquireVersionLock(ArtifactIdentifier.ArtifactStem stem)
      throws IOException {
    if (useTempIndex(stem)) {
      tempIndex.acquireVersionLock(stem);
    } else {
      masterIndex.acquireVersionLock(stem);
    }
  }

  @Override
  public void releaseVersionLock(ArtifactIdentifier.ArtifactStem stem) {
    if (useTempIndex(stem)) {
      tempIndex.releaseVersionLock(stem);
    } else {
      masterIndex.releaseVersionLock(stem);
    }
  }

  @Override
  public Artifact indexArtifact(ArtifactData artifactData) throws IOException {
    if (useTempIndex(artifactData)) {
      return tempIndex.indexArtifact(artifactData);
    } else {
      return masterIndex.indexArtifact(artifactData);
    }
  }

  @Override
  public Artifact getArtifact(String artifactId) throws IOException {
    if (useTempIndex(artifactId)) {
      return tempIndex.getArtifact(artifactId);
    } else {
      return masterIndex.getArtifact(artifactId);
    }
  }

  @Override
  public Artifact getArtifact(UUID artifactId) throws IOException {
    if (useTempIndex(artifactId)) {
      return tempIndex.getArtifact(artifactId);
    } else {
      return masterIndex.getArtifact(artifactId);
    }
  }

  @Override
  public Artifact commitArtifact(String artifactId) throws IOException {
    if (useTempIndex(artifactId)) {
      return tempIndex.commitArtifact(artifactId);
    } else {
      return masterIndex.commitArtifact(artifactId);
    }
  }

  @Override
  public Artifact commitArtifact(UUID artifactId) throws IOException {
    if (useTempIndex(artifactId)) {
      return tempIndex.commitArtifact(artifactId);
    } else {
      return masterIndex.commitArtifact(artifactId);
    }
  }

  @Override
  public boolean deleteArtifact(String artifactId) throws IOException {
    if (useTempIndex(artifactId)) {
      return tempIndex.deleteArtifact(artifactId);
    } else {
      return masterIndex.deleteArtifact(artifactId);
    }
  }

  @Override
  public boolean deleteArtifact(UUID artifactId) throws IOException {
    if (useTempIndex(artifactId)) {
      return tempIndex.deleteArtifact(artifactId);
    } else {
      return masterIndex.deleteArtifact(artifactId);
    }
  }

  @Override
  public boolean artifactExists(String artifactId) throws IOException {
    if (useTempIndex(artifactId)) {
      return tempIndex.artifactExists(artifactId);
    } else {
      return masterIndex.artifactExists(artifactId);
    }
  }

  @Override
  public Artifact updateStorageUrl(String artifactId, String storageUrl)
      throws IOException {
    if (useTempIndex(artifactId)) {
      return tempIndex.updateStorageUrl(artifactId, storageUrl);
    } else {
      return masterIndex.updateStorageUrl(artifactId, storageUrl);
    }
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
    if (useTempIndex(collection, auid)) {
      return tempIndex.getArtifacts(collection, auid, includeUncommitted);
    } else {
      return masterIndex.getArtifacts(collection, auid, includeUncommitted);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String collection,
                                                    String auid,
                                                    boolean includeUncommitted)
      throws IOException {
    if (useTempIndex(auid)) {
      return tempIndex.getArtifactsAllVersions(collection, auid,
                                               includeUncommitted);
    } else {
      return masterIndex.getArtifactsAllVersions(collection, auid,
                                                 includeUncommitted);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsWithPrefix(String collection,
                                                   String auid, String prefix)
      throws IOException {
    if (useTempIndex(auid)) {
      return tempIndex.getArtifactsWithPrefix(collection, auid, prefix);
    } else {
      return masterIndex.getArtifactsWithPrefix(collection, auid, prefix);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsWithPrefixAllVersions(String collection,
                                                              String auid,
                                                              String prefix)
      throws IOException {
    if (useTempIndex(collection, auid)) {
      return tempIndex.getArtifactsWithPrefixAllVersions(collection, auid,
                                                         prefix);
    } else {
      return masterIndex.getArtifactsWithPrefixAllVersions(collection, auid,
                                                           prefix);
    }
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
    if (useTempIndex(collection, auid)) {
      return tempIndex.getArtifactsAllVersions( collection, auid, url);
    } else {
      return masterIndex.getArtifactsAllVersions(collection, auid, url);
    }
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
    if (useTempIndex(collection, auid)) {
      return tempIndex.getArtifact(collection, auid, url, includeUncommitted);
    } else {
      return masterIndex.getArtifact(collection, auid, url, includeUncommitted);
    }
  }

  @Override
  public Artifact getArtifactVersion(String collection,
                                     String auid,
                                     String url,
                                     Integer version,
                                     boolean includeUncommitted)
      throws IOException {
    if (useTempIndex(collection, auid)) {
      return tempIndex.getArtifactVersion(collection, auid, url,
                                          version, includeUncommitted);
    } else {
      return masterIndex.getArtifactVersion(collection, auid, url,
                                            version, includeUncommitted);
    }
  }

  @Override
  public AuSize auSize(String collection, String auid) throws IOException {
    if (useTempIndex(collection, auid)) {
      return tempIndex.auSize(collection, auid);
    } else {
      return masterIndex.auSize(collection, auid);
    }
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
  public void setBulkStore(String collection, String auid) {
    bulkStoreAuids.add(key(collection, auid));
  }

  @Override
  public void finishBulkStore(String collection, String auid) {
    // copy Artifact to master indexy
    bulkStoreAuids.remove(key(collection, auid));

    try {
      Iterable<Artifact> artifacts = tempIndex.getArtifactsAllVersions(collection, auid, true);
      ((SolrArtifactIndex)masterIndex).indexArtifacts(artifacts);
    } catch (IOException e) {
      log.error("Failed to retrieve and bulk add artifacts", e);
    }
  }

  private String key(String collection, String auid) {
    return collection + auid;
  }

}
