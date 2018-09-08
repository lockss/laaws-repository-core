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

package org.lockss.laaws.rs.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lockss.laaws.rs.io.AuidLockMap;
import org.lockss.laaws.rs.io.RepoAuid;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.VolatileArtifactIndex;
import org.lockss.laaws.rs.io.storage.*;
import org.lockss.laaws.rs.io.storage.warc.VolatileWarcArtifactDataStore;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * Base implementation of the LOCKSS Repository service.
 */
public class BaseLockssRepository implements LockssRepository {
  private final static Log log = LogFactory.getLog(BaseLockssRepository.class);

  protected ArtifactDataStore<ArtifactIdentifier, ArtifactData, RepositoryArtifactMetadata> store;
  protected ArtifactIndex index;
  protected AuidLockMap auidLockMap;

  /**
   * Constructor. By default, we spin up a volatile in-memory LOCKSS repository.
   */
  public BaseLockssRepository() {
    this(new VolatileArtifactIndex(), new VolatileWarcArtifactDataStore());
  }

  /**
   * Configures this LOCKSS repository with the provided artifact index and storage layers.
   *
   * @param index An instance of {@code ArtifactIndex}.
   * @param store An instance of {@code ArtifactDataStore}.
   */
  public BaseLockssRepository(ArtifactIndex index, ArtifactDataStore<ArtifactIdentifier, ArtifactData, RepositoryArtifactMetadata> store) {
    if ((index == null) || (store == null)) {
      throw new IllegalArgumentException("Cannot start repository with a null artifact index or store");
    }

    this.index = index;
    this.store = store;
    this.store.setArtifactIndex(this.index);
    this.auidLockMap = new AuidLockMap();
  }

  /**
   * Adds an artifact to this LOCKSS repository.
   *
   * @param artifactData {@code ArtifactData} instance to add to this LOCKSS repository.
   * @return The artifact ID of the newly added artifact.
   * @throws IOException
   */
  @Override
  public Artifact addArtifact(ArtifactData artifactData) throws IOException {
    if (artifactData == null)
      throw new IllegalArgumentException("Null ArtifactData");

    ArtifactIdentifier artifactId = artifactData.getIdentifier();

    Lock auidLock = auidLockMap.getLock(new RepoAuid(artifactId.getCollection(), artifactId.getAuid()));
    auidLock.lock();

    try {
      //// Determine the next artifact version

      // Retrieve latest version in this URL lineage
      Artifact latestVersion = index.getArtifact(
          artifactId.getCollection(),
          artifactId.getAuid(),
          artifactId.getUri(),
          true
      );

      // Create a new artifact identifier for this
      ArtifactIdentifier newId = new ArtifactIdentifier(
          // Assign a new artifact ID
          UUID.randomUUID().toString(),
          artifactId.getCollection(),
          artifactId.getAuid(),
          artifactId.getUri(),
          // Set the next version
          (latestVersion == null) ? 1 : latestVersion.getVersion() + 1
      );

      artifactData.setIdentifier(newId);

      //// Add the ArtifactData to the repository
      Artifact storedArtifact = store.addArtifactData(artifactData);
      // TODO: Artifact artifact = index.indexArtifact(storedArtifact);
      Artifact artifact = index.indexArtifact(artifactData);

      return artifact;
    } finally {
      auidLock.unlock();
    }
  }

  /**
   * Retrieves an artifact from this LOCKSS repository.
   *
   * @param artifactId A {@code String} with the artifact ID of the artifact to retrieve from this repository.
   * @return The {@code ArtifactData} referenced by this artifact ID.
   * @throws IOException
   */
  @Override
  public ArtifactData getArtifactData(String collection, String artifactId) throws IOException {
    if ((collection == null) || (artifactId == null))
      throw new IllegalArgumentException("Null collection id or artifact id");

    // Return null if artifact doesn't exist
    if (!artifactExists(collection, artifactId)) {
      return null;
    }

    // TODO: An AU level lock is overkill here
    Artifact artifact = index.getArtifact(artifactId);
    Lock auidLock = auidLockMap.getLock(new RepoAuid(artifact.getCollection(), artifact.getAuid()));
    auidLock.lock();

    try {
      return store.getArtifactData(index.getArtifact(artifactId));
    } finally {
      auidLock.unlock();
    }
  }

  /**
   * Commits an artifact to this LOCKSS repository for permanent storage and inclusion in LOCKSS repository queries.
   *
   * @param collection A {code String} containing the collection ID containing the artifact to commit.
   * @param artifactId A {@code String} with the artifact ID of the artifact to commit to the repository.
   * @return An {@code Artifact} containing the updated artifact state information.
   * @throws IOException
   */
  @Override
  public Artifact commitArtifact(String collection, String artifactId) throws IOException {
    if ((collection == null) || (artifactId == null))
      throw new IllegalArgumentException("Null collection id or artifact id");

    // Get artifact as it is currently
    Artifact artifact = index.getArtifact(artifactId);

    if (artifact == null) {
      log.warn("Artifact not found in index");
      return null;
    }

    Lock auidLock = auidLockMap.getLock(new RepoAuid(artifact.getCollection(), artifact.getAuid()));
    auidLock.lock();

    try {
      // Record the changed status in store
      store.updateArtifactMetadata(artifact.getIdentifier(), new RepositoryArtifactMetadata(
          artifact.getIdentifier(),
          true,
          false
      ));

      // Update the commit status in index and return the updated artifact
      return index.commitArtifact(artifactId);
    } finally {
      auidLock.unlock();
    }
  }

  /**
   * Permanently removes an artifact from this LOCKSS repository.
   *
   * @param artifactId A {@code String} with the artifact ID of the artifact to remove from this LOCKSS repository.
   * @throws IOException
   */
  @Override
  public void deleteArtifact(String collection, String artifactId) throws IOException {
    if ((collection == null) || (artifactId == null))
      throw new IllegalArgumentException("Null collection id or artifact id");

    Artifact artifact = index.getArtifact(artifactId);

    if ((artifact == null))
      throw new IllegalArgumentException("Non-existent artifact id: " + artifactId);

    Lock auidLock = auidLockMap.getLock(new RepoAuid(artifact.getCollection(), artifact.getAuid()));
    auidLock.lock();

    try {
      store.deleteArtifactData(artifact);
      index.deleteArtifact(artifactId);
    } finally {
      auidLock.unlock();
    }
  }

  /**
   * Checks whether an artifact exists in this LOCKSS repository.
   *
   * @param artifactId A {@code String} containing the artifact ID to check.
   * @return A boolean indicating whether an artifact exists in this repository.
   */
  @Override
  public Boolean artifactExists(String collectionId, String artifactId) throws IOException {
    if ((collectionId == null) || (artifactId == null))
      throw new IllegalArgumentException("Null collection id or artifact id");

    return index.artifactExists(artifactId);
  }

  /**
   * Checks whether an artifact is committed to this LOCKSS repository.
   *
   * @param artifactId A {@code String} containing the artifact ID to check.
   * @return A boolean indicating whether the artifact is committed.
   */
  @Override
  public Boolean isArtifactCommitted(String collectionId, String artifactId) throws IOException {
    if ((collectionId == null) || (artifactId == null))
      throw new IllegalArgumentException("Null collection id or artifact id");

    Artifact artifact = index.getArtifact(artifactId);

    if ((artifact == null))
      throw new IllegalArgumentException("Non-existent artifact id: " + artifactId);

    return artifact.getCommitted();
  }

  /**
   * Provides the collection identifiers of the committed artifacts in the index.
   *
   * @return An {@code Iterator<String>} with the index committed artifacts
   * collection identifiers.
   */
  @Override
  public Iterable<String> getCollectionIds() throws IOException {
    return index.getCollectionIds();
  }

  /**
   * Returns a list of Archival Unit IDs (AUIDs) in this LOCKSS repository collection.
   *
   * @param collection A {@code String} containing the LOCKSS repository collection ID.
   * @return A {@code Iterator<String>} iterating over the AUIDs in this LOCKSS repository collection.
   * @throws IOException
   */
  @Override
  public Iterable<String> getAuIds(String collection) throws IOException {
    if (collection == null)
      throw new IllegalArgumentException("Null collection");

    return index.getAuIds(collection);
  }

  /**
   * Returns the committed artifacts of the latest version of all URLs, from a specified Archival Unit and collection.
   *
   * @param collection A {@code String} containing the collection ID.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @return An {@code Iterator<Artifact>} containing the latest version of all URLs in an AU.
   * @throws IOException
   */
  @Override
  public Iterable<Artifact> getAllArtifacts(String collection, String auid) throws IOException {
    if ((collection == null) || (auid == null))
      throw new IllegalArgumentException("Null collection id or au id");

    return index.getAllArtifacts(collection, auid);
  }

  /**
   * Returns the committed artifacts of all versions of all URLs, from a specified Archival Unit and collection.
   *
   * @param collection A String with the collection identifier.
   * @param auid       A String with the Archival Unit identifier.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all version of all URLs in an AU.
   */
  @Override
  public Iterable<Artifact> getAllArtifactsAllVersions(String collection, String auid) throws IOException {
    if ((collection == null) || (auid == null))
      throw new IllegalArgumentException("Null collection id or au id");

    return index.getAllArtifactsAllVersions(collection, auid);
  }

  /**
   * Returns the committed artifacts of the latest version of all URLs matching a prefix, from a specified Archival
   * Unit and collection.
   *
   * @param collection A {@code String} containing the collection ID.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @param prefix     A {@code String} containing a URL prefix.
   * @return An {@code Iterator<Artifact>} containing the latest version of all URLs matching a prefix in an AU.
   * @throws IOException
   */
  @Override
  public Iterable<Artifact> getAllArtifactsWithPrefix(String collection, String auid, String prefix) throws IOException {
    if ((collection == null) || (auid == null) || (prefix == null))
      throw new IllegalArgumentException("Null collection id, au id or prefix");

    return index.getAllArtifactsWithPrefix(collection, auid, prefix);
  }

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified Archival Unit and
   * collection.
   *
   * @param collection A String with the collection identifier.
   * @param auid       A String with the Archival Unit identifier.
   * @param prefix     A String with the URL prefix.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matchign a
   * prefix from an AU.
   */
  @Override
  public Iterable<Artifact> getAllArtifactsWithPrefixAllVersions(String collection, String auid, String prefix) throws IOException {
    if ((collection == null) || (auid == null) || (prefix == null))
      throw new IllegalArgumentException("Null collection id, au id or prefix");

    return index.getAllArtifactsWithPrefixAllVersions(collection, auid, prefix);
  }

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified Archival Unit and collection.
   *
   * @param collection A {@code String} with the collection identifier.
   * @param auid       A {@code String} with the Archival Unit identifier.
   * @param url        A {@code String} with the URL to be matched.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL from an
   * Archival Unit.
   */
  @Override
  public Iterable<Artifact> getArtifactAllVersions(String collection, String auid, String url) throws IOException {
    if ((collection == null) || (auid == null) || (url == null))
      throw new IllegalArgumentException("Null collection id, au id or url");

    return index.getArtifactAllVersions(collection, auid, url);
  }

  /**
   * Returns the artifact of the latest version of given URL, from a specified Archival Unit and collection.
   *
   * @param collection A {@code String} containing the collection ID.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @param url        A {@code String} containing a URL.
   * @return The {@code Artifact} representing the latest version of the URL in the AU.
   * @throws IOException
   */
  @Override
  public Artifact getArtifact(String collection, String auid, String url) throws IOException {
    if ((collection == null) || (auid == null) || (url == null))
      throw new IllegalArgumentException("Null collection id, au id or url");

    return index.getArtifact(collection, auid, url);
  }

  /**
   * Returns the artifact of a given version of a URL, from a specified Archival Unit and collection.
   *
   * @param collection A String with the collection identifier.
   * @param auid       A String with the Archival Unit identifier.
   * @param url        A String with the URL to be matched.
   * @param version    A String with the version.
   * @return The {@code Artifact} of a given version of a URL, from a specified AU and collection.
   */
  @Override
  public Artifact getArtifactVersion(String collection, String auid, String url, Integer version) throws IOException {
    if ((collection == null) || (auid == null) ||
        (url == null) || version == null)
      throw new IllegalArgumentException("Null collection id, au id, url or version");

    return index.getArtifactVersion(collection, auid, url, version);
  }

  /**
   * Returns the size, in bytes, of AU in a collection.
   *
   * @param collection A {@code String} containing the collection ID.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @return A {@code Long} with the total size of the specified AU in bytes.
   */
  @Override
  public Long auSize(String collection, String auid) throws IOException {
    if ((collection == null) || (auid == null))
      throw new IllegalArgumentException("Null collection id or au id");

    return index.auSize(collection, auid);
  }

  /**
   * Returns a boolean indicating whether this repository is ready.
   * <p>
   * Delegates to readiness of internal artifact index and data store components.
   *
   * @return
   */
  @Override
  public boolean isReady() {
    return store.isReady() && index.isReady();
  }
}
