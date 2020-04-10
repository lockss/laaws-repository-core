/*
 * Copyright (c) 2017-2020, Board of Trustees of Leland Stanford Jr. University,
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

import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.VolatileArtifactIndex;
import org.lockss.laaws.rs.io.storage.*;
import org.lockss.laaws.rs.io.storage.warc.VolatileWarcArtifactDataStore;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;
import org.lockss.laaws.rs.model.RepositoryInfo;
import org.lockss.laaws.rs.util.*;
import org.lockss.util.jms.JmsFactory;
import org.lockss.util.storage.StorageInfo;
import org.lockss.log.L4JLogger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Base implementation of the LOCKSS Repository service.
 */
public class BaseLockssRepository implements LockssRepository,
					     JmsFactorySource {
  private final static L4JLogger log = L4JLogger.getLogger();

  protected ArtifactDataStore<ArtifactIdentifier, ArtifactData, RepositoryArtifactMetadata> store;
  protected ArtifactIndex index;
  protected JmsFactory jmsFact;

  /**
   * Create a LOCKSS repository with the provided artifact index and storage layers.
   *
   * @param index An instance of {@code ArtifactIndex}.
   * @param store An instance of {@code ArtifactDataStore}.
   */
  public BaseLockssRepository(ArtifactIndex index, ArtifactDataStore store) throws IOException {
    if (index == null || store == null) {
      throw new IllegalArgumentException("Cannot start repository with a null artifact index or store");
    }

    this.index = index;
    this.store = store;
  }

  /** No-arg constructor for subclasses */
  protected BaseLockssRepository() throws IOException {
  }

  @Override
  public void initRepository() throws IOException {
    log.info("Initializing repository");
    index.initIndex();
    store.initDataStore();
  }

  @Override
  public void shutdownRepository() throws InterruptedException {
    log.info("Shutting down repository");
    index.shutdownIndex();
    store.shutdownDataStore();
  }

  /** JmsFactorySource method to store a JmsFactory for use by (a user of)
   * this index.
   * @param fact a JmsFactory for creating JmsProducer and JmsConsumer
   * instances.
   */
  @Override
  public void setJmsFactory(JmsFactory fact) {
    this.jmsFact = fact;
  }

  /** JmsFactorySource method to provide a JmsFactory.
   * @return a JmsFactory for creating JmsProducer and JmsConsumer
   * instances.
   */
  public JmsFactory getJmsFactory() {
    return jmsFact;
  }

  /**
   * Returns information about the repository's storage areas
   *
   * @return A {@code RepositoryInfo}
   * @throws IOException if there are problems getting the repository data.
   */
  @Override
  public RepositoryInfo getRepositoryInfo() throws IOException {
    StorageInfo ind = null;
    StorageInfo sto = null;
    try {
      ind = index.getStorageInfo();
    } catch (Exception e) {
      log.warn("Couldn't get index space", e);
    }
    try {
      sto = store.getStorageInfo();
    } catch (Exception e) {
      log.warn("Couldn't get store space", e);
    }
    return new RepositoryInfo(sto, ind);
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
    if (artifactData == null) {
      throw new IllegalArgumentException("Null ArtifactData");
    }

    ArtifactIdentifier artifactId = artifactData.getIdentifier();

    //// Determine the next artifact version
    synchronized (index) {
      // Retrieve latest version in this URL lineage
      Artifact latestVersion = index.getArtifact(
          artifactId.getCollection(),
          artifactId.getAuid(),
          artifactId.getUri(),
          true
      );

      // Create a new artifact identifier for this artifact
      ArtifactIdentifier newId = new ArtifactIdentifier(
          // Assign a new artifact ID
          UUID.randomUUID().toString(), // FIXME: Namespace collision unlikely but possible
          artifactId.getCollection(),
          artifactId.getAuid(),
          artifactId.getUri(),
          // Set the next version
          (latestVersion == null) ? 1 : latestVersion.getVersion() + 1
      );

      // Set the new artifact identifier
      artifactData.setIdentifier(newId);

      // Add the artifact the data store and index
      Artifact artifact = store.addArtifactData(artifactData);

      return artifact;
    }
  }

  /**
   * Returns the artifact with the specified artifactId
   *
   * @param artifactId
   * @return The {@code Artifact} with the artifactId, or null if none
   * @throws IOException
   */
  public Artifact getArtifactFromId(String artifactId) throws IOException {
    return index.getArtifact(artifactId);
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

    // Throw if artifact doesn't exist
    if (!artifactExists(collection, artifactId)) {
      throw new LockssNoSuchArtifactIdException("Non-existent artifact id: "
						+ artifactId);
    }
    return store.getArtifactData(index.getArtifact(artifactId));
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
    if ((collection == null) || (artifactId == null)) {
      throw new IllegalArgumentException("Null collection id or artifact id");
    }

    synchronized (index) {
      // Get artifact as it is currently
      Artifact artifact = index.getArtifact(artifactId);

      if (artifact == null) {
	throw new LockssNoSuchArtifactIdException("Non-existent artifact id: "
						  + artifactId);
      }

      if (!artifact.getCommitted()) {
        // Commit artifact in data store and index
        Future<Artifact> future = store.commitArtifactData(artifact);
        index.commitArtifact(artifactId);

        if (future != null) {
          try {
            Artifact committedArtifact = future.get();
            index.updateStorageUrl(artifact.getId(), committedArtifact.getStorageUrl());
            return committedArtifact;
          } catch (InterruptedException e) {
            log.error(
                "Move of artifact [artifactId: {}] to permanent storage was interrupted: {}",
                artifactId,
                e
            );

            // TODO: Requeue?
            throw new IOException(e);
          } catch (ExecutionException e) {
            log.error(
                "Caught ExecutionException while moving artifact [artifactId: {}] to permanent storage",
                artifactId,
                e
            );

            // TODO: Need to discuss what to do here - for now wrap and throw
            throw new IOException(e);
          }
        }
      }

      // TODO: An Artifact could be marked as committed but have no pending move to permanent storage queued
      return artifact;
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
    if (collection == null || artifactId == null) {
      throw new IllegalArgumentException("Null collection id or artifact id");
    }

    synchronized (index) {
      Artifact artifact = index.getArtifact(artifactId);

      if (artifact == null) {
        throw new LockssNoSuchArtifactIdException("Non-existent artifact id: "
						  + artifactId);
      }

      // Remove from index and data store
      store.deleteArtifactData(artifact);
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
    if (collectionId == null || artifactId == null) {
      throw new IllegalArgumentException("Null collection id or artifact id");
    }

    synchronized (index) {
      return index.artifactExists(artifactId);
    }
  }

  /**
   * Checks whether an artifact is committed to this LOCKSS repository.
   *
   * @param artifactId A {@code String} containing the artifact ID to check.
   * @return A boolean indicating whether the artifact is committed.
   */
  @Override
  public Boolean isArtifactCommitted(String collectionId, String artifactId) throws IOException {
    if (collectionId == null || artifactId == null) {
      throw new IllegalArgumentException("Null collection id or artifact id");
    }

    synchronized (index) {
      Artifact artifact = index.getArtifact(artifactId);

      if (artifact == null) {
        throw new LockssNoSuchArtifactIdException("Non-existent artifact id: "
						  + artifactId);
      }

      return artifact.getCommitted();
    }
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
    if (collection == null) {
      throw new IllegalArgumentException("Null collection");
    }

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
  public Iterable<Artifact> getArtifacts(String collection, String auid) throws IOException {
    if (collection == null || auid == null) {
      throw new IllegalArgumentException("Null collection id or au id");
    }

    return index.getArtifacts(collection, auid);
  }

  /**
   * Returns the committed artifacts of all versions of all URLs, from a specified Archival Unit and collection.
   *
   * @param collection A String with the collection identifier.
   * @param auid       A String with the Archival Unit identifier.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all version of all URLs in an AU.
   */
  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String collection, String auid) throws IOException {
    if (collection == null || auid == null) {
      throw new IllegalArgumentException("Null collection id or au id");
    }

    return index.getArtifactsAllVersions(collection, auid);
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
  public Iterable<Artifact> getArtifactsWithPrefix(String collection, String auid, String prefix) throws IOException {
    if (collection == null || auid == null || prefix == null) {
      throw new IllegalArgumentException("Null collection id, au id or prefix");
    }

    return index.getArtifactsWithPrefix(collection, auid, prefix);
  }

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified Archival Unit and
   * collection.
   *
   * @param collection A String with the collection identifier.
   * @param auid       A String with the Archival Unit identifier.
   * @param prefix     A String with the URL prefix.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix from an AU.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithPrefixAllVersions(String collection, String auid, String prefix) throws IOException {
    if (collection == null || auid == null || prefix == null) {
      throw new IllegalArgumentException("Null collection id, au id or prefix");
    }

    return index.getArtifactsWithPrefixAllVersions(collection, auid, prefix);
  }

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a collection.
   *
   * @param collection A String with the collection identifier.
   * @param prefix     A String with the URL prefix.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithPrefixAllVersionsAllAus(String collection, String prefix) throws IOException {
    if (collection == null || prefix == null) {
      throw new IllegalArgumentException("Null collection id or prefix");
    }

    return index.getArtifactsWithPrefixAllVersionsAllAus(collection, prefix);
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
  public Iterable<Artifact> getArtifactsAllVersions(String collection, String auid, String url) throws IOException {
    if (collection == null || auid == null || url == null) {
      throw new IllegalArgumentException("Null collection id, au id or url");
    }

    return index.getArtifactsAllVersions(collection, auid, url);
  }

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified collection.
   *
   * @param collection A {@code String} with the collection identifier.
   * @param url        A {@code String} with the URL to be matched.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL.
   */
  @Override
  public Iterable<Artifact> getArtifactsAllVersionsAllAus(String collection, String url) throws IOException {
    if (collection == null || url == null) {
      throw new IllegalArgumentException("Null collection id or url");
    }

    return index.getArtifactsAllVersionsAllAus(collection, url);
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
    if (collection == null || auid == null || url == null) {
      throw new IllegalArgumentException("Null collection id, au id or url");
    }

    return index.getArtifact(collection, auid, url);
  }

  /**
   * Returns the artifact of a given version of a URL, from a specified Archival Unit and collection.
   *
   * @param collection A String with the collection identifier.
   * @param auid       A String with the Archival Unit identifier.
   * @param url        A String with the URL to be matched.
   * @param version    A String with the version.
   * @param includeUncommitted
   *          A boolean with the indication of whether an uncommitted artifact
   *          may be returned.
   * @return The {@code Artifact} of a given version of a URL, from a specified AU and collection.
   */
  @Override
  public Artifact getArtifactVersion(String collection, String auid, String url, Integer version, boolean includeUncommitted) throws IOException {
    if (collection == null || auid == null || url == null || version == null) {
      throw new IllegalArgumentException("Null collection id, au id, url or version");
    }

    return index.getArtifactVersion(collection, auid, url, version,
	includeUncommitted);
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
    if (collection == null || auid == null) {
      throw new IllegalArgumentException("Null collection id or au id");
    }

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
