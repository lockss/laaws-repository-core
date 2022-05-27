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

package org.lockss.laaws.rs.io.index;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.lockss.laaws.rs.core.SemaphoreMap;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.ArtifactComparators;
import org.lockss.log.L4JLogger;
import org.lockss.util.storage.StorageInfo;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ArtifactData index implemented in memory, not persisted.
 */
public class VolatileArtifactIndex extends AbstractArtifactIndex {
    private final static L4JLogger log = L4JLogger.getLogger();

    /** Label to describe type of VolatileArtifactIndex */
    public static String ARTIFACT_INDEX_TYPE = "In-memory";

    // Internal map from artifact ID to Artifact
    protected Map<String, Artifact> index = new ConcurrentHashMap<>();

    /**
     * Map from artifact stem to semaphore. Used for artifact version locking.
     */
    private SemaphoreMap<ArtifactIdentifier.ArtifactStem> versionLock = new SemaphoreMap<>();

    @Override
    public void init() {
      setState(ArtifactIndexState.INITIALIZED);
    }

  @Override
  public void start() {
    setState(ArtifactIndexState.RUNNING);
  }

  @Override
    public void stop() {
      setState(ArtifactIndexState.STOPPED);
    }

    /**
     * Returns information about the storage size and free space
     * @return A {@code StorageInfo}
     */
    @Override
    public StorageInfo getStorageInfo() {
      return StorageInfo.fromRuntime().setType(ARTIFACT_INDEX_TYPE);
    }

  @Override
  public void acquireVersionLock(ArtifactIdentifier.ArtifactStem stem) throws IOException {
    // Acquire the lock for this artifact stem
    try {
      versionLock.getLock(stem);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted while waiting to acquire artifact version lock");
    }

  }

  @Override
  public void releaseVersionLock(ArtifactIdentifier.ArtifactStem stem) {
    // Release the lock for the artifact stem
    versionLock.releaseLock(stem);
  }

  /**
     * Adds an artifact to the index.
     * 
     * @param artifactData
     *          An ArtifactData with the artifact to be added to the index,.
     * @return an Artifact with the artifact indexing data.
     */
    @Override
    public Artifact indexArtifact(ArtifactData artifactData) {
      log.debug2("Adding artifact to index: {}", artifactData);

        if (artifactData == null) {
          throw new IllegalArgumentException("Null artifact data");
        }

        ArtifactIdentifier artifactId = artifactData.getIdentifier();

        if (artifactId == null) {
          throw new IllegalArgumentException("ArtifactData has null identifier");
        }

        String id = artifactId.getId();

        if (StringUtils.isEmpty(id)) {
          throw new IllegalArgumentException(
              "ArtifactIdentifier has null or empty id");
        }

        // Get artifact's repository state
        ArtifactRepositoryState state = artifactData.getArtifactRepositoryState();

        // Create and populate an Artifact bean for this ArtifactData
        Artifact artifact = new Artifact(
            artifactId,
            state == null ? false : state.isCommitted(),
            artifactData.getStorageUrl().toString(),
            artifactData.getContentLength(),
            artifactData.getContentDigest()
        );

        // Save the artifact collection date.
        artifact.setCollectionDate(artifactData.getCollectionDate());

        // Add Artifact to the index
        addToIndex(id, artifact);

        log.debug("Added artifact to index: {}", artifact);

        return artifact;
    }

    /**
     * Provides the index data of an artifact with a given text index
     * identifier.
     * 
     * @param artifactId
     *          A String with the artifact index identifier.
     * @return an Artifact with the artifact indexing data.
     */
    @Override
    public Artifact getArtifact(String artifactId) {
      if (StringUtils.isEmpty(artifactId)) {
        throw new IllegalArgumentException("Null or empty artifact ID");
      }

      return index.get(artifactId);
    }

    /**
     * Provides the index data of an artifact with a given index identifier
     * UUID.
     * 
     * @param artifactId
     *          An UUID with the artifact index identifier.
     * @return an Artifact with the artifact indexing data.
     */
    @Override
    public Artifact getArtifact(UUID artifactId) {
      if (artifactId == null) {
        throw new IllegalArgumentException("Null UUID");
      }

      return getArtifact(artifactId.toString());
    }

    /**
     * Commits to the index an artifact with a given text index identifier.
     * 
     * @param artifactId
     *          A String with the artifact index identifier.
     * @return an Artifact with the committed artifact indexing data.
     */
    @Override
    public Artifact commitArtifact(String artifactId) {
      if (StringUtils.isEmpty(artifactId)) {
        throw new IllegalArgumentException("Null or empty artifact ID");
      }

      Artifact artifact = index.get(artifactId);

      if (artifact != null) {
        artifact.setCommitted(true);
      }

      return artifact;
    }

    /**
     * Commits to the index an artifact with a given index identifier UUID.
     * 
     * @param artifactId
     *          An UUID with the artifact index identifier.
     * @return an Artifact with the committed artifact indexing data.
     */
    @Override
    public Artifact commitArtifact(UUID artifactId) {
      if (artifactId == null) {
        throw new IllegalArgumentException("Null UUID");
      }

      return commitArtifact(artifactId.toString());
    }

    /**
     * Removes from the index an artifact with a given text index identifier.
     * 
     * @param artifactId
     *          A String with the artifact index identifier.
     * @return <code>true</code> if the artifact was removed from in the index,
     * <code>false</code> otherwise.
     */
    @Override
    public boolean deleteArtifact(String artifactId) {
      if (StringUtils.isEmpty(artifactId)) {
        throw new IllegalArgumentException("Null or empty identifier");
      }

      return removeFromIndex(artifactId) != null;
    }

    /**
     * Removes from the index an artifact with a given index identifier UUID.
     * 
     * @param artifactId
     *          A String with the artifact index identifier.
     * @return <code>true</code> if the artifact was removed from in the index,
     * <code>false</code> otherwise.
     */
    @Override
    public boolean deleteArtifact(UUID artifactId) {
      if (artifactId == null) {
        throw new IllegalArgumentException("Null UUID");
      }

      return deleteArtifact(artifactId.toString());
    }

    /**
     * Provides an indication of whether an artifact with a given text index
     * identifier exists in the index.
     * 
     * @param artifactId
     *          A String with the artifact identifier.
     * @return <code>true</code> if the artifact exists in the index,
     * <code>false</code> otherwise.
     */
    @Override
    public boolean artifactExists(String artifactId) {
      if (StringUtils.isEmpty(artifactId)) {
        throw new IllegalArgumentException("Null or empty artifact ID");
      }

      return index.containsKey(artifactId);
    }
    
    @Override
    public Artifact updateStorageUrl(String artifactId, String storageUrl) {
      if (StringUtils.isEmpty(artifactId)) {
        throw new IllegalArgumentException("Invalid artifact ID");
      }

      if (StringUtils.isEmpty(storageUrl)) {
        throw new IllegalArgumentException("Cannot update storage URL: Null or empty storage URL");
      }

      // Retrieve the Artifact from the internal artifacts map
      Artifact artifact = index.get(artifactId);

      // Return null if the artifact could not be found
      if (artifact == null) {
        log.warn("Could not update storage URL: Artifact not found [artifactId: " + artifactId + "]");
        return null;
      }

      // Update the storage URL of this Artifact in the internal artifacts map
      artifact.setStorageUrl(storageUrl);

      // Return the artifact
      return artifact;
    }

    /**
     * Provides the collection identifiers of the committed artifacts in the index.
     *
     * @return An {@code Iterator<String>} with the index committed artifacts
     * collection identifiers.
     */
    @Override
    public Iterable<String> getCollectionIds() {
      List<String> res = index.values().stream()
        .map(x -> x.getCollection())
        .distinct()
        .sorted()
        .collect(Collectors.toList());
      return res;
    }

    /**
     * Returns a list of Archival Unit IDs (AUIDs) in this LOCKSS repository collection.
     *
     * @param collection
     *          A {@code String} containing the LOCKSS repository collection ID.
     * @return A {@code Iterator<String>} iterating over the AUIDs in this LOCKSS repository collection.
     * @throws IOException
     */
    @Override
    public Iterable<String> getAuIds(String collection) throws IOException {
      ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
      query.filterByCollection(collection);

      List<String> res = index.values().stream()
        .filter(query.build())
        .map(x -> x.getAuid()).distinct()
        .sorted()
        .collect(Collectors.toList());
      return res;
    }

    /**
     * Returns the committed artifacts of the latest version of all URLs, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @return An {@code Iterator<Artifact>} containing the latest version of all URLs in an AU.
     * @throws IOException
     */
    @Override
    public Iterable<Artifact> getArtifacts(String collection, String auid, boolean includeUncommitted) {
        ArtifactPredicateBuilder q = new ArtifactPredicateBuilder();

        // Filter by committed status equal to true?
        if (!includeUncommitted)
            q.filterByCommitStatus(true);

        q.filterByCollection(collection);
        q.filterByAuid(auid);

        // Filter, then group the Artifacts by URI, and pick the Artifacts with max version from each group
        Map<String, Optional<Artifact>> result = index.values().stream()
          .filter(q.build())
          .collect(Collectors.groupingBy(Artifact::getUri,
                                         Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))));

        // Return an iterator over the artifact from each group (one per URI), after sorting them by artifact URI then
        // descending version.
        return IteratorUtils.asIterable(result.values().stream()
                                        .filter(Optional::isPresent).map(x -> x.get())
                                        .sorted(ArtifactComparators.BY_URI).iterator());
    }

    /**
     * Returns the artifacts of all versions of all URLs, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param includeUncommitted
     *          A {@code boolean} indicating whether to return all the versions among both committed and uncommitted
     *          artifacts.
     * @return An {@code Iterator<Artifact>} containing the artifacts of all version of all URLs in an AU.
     */
    @Override
    public Iterable<Artifact> getArtifactsAllVersions(String collection, String auid, boolean includeUncommitted) {
        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();

        if (!includeUncommitted) {
            query.filterByCommitStatus(true);
        }

        query.filterByCollection(collection);
        query.filterByAuid(auid);

        // Apply the filter, sort by artifact URL then descending version, and return an iterator over the Artifacts
        return IteratorUtils.asIterable(getIterableArtifacts().stream().filter(query.build())
            .sorted(ArtifactComparators.BY_URI_BY_DECREASING_VERSION).iterator());
    }

    /**
     * Returns the artifacts of the latest committed version of all URLs matching a prefix, from a specified Archival
     * Unit and collection.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @param prefix
     *          A {@code String} containing a URL prefix.
     * @return An {@code Iterator<Artifact>} containing the latest version of all URLs matching a prefix in an AU.
     * @throws IOException
     */
    @Override
    public Iterable<Artifact> getArtifactsWithPrefix(String collection, String auid, String prefix) throws IOException {
        ArtifactPredicateBuilder q = new ArtifactPredicateBuilder();
        q.filterByCommitStatus(true);
        q.filterByCollection(collection);
        q.filterByAuid(auid);
        q.filterByURIPrefix(prefix);

        // Apply the filter, group the Artifacts by URL, then pick the Artifact with highest version from each group
        Map<String, Optional<Artifact>> result = index.values().stream()
          .filter(q.build())
          .collect(Collectors.groupingBy(Artifact::getUri,
                                         Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))));

        // Return an iterator over the artifact from each group (one per URI), after sorting them by artifact URI then
        // descending version.
        return IteratorUtils.asIterable(result.values().stream()
                                        .filter(Optional::isPresent).map(x -> x.get())
                                        .sorted(ArtifactComparators.BY_URI).iterator());
    }

    /**
     * Returns the artifacts of all committed versions of all URLs matching a prefix, from a specified Archival Unit and
     * collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param prefix
     *          A String with the URL prefix.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
     *         prefix from an AU.
     */
    @Override
    public Iterable<Artifact> getArtifactsWithPrefixAllVersions(String collection, String auid, String prefix) {
        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);
        query.filterByAuid(auid);
        query.filterByURIPrefix(prefix);

	// Apply filter then sort the resulting Artifacts by URL and descending version
	return IteratorUtils.asIterable(getIterableArtifacts().stream().filter(query.build())
            .sorted(ArtifactComparators.BY_URI_BY_DECREASING_VERSION).iterator());
    }

    /**
     * Returns the artifacts of all committed versions of all URLs matching a prefix, from a specified collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param urlPrefix
     *          A String with the URL prefix.
     * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
     *                   versions of an artifact.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
     *         prefix.
     */
    @Override
    public Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String collection, String urlPrefix,
                                                                  ArtifactVersions versions) {

      if (!(versions == ArtifactVersions.ALL ||
            versions == ArtifactVersions.LATEST)) {
        throw new IllegalArgumentException("Versions must be ALL or LATEST");
      }

      if (collection == null) {
        throw new IllegalArgumentException("Collection is null");
      }

      ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
      query.filterByCommitStatus(true);
      query.filterByCollection(collection);

      if (urlPrefix != null) {
        query.filterByURIPrefix(urlPrefix);
      }

      // Apply predicates filter to Artifact stream
      Stream<Artifact> allVersions = index.values().stream().filter(query.build());

      if (versions == ArtifactVersions.LATEST) {
        Stream<Artifact> latestVersions = allVersions
          // Group the Artifacts by URL then pick the Artifact with highest version from each group
          .collect(Collectors.groupingBy(artifact -> artifact.getIdentifier().getArtifactStem(),
                                         Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))))
          .values()
          .stream()
          .filter(Optional::isPresent)
          .map(Optional::get);

        return IteratorUtils.asIterable(
                                        latestVersions.sorted(ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION).iterator());
      }

      return IteratorUtils.asIterable(
                                      allVersions.sorted(ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION).iterator());
    }

    /**
     * Returns the committed artifacts of all versions of a given URL, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A {@code String} with the collection identifier.
     * @param auid
     *          A {@code String} with the Archival Unit identifier.
     * @param url
     *          A {@code String} with the URL to be matched.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL from an
     *         Archival Unit.
     */
    @Override
    public Iterable<Artifact> getArtifactsAllVersions(String collection, String auid, String url) {
        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);
        query.filterByAuid(auid);
        query.filterByURIMatch(url);

        // Apply filter then sort the resulting Artifacts by URL and descending version
        return IteratorUtils.asIterable(getIterableArtifacts().stream().filter(query.build())
            .sorted(ArtifactComparators.BY_DECREASING_VERSION).iterator());
    }

    /**
     * Returns the committed artifacts of all versions of a given URL, from a specified collection.
     *
     * @param collection
     *          A {@code String} with the collection identifier.
     * @param url
     *          A {@code String} with the URL to be matched.
     * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
     *                   versions of an artifact.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL.
     */
    @Override
    public Iterable<Artifact> getArtifactsWithUrlFromAllAus(String collection, String url, ArtifactVersions versions) {
      if (!(versions == ArtifactVersions.ALL ||
          versions == ArtifactVersions.LATEST)) {
        throw new IllegalArgumentException("Versions must be ALL or LATEST");
      }

      if (collection == null || url == null) {
        throw new IllegalArgumentException("Collection or URL is null");
      }

      ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);
        query.filterByURIMatch(url);

        // Apply predicates filter to Artifact stream
        Stream<Artifact> allVersions = index.values().stream().filter(query.build());

        if (versions == ArtifactVersions.LATEST) {
          Stream<Artifact> latestVersions = allVersions
            // Group the Artifacts by URL then pick the Artifact with highest version from each group
            .collect(Collectors.groupingBy(artifact -> artifact.getIdentifier().getArtifactStem(),
                                           Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))))
            .values()
            .stream()
            .filter(Optional::isPresent)
            .map(Optional::get);

          return IteratorUtils.asIterable(latestVersions.sorted(ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION).iterator());
        }

        return IteratorUtils.asIterable(allVersions.sorted(ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION).iterator());
    }

    /**
     * Returns the artifact of the latest version of given URL, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @param url
     *          A {@code String} containing a URL.
     * @param includeUncommitted
     *          A {@code boolean} indicating whether to return the latest version among both committed and uncommitted
     *          artifacts of a URL.
     * @return An {@code Artifact} representing the latest version of the URL in the AU.
     * @throws IOException
     */
    @Override
    public Artifact getArtifact(String collection, String auid, String url, boolean includeUncommitted) {
        ArtifactPredicateBuilder q = new ArtifactPredicateBuilder();

        if (!includeUncommitted) {
            q.filterByCommitStatus(true);
        }

        q.filterByCollection(collection);
        q.filterByAuid(auid);
        q.filterByURIMatch(url);

        // Apply the filter then get the artifact with max version
        Optional<Artifact> result = index.values().stream().filter(q.build()).max(Comparator.comparingInt(Artifact::getVersion));

        // Return the artifact, or null if one was not found
        return result.orElse(null);
    }

    /**
     * Returns the artifact of a given version of a URL, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param url
     *          A String with the URL to be matched.
     * @param version
     *          A String with the version.
     * @param includeUncommitted
     *          A boolean with the indication of whether an uncommitted artifact
     *          may be returned.
     * @return The {@code Artifact} of a given version of a URL, from a specified AU and collection.
     */
    @Override
    public Artifact getArtifactVersion(String collection, String auid, String url, Integer version, boolean includeUncommitted) {
      ArtifactPredicateBuilder q = new ArtifactPredicateBuilder();

      // Only filter by commit status when no uncommitted artifact is to be returned.
      if (!includeUncommitted) {
	q.filterByCommitStatus(true);
      }

      q.filterByCollection(collection);
      q.filterByAuid(auid);
      q.filterByURIMatch(url);
      q.filterByVersion(version);

      List<Artifact> artifacts = index.values().stream().filter(q.build()).collect(Collectors.toList());

      switch (artifacts.size()) {
      case 0:
        return null;
      case 1:
        return artifacts.get(0);
      default:
        String errMsg = "Found more than one artifact having the same version!";
        log.error(errMsg);
        throw new IllegalStateException(errMsg);
      }
    }

    /**
     * Returns the size, in bytes, of AU in a collection.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @return A {@link AuSize} with byte size statistics of the specified AU.
     */
    @Override
    public AuSize auSize(String collection, String auid) {
      AuSize auSize = new AuSize();

      auSize.setTotalAllVersions(0L);
      auSize.setTotalLatestVersions(0L);
      // auSize.setTotalWarcSize(null);

      ArtifactPredicateBuilder q = new ArtifactPredicateBuilder();
      q.filterByCommitStatus(true);
      q.filterByCollection(collection);
      q.filterByAuid(auid);

      boolean isAuEmpty = !index.values()
          .stream()
          .filter(q.build())
          .findFirst()
          .isPresent();

      if (isAuEmpty) {
        auSize.setTotalWarcSize(0L);
        return auSize;
      }

      auSize.setTotalAllVersions(
          index.values()
              .stream()
              .filter(q.build())
              .mapToLong(Artifact::getContentLength)
              .sum());

      Map<String, Optional<Artifact>> latestArtifactVersions =
          index.values()
              .stream()
              .filter(q.build())
              .collect(Collectors.groupingBy(Artifact::getUri, Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))));

      auSize.setTotalLatestVersions(
          latestArtifactVersions.values().stream()
              .filter(Optional::isPresent)
              .map(Optional::get)
              .mapToLong(Artifact::getContentLength)
              .sum());

      return auSize;
    }
    /**
     * Adds an artifact to the index.
     *
     * @param id
     *          A String with the identifier of the article to be added.
     * @param artifact
     *          An Artifact with the artifact to be added.
     */
    protected void addToIndex(String id, Artifact artifact) {
      // Add Artifact to the index.
      index.put(id, artifact);
    }

    /**
     * Removes an artifact from the index.
     *
     * @param id
     *          A String with the identifier of the article to be removed.
     * @return an Artifact with the artifact that has been removed from the
     *         index or null if not found
     */
    protected Artifact removeFromIndex(String id) {
      // Remove Artifact from the index.
      return index.remove(id);
    }

    private Collection<Artifact> getIterableArtifacts() {
      return index.values();
    }

    @Override
    public String toString() {
      return "[VolatileArtifactIndex index=" + index + "]";
    }

    /**
     * Returns a boolean indicating whether this artifact index is ready.
     *
     * Always returns true in the violate implementation.
     */
    @Override
    public boolean isReady() {
        return getState() == ArtifactIndexState.RUNNING;
    }
}
