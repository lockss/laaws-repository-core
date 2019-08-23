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

package org.lockss.laaws.rs.io.index;

import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.util.ArtifactComparators;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.Artifact;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.lockss.log.L4JLogger;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ArtifactData index implemented in memory, not persisted.
 */
public class VolatileArtifactIndex extends AbstractArtifactIndex {
    private final static L4JLogger log = L4JLogger.getLogger();

    // Internal map from artifact ID to Artifact
    protected Map<String, Artifact> index = new LinkedHashMap<>();

    @Override
    public void initIndex() {
      setState(ArtifactIndexState.READY);
    }

    @Override
    public void shutdownIndex() {
      setState(ArtifactIndexState.SHUTDOWN);
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

        // Create and populate an Artifact bean for this ArtifactData
        Artifact artifact = new Artifact(
                artifactId,
                false,
                artifactData.getStorageUrl(),
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

      synchronized (index) {
        return index.get(artifactId);
      }
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

      synchronized (index) {
        Artifact artifact = index.get(artifactId);

        if (artifact != null) {
          artifact.setCommitted(true);
          removeFromIndex(artifactId);
          addToIndex(artifactId, artifact);
        }

        return artifact;
      }
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

      boolean result = false;

      synchronized (index) {
        if (removeFromIndex(artifactId) != null) {
          result = index.get(artifactId) == null;
        }
      }

      return result;
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

      synchronized (index) {
        return index.containsKey(artifactId);
      }
    }
    
    @Override
    public Artifact updateStorageUrl(String artifactId, String storageUrl) {
      if (StringUtils.isEmpty(artifactId)) {
        throw new IllegalArgumentException("Cannot update storage URL: Null or empty artifact ID");
      }

      if (StringUtils.isEmpty(storageUrl)) {
        throw new IllegalArgumentException("Cannot update storage URL: Null or empty storage URL");
      }

      synchronized (index) {
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
    }

    /**
     * Provides the collection identifiers of the committed artifacts in the index.
     *
     * @return An {@code Iterator<String>} with the index committed artifacts
     * collection identifiers.
     */
    @Override
    public Iterable<String> getCollectionIds() {
      synchronized (index) {
        Stream<Artifact> artifactStream = index.values().stream();
        Stream<Artifact> committedArtifacts = artifactStream.filter(x -> x.getCommitted());
        Map<String, List<Artifact>> collections = committedArtifacts.collect(Collectors.groupingBy(Artifact::getCollection));

        // Sort the collection IDs for return
        List<String> collectionIds = new ArrayList<String>(collections.keySet());
        Collections.sort(collectionIds);

        // Interface requires an iterator since this list could be very large in other implementations
        return IteratorUtils.asIterable(collectionIds.iterator());
      }
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
      synchronized (index) {
        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);

        return IteratorUtils.asIterable(
            index.values().stream().filter(query.build()).map(x -> x.getAuid()).distinct().sorted().iterator()
        );
      }
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
      synchronized (index) {
        Map<String, Optional<Artifact>> result = index.values().stream().filter(q.build()).collect(
            Collectors.groupingBy(Artifact::getUri, Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion)))
        );

        // Return an iterator over the artifact from each group (one per URI), after sorting them by artifact URI then
        // descending version.
        return IteratorUtils.asIterable(result.values().stream().filter(Optional::isPresent).map(x -> x.get())
                .sorted(ArtifactComparators.BY_URI).iterator());
      }
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
      synchronized (index) {
        return IteratorUtils.asIterable(index.values().stream().filter(query.build())
            .sorted(ArtifactComparators.BY_URI_BY_DECREASING_VERSION).iterator());
      }
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

      synchronized (index) {
        // Apply the filter, group the Artifacts by URL, then pick the Artifact with highest version from each group
        Map<String, Optional<Artifact>> result = index.values().stream().filter(q.build()).collect(
            Collectors.groupingBy(Artifact::getUri, Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion)))
        );

        // Return an iterator over the artifact from each group (one per URI), after sorting them by artifact URI then
        // descending version.
        return IteratorUtils.asIterable(result.values().stream().filter(Optional::isPresent).map(x -> x.get())
            .sorted(ArtifactComparators.BY_URI).iterator());
      }
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

        synchronized (index) {
          // Apply filter then sort the resulting Artifacts by URL and descending version
          return IteratorUtils.asIterable(index.values().stream().filter(query.build())
              .sorted(ArtifactComparators.BY_URI_BY_DECREASING_VERSION).iterator());
        }
    }

    /**
     * Returns the artifacts of all committed versions of all URLs matching a prefix, from a specified collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param prefix
     *          A String with the URL prefix.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
     *         prefix.
     */
    @Override
    public Iterable<Artifact> getArtifactsWithPrefixAllVersionsAllAus(String collection, String prefix) {
        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);

        // Q: Perhaps it would be better to throw an IllegalArgumentException if prefix is null? Without this filter, we
        //    return all the committed artifacts in a collection. Is that useful?
        if (prefix != null) {
          query.filterByURIPrefix(prefix);
        }

        synchronized (index) {
          // Apply filter then sort the resulting Artifacts by URL, date, AUID and descending version
          return IteratorUtils.asIterable(index.values().stream().filter(query.build())
              .sorted(ArtifactComparators.BY_URI_BY_DATE_BY_AUID_BY_DECREASING_VERSION).iterator());
        }
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

        synchronized (index) {
          // Apply filter then sort the resulting Artifacts by URL and descending version
          return IteratorUtils.asIterable(index.values().stream().filter(query.build())
              .sorted(ArtifactComparators.BY_DECREASING_VERSION).iterator());
        }
    }

    /**
     * Returns the committed artifacts of all versions of a given URL, from a specified collection.
     *
     * @param collection
     *          A {@code String} with the collection identifier.
     * @param url
     *          A {@code String} with the URL to be matched.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL.
     */
    @Override
    public Iterable<Artifact> getArtifactsAllVersionsAllAus(String collection, String url) {
        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);
        query.filterByURIMatch(url);

        synchronized (index) {
          // Apply filter then sort the resulting Artifacts by date, AUID and descending version
          return IteratorUtils.asIterable(index.values().stream().filter(query.build())
              .sorted(ArtifactComparators.BY_URI_BY_DATE_BY_AUID_BY_DECREASING_VERSION).iterator());
        }
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

        synchronized (index) {
          // Apply the filter then get the artifact with max version
          Optional<Artifact> result = index.values().stream().filter(q.build()).max(Comparator.comparingInt(Artifact::getVersion));

          // Return the artifact, or null if one was not found
          return result.orElse(null);
        }
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
     * @return The {@code Artifact} of a given version of a URL, from a specified AU and collection.
     */
    @Override
    public Artifact getArtifactVersion(String collection, String auid, String url, Integer version) {
      ArtifactPredicateBuilder q = new ArtifactPredicateBuilder();
      q.filterByCommitStatus(true);
      q.filterByCollection(collection);
      q.filterByAuid(auid);
      q.filterByURIMatch(url);
      q.filterByVersion(version);

      synchronized (index) {
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
    }

    /**
     * Returns the size, in bytes, of AU in a collection.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @return A {@code Long} with the total size of the specified AU in bytes.
     */
    @Override
    public Long auSize(String collection, String auid) {
        ArtifactPredicateBuilder q = new ArtifactPredicateBuilder();
        q.filterByCommitStatus(true);
        q.filterByCollection(collection);
        q.filterByAuid(auid);

        synchronized (index) {
          Map<String, Optional<Artifact>> result = index.values().stream()
              .filter(q.build())
              .collect(Collectors.groupingBy(Artifact::getUri,
                  Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion)))
              );

          return result.values().stream()
              .filter(Optional::isPresent)
              .map(Optional::get)
              .mapToLong(Artifact::getContentLength)
              .sum();
        }
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
      synchronized (index) {
        // Add Artifact to the index.
        index.put(id, artifact);
      }
    }

    /**
     * Removes an artifact from the index.
     *
     * @param id
     *          A String with the identifier of the article to be removed.
     * @return an Artifact with the artifact that has been removed from the
     *         index.
     */
    protected Artifact removeFromIndex(String id) {
      synchronized (index) {
        // Remove Artifact from the index.
        return index.remove(id);
      }
    }

    @Override
    public String toString() {
      synchronized (index) {
        return "[VolatileArtifactIndex index=" + index + "]";
      }
    }

    /**
     * Returns a boolean indicating whether this artifact index is ready.
     *
     * Always returns true in the violate implementation.
     */
    @Override
    public boolean isReady() {
        return getState() == ArtifactIndexState.READY;
    }
}
