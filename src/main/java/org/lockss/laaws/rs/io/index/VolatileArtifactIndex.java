/*
 * Copyright (c) 2017-2018, Board of Trustees of Leland Stanford Jr. University,
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.util.ArtifactComparators;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.Artifact;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ArtifactData index implemented in memory, not persisted.
 */
public class VolatileArtifactIndex implements ArtifactIndex {
    private final static Log log = LogFactory.getLog(VolatileArtifactIndex.class);

    // Map from artifact ID to Artifact
    private Map<String, Artifact> index = new LinkedHashMap<>();

    /**
     * Adds an artifact to the index.
     * 
     * @param artifactData
     *          An ArtifactData with the artifact to be added to the index,.
     * @return an Artifact with the artifact indexing data.
     */
    @Override
    public Artifact indexArtifact(ArtifactData artifactData) {
        if (artifactData == null) {
          throw new IllegalArgumentException("Null artifact");
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

        // Add Artifact to the index
        index.put(id, artifact);

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
          throw new IllegalArgumentException("Null or empty identifier");
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
          throw new IllegalArgumentException("Null or empty identifier");
        }
        Artifact indexedData = index.get(artifactId);

        if (indexedData != null) {
          indexedData.setCommitted(true);
        }

        return indexedData;
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

      synchronized (this) {
        if (index.remove(artifactId) != null) {
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
          throw new IllegalArgumentException("Null or empty identifier");
        }
        return index.containsKey(artifactId);
    }

    /**
     * Provides the collection identifiers of the committed artifacts in the index.
     *
     * @return An {@code Iterator<String>} with the index committed artifacts
     * collection identifiers.
     */
    @Override
    public Iterable<String> getCollectionIds() {
        Stream<Artifact> artifactStream = index.values().stream();
        Stream<Artifact> committedArtifacts = artifactStream.filter(x -> x.getCommitted());
        Map<String, List<Artifact>> collections = committedArtifacts.collect(Collectors.groupingBy(Artifact::getCollection));

        // Sort the collection IDs for return
        List<String> collectionIds = new ArrayList(collections.keySet());
        Collections.sort(collectionIds);

        // Interface requires an iterator since this list could be very large in other implementations
        return IteratorUtils.asIterable(collectionIds.iterator());

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
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);

        return IteratorUtils.asIterable(
                index.values().stream().filter(query.build()).map(x -> x.getAuid()).distinct().sorted().iterator()
        );
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
    public Iterable<Artifact> getAllArtifacts(String collection, String auid) throws IOException {
        ArtifactPredicateBuilder q = new ArtifactPredicateBuilder();
        q.filterByCommitStatus(true);
        q.filterByCollection(collection);
        q.filterByAuid(auid);

        // Filter, then group the Artifacts by URI, and pick the Artifacts with max version from each group
        Map<String, Optional<Artifact>> result = index.values().stream().filter(q.build()).collect(
                Collectors.groupingBy(Artifact::getUri, Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion)))
        );

        // Return an iterator over the artifact from each group (one per URI), after sorting them by artifact URI then
        // descending version.
        return IteratorUtils.asIterable(result.values().stream().filter(Optional::isPresent).map(x -> x.get())
                .sorted(ArtifactComparators.BY_URI_SLASH_FIRST).iterator());
    }

    /**
     * Returns the artifacts of all committed versions of all URLs, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all version of all URLs in an AU.
     */
    @Override
    public Iterable<Artifact> getAllArtifactsAllVersions(String collection, String auid) {
        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);
        query.filterByAuid(auid);

        // Apply the filter, sort by artifact URL then descending version, and return an iterator over the Artifacts
        return IteratorUtils.asIterable(index.values().stream().filter(query.build())
                .sorted(ArtifactComparators.BY_URI_SLASH_FIRST_BY_DECREASING_VERSION).iterator());
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
    public Iterable<Artifact> getAllArtifactsWithPrefix(String collection, String auid, String prefix) throws IOException {
        ArtifactPredicateBuilder q = new ArtifactPredicateBuilder();
        q.filterByCommitStatus(true);
        q.filterByCollection(collection);
        q.filterByAuid(auid);
        q.filterByURIPrefix(prefix);

        // Apply the filter, group the Artifacts by URL, then pick the Artifact with highest version from each group
        Map<String, Optional<Artifact>> result = index.values().stream().filter(q.build()).collect(
                Collectors.groupingBy(Artifact::getUri, Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion)))
        );

        // Return an iterator over the artifact from each group (one per URI), after sorting them by artifact URI then
        // descending version.
        return IteratorUtils.asIterable(result.values().stream().filter(Optional::isPresent).map(x -> x.get())
                .sorted(ArtifactComparators.BY_URI_SLASH_FIRST).iterator());
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
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matchign a
     *         prefix from an AU.
     */
    @Override
    public Iterable<Artifact> getAllArtifactsWithPrefixAllVersions(String collection, String auid, String prefix) {
        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);
        query.filterByAuid(auid);
        query.filterByURIPrefix(prefix);

        // Apply filter then sort the resulting Artifacts by URL and descending version
        return IteratorUtils.asIterable(index.values().stream().filter(query.build())
                .sorted(ArtifactComparators.BY_URI_SLASH_FIRST_BY_DECREASING_VERSION).iterator());
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
    public Iterable<Artifact> getArtifactAllVersions(String collection, String auid, String url) {
        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);
        query.filterByAuid(auid);
        query.filterByURIMatch(url);

        // Apply filter then sort the resulting Artifacts by URL and descending version
        return IteratorUtils.asIterable(index.values().stream().filter(query.build())
                .sorted(ArtifactComparators.BY_DECREASING_VERSION).iterator());
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

        // Apply filter
        Iterator<Artifact> result = index.values().stream().filter(q.build()).iterator();

        if (!result.hasNext()) {
          return null;
        }
        Artifact ret = result.next();
        
        // There should be only one matching artifact
        if (result.hasNext()) { // awful hack
          int i = 1;
          while (result.hasNext()) { ++i; result.next(); }
            log.error(
                String.format("Found %d artifacts having the same (Collection, AUID, URL, Version)", i)
            );
            // TODO: Should we throw IllegalStateException?
        }

        return ret;
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

        return index.values().stream().filter(q.build()).mapToLong(artifact -> artifact.getContentLength()).sum();
    }
}
