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

import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.Artifact;

import java.io.IOException;

/**
 * The LOCKSS Repository API:
 *
 * This is the interface of the abstract LOCKSS repository service.
 */
public interface LockssRepository {
  
    /**
     * Adds an artifact to this LOCKSS repository.
     *
     * @param artifactData
     *          {@code ArtifactData} instance to add to this LOCKSS repository.
     * @return The artifact ID of the newly added artifact.
     * @throws IOException
     */
    Artifact addArtifact(ArtifactData artifactData) throws IOException;

    /**
     * Retrieves an artifact from this LOCKSS repository.
     *
     * @param artifact
     *          An artifact to retrieve from this repository.
     * @return The {@code ArtifactData} referenced by this artifact.
     * @throws IOException
     */
    default ArtifactData getArtifactData(Artifact artifact) throws IOException {
      return getArtifactData(artifact.getCollection(), artifact.getId());
    }

    /**
     * Retrieves an artifact from this LOCKSS repository.
     *
     * @param collection
     *          The collection ID of the artifact.
     * @param artifactId
     *          A {@code String} with the artifact ID of the artifact to retrieve from this repository.
     * @return The {@code ArtifactData} referenced by this artifact ID.
     * @throws IOException
     */
    ArtifactData getArtifactData(String collection,
                                 String artifactId)
        throws IOException;

    /**
     * Commits an artifact to this LOCKSS repository for permanent storage and inclusion in LOCKSS repository queries.
     *
     * @param artifact
     *          A {code String} containing the collection ID of the collection containing the artifact to commit.
     * @return An {@code Artifact} containing the updated artifact state information.
     * @throws IOException
     */
    default Artifact commitArtifact(Artifact artifact) throws IOException {
      return commitArtifact(artifact.getCollection(), artifact.getId());
    }

    /**
     * Commits an artifact to this LOCKSS repository for permanent storage and inclusion in LOCKSS repository queries.
     *
     * @param collection
     *          A {code String} containing the collection ID of the collection containing the artifact to commit.
     * @param artifactId
     *          A {@code String} with the artifact ID of the artifact to commit to the repository.
     * @return An {@code Artifact} containing the updated artifact state information.
     * @throws IOException
     */
    Artifact commitArtifact(String collection,
                            String artifactId)
        throws IOException;

    /**
     * Permanently removes an artifact from this LOCKSS repository.
     *
     * @param artifact
     *          The artifact to remove from this LOCKSS repository.
     * @throws IOException
     */
    default void deleteArtifact(Artifact artifact) throws IOException {
      deleteArtifact(artifact.getCollection(), artifact.getId());
    }

    /**
     * Permanently removes an artifact from this LOCKSS repository.
     *
     * @param collection
     *          A {code String} containing the collection ID containing the artifact to delete.
     * @param artifactId
     *          A {@code String} with the artifact ID of the artifact to remove from this LOCKSS repository.
     * @throws IOException
     */
    void deleteArtifact(String collection,
                        String artifactId)
        throws IOException;

    /**
     * Checks whether an artifact exists in this LOCKSS repository.
     *
     * @param artifactId
     *          A {@code String} containing the artifact ID to check.
     * @return A boolean indicating whether an artifact exists in this repository.
     */
    Boolean artifactExists(String collection, String artifactId) throws IOException;

    /**
     * Checks whether an artifact is committed to this LOCKSS repository.
     *
     * @param artifactId
     *          A {@code String} containing the artifact ID to check.
     * @return A boolean indicating whether the artifact is committed.
     */
    Boolean isArtifactCommitted(String collection, String artifactId) throws IOException;

    /**
     * Provides the collection identifiers of the committed artifacts in the index.
     *
     * @return An {@code Iterable<String>} with the index committed artifacts
     * collection identifiers.
     */
    Iterable<String> getCollectionIds() throws IOException;

    /**
     * Returns a list of Archival Unit IDs (AUIDs) in this LOCKSS repository collection.
     *
     * @param collection
     *          A {@code String} containing the LOCKSS repository collection ID.
     * @return A {@code Iterable<String>} iterating over the AUIDs in this LOCKSS repository collection.
     * @throws IOException
     */
    Iterable<String> getAuIds(String collection) throws IOException;

    /**
     * Returns the committed artifacts of the latest version of all URLs, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @return An {@code Iterable<Artifact>} containing the latest version of all URLs in an AU.
     * @throws IOException
     */
    Iterable<Artifact> getAllArtifacts(String collection,
                                       String auid)
        throws IOException;

    /**
     * Returns the committed artifacts of all versions of all URLs, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @return An {@code Iterable<Artifact>} containing the committed artifacts of all version of all URLs in an AU.
     */
    Iterable<Artifact> getAllArtifactsAllVersions(String collection,
                                                  String auid)
        throws IOException;

    /**
     * Returns the committed artifacts of the latest version of all URLs matching a prefix, from a specified Archival
     * Unit and collection.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @param prefix
     *          A {@code String} containing a URL prefix.
     * @return An {@code Iterable<Artifact>} containing the latest version of all URLs matching a prefix in an AU.
     * @throws IOException
     */
    Iterable<Artifact> getAllArtifactsWithPrefix(String collection,
                                                 String auid,
                                                 String prefix)
        throws IOException;

    /**
     * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified Archival Unit and
     * collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param prefix
     *          A String with the URL prefix.
     * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of all URLs matchign a
     *         prefix from an AU.
     */
    Iterable<Artifact> getAllArtifactsWithPrefixAllVersions(String collection,
                                                            String auid,
                                                            String prefix)
        throws IOException;

    /**
     * Returns the committed artifacts of all versions of a given URL, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A {@code String} with the collection identifier.
     * @param auid
     *          A {@code String} with the Archival Unit identifier.
     * @param url
     *          A {@code String} with the URL to be matched.
     * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of a given URL from an
     *         Archival Unit.
     */
    Iterable<Artifact> getArtifactAllVersions(String collection,
                                              String auid,
                                              String url)
        throws IOException;

    /**
     * Returns the artifact of the latest version of given URL, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @param url
     *          A {@code String} containing a URL.
     * @return The {@code Artifact} representing the latest version of the URL in the AU.
     * @throws IOException
     */
    Artifact getArtifact(String collection,
                         String auid,
                         String url)
        throws IOException;

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
    Artifact getArtifactVersion(String collection,
                                String auid,
                                String url,
                                Integer version)
        throws IOException;


    /**
     * Returns the size, in bytes, of AU in a collection.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @return A {@code Long} with the total size of the specified AU in bytes.
     */
    Long auSize(String collection, String auid) throws IOException;
}