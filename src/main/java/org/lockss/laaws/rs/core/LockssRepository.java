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

import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIndexData;

import java.io.IOException;
import java.util.Iterator;

/**
 * The LOCKSS Repository API:
 *
 * This is the interface of the abstract LOCKSS repository service.
 */
public interface LockssRepository {
    /**
     * Adds an artifact to the LOCKSS repository.
     *
     * @param artifact
     *          {@code Artifact} instance to add to the LOCKSS repository.
     * @return The artifact ID of the newly added artifact.
     * @throws IOException
     */
    String addArtifact(Artifact artifact) throws IOException;

    /**
     * Retrieves an artifact from the LOCKSS repository.
     *
     * @param artifactId
     *          A String with the Artifact ID of the artifact to retrieve from the repository.
     * @return An {@code Artifact} referenced by this artifact ID.
     * @throws IOException
     */
    Artifact getArtifact(String collection, String artifactId) throws IOException;

    /**
     * Commits an artifact to the LOCKSS repository for permanent storage and inclusion in LOCKSS repository queries.
     *
     * @param artifactId
     *          A String with the Artifact ID of the artifact to commit to the repository.
     * @return TODO
     * @throws IOException
     */
    ArtifactIndexData commitArtifact(String collection, String artifactId) throws IOException;

    /**
     * Permanently removes an artifact from the LOCKSS repository.
     *
     * @param artifactId
     *          A String with the Artifact ID of the artifact to remove from the LOCKSS repository.
     * @throws IOException
     */
    void deleteArtifact(String collection, String artifactId) throws IOException;

    /**
     * Returns a boolean indicating whether an artifact by an artifact ID exists in this LOCKSS repository.
     *
     * @param artifactId
     *          A String with the Artifact ID of the artifact to check for existence.
     * @return A boolean indicating whether an artifact exists in this repository.
     */
    boolean artifactExists(String artifactId) throws IOException;

    /**
     * Returns a boolean indicating whether an artifact is committed in this LOCKSS repository.
     *
     * @param artifactId
     *          Artifact ID of the artifact to check committed status.
     * @return A boolean indicating whether the artifact is committed.
     */
    boolean isArtifactCommitted(String artifactId) throws IOException;

    /**
     * Provides the collection identifiers of the committed artifacts in the
     * index.
     *
     * @return an {@code Iterator<String>} with the index committed artifacts
     * collection identifiers.
     */
    Iterator<String> getCollectionIds() throws IOException;

    /**
     * Returns an interator over the Archival Unit IDs (AUIDs) in this collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @return A {@code Iterator<String>} with the AUIDs in the collection.
     */
    Iterator<String> getAuIds(String collection) throws IOException;

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     *         artifacts in the collection that belong to the Archival Unit.
     */
    Iterator<ArtifactIndexData> getArtifactsInAU(String collection, String auid) throws IOException;

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain a URL with a given prefix.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param prefix
     *          A String with the URL prefix.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain a URL with the given prefix.
     */
    Iterator<ArtifactIndexData> getArtifactsInAUWithURL(String collection, String auid, String prefix) throws IOException;

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain an exact match of a URL.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param url
     *          A String with the URL to be matched.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain an exact match of a URL.
     */
    Iterator<ArtifactIndexData> getArtifactsInAUWithURLMatch(String collection,
                                                             String auid, String url) throws IOException;

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain a URL with a given prefix and that match a
     * given version.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param prefix
     *          A String with the URL prefix.
     * @param version
     *          A String with the version.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain a URL with the given prefix and that match the given
     *         version.
     */
    Iterator<ArtifactIndexData> getArtifactsInAUWithURL(String collection,
                                                        String auid, String prefix, String version) throws IOException;

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain an exact match of a URL and that match a
     * given version.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param url
     *          A String with the URL to be matched.
     * @param version
     *          A String with the version.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain an exact match of a URL and that match the given
     *         version.
     */
    Iterator<ArtifactIndexData> getArtifactsInAUWithURLMatch(String collection,
                                                             String auid, String url, String version) throws IOException;
}
