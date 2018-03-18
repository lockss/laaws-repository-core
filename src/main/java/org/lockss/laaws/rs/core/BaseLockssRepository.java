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
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.VolatileArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactStore;
import org.lockss.laaws.rs.io.storage.warc.VolatileWarcArtifactStore;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Base implementation of the LOCKSS Repository service.
 */
public class BaseLockssRepository implements LockssRepository {
    private final static Log log = LogFactory.getLog(BaseLockssRepository.class);

    protected ArtifactStore store = null;
    protected ArtifactIndex index = null;

    /**
     * Constructor. By default, we spin up a volatile in-memory LOCKSS repository.
     */
    public BaseLockssRepository() {
        this(new VolatileArtifactIndex(), new VolatileWarcArtifactStore());
    }

    /**
     * Configures this LOCKSS repository with the provided artifact index and storage layers.
     *
     * @param index
     *          An instance of {@code ArtifactIndex}.
     * @param store
     *          An instance of {@code ArtifactStore}.
     */
    public BaseLockssRepository(ArtifactIndex index, ArtifactStore store) {
        this.index = index;
        this.store = store;
    }

    /**
     * Adds an artifact to this LOCKSS repository.
     *
     * @param artifactData
     *          {@code ArtifactData} instance to add to this LOCKSS repository.
     * @return The artifact ID of the newly added artifact.
     * @throws IOException
     */
    @Override
    public String addArtifact(ArtifactData artifactData) throws IOException {
        if (artifactData == null)
            throw new IllegalArgumentException("ArtifactData is null");

        ArtifactData storedArtifactData = store.addArtifact(artifactData);
        Artifact artifact = index.indexArtifact(storedArtifactData);
        return artifact.getId();
    }

    /**
     * Retrieves an artifact from this LOCKSS repository.
     *
     * @param artifactId
     *          A {@code String} with the artifact ID of the artifact to retrieve from this repository.
     * @return The {@code ArtifactData} referenced by this artifact ID.
     * @throws IOException
     */
    @Override
    public ArtifactData getArtifact(String collection, String artifactId) throws IOException {
        try {
            Artifact artifact = index.getArtifactIndexData(artifactId);
            if (artifact == null)
                return null;

            return store.getArtifact(index.getArtifactIndexData(artifactId));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    /**
     * Commits an artifact to this LOCKSS repository for permanent storage and inclusion in LOCKSS repository queries.
     *
     * @param collection
     *          A {code String} containing the collection ID containing the artifact to commit.
     * @param artifactId
     *          A {@code String} with the artifact ID of the artifact to commit to the repository.
     * @return An {@code Artifact} containing the updated artifact state information.
     * @throws IOException
     */
    @Override
    public Artifact commitArtifact(String collection, String artifactId) throws IOException {
        if ((collection == null) || (artifactId == null))
            throw new IllegalArgumentException("Null collection or artifactId");

        // Get artifact as it is currently
        Artifact artifact = index.getArtifactIndexData(artifactId);
        ArtifactData artifactData = null;

        // Record the changed status in store
        store.updateArtifactMetadata(artifact.getIdentifier(), new RepositoryArtifactMetadata(
                artifact.getIdentifier(),
                true,
                false
        ));

        // Update the commit status in index and return the updated artifact
        return index.commitArtifact(artifactId);
    }

    /**
     * Permanently removes an artifact from this LOCKSS repository.
     *
     * @param artifactId
     *          A {@code String} with the artifact ID of the artifact to remove from this LOCKSS repository.
     * @throws IOException
     */
    @Override
    public void deleteArtifact(String collection, String artifactId) throws IOException {
        if ((collection == null) || (artifactId == null))
            throw new IllegalArgumentException("Null collection ID or artifact ID");

        try {
            store.deleteArtifact(index.getArtifactIndexData(artifactId));
            index.deleteArtifact(artifactId);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    /**
     * Checks whether an artifact exists in this LOCKSS repository.
     *
     * @param artifactId
     *          A {@code String} containing the artifact ID to check.
     * @return A boolean indicating whether an artifact exists in this repository.
     */
    @Override
    public boolean artifactExists(String artifactId) throws IOException {
        return index.artifactExists(artifactId);
    }

    /**
     * Checks whether an artifact is committed to this LOCKSS repository.
     *
     * @param artifactId
     *          A {@code String} containing the artifact ID to check.
     * @return A boolean indicating whether the artifact is committed.
     */
    @Override
    public boolean isArtifactCommitted(String artifactId) throws IOException {
        Artifact artifact = index.getArtifactIndexData(artifactId);
        return artifact.getCommitted();
    }

    /**
     * Provides the collection identifiers of the committed artifacts in the
     * index.
     *
     * @return An {@code Iterator<String>} with the index committed artifacts
     * collection identifiers.
     */
    @Override
    public Iterator<String> getCollectionIds() throws IOException {
        return index.getCollectionIds();
    }

    /**
     * Returns an iterator over the Archival Unit IDs (AUIDs) in this collection.
     *
     * @param collection
     *          A {@code String} with the collection identifier.
     * @return A {@code Iterator<String>} with the AUIDs in the collection.
     */
    @Override
    public Iterator<String> getAuIds(String collection) throws IOException {
        return index.getAuIds(collection);
    }

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit.
     *
     * @param collection
     *          A {@code String} with the collection identifier.
     * @param auid
     *          A {@code String} with the Archival Unit identifier.
     * @return An {@code Iterator<Artifact>} with the committed
     *         artifacts in the collection that belong to the Archival Unit.
     */
    @Override
    public Iterator<Artifact> getArtifactsInAU(String collection, String auid) throws IOException {
        return index.getArtifactsInAU(collection, auid);
    }

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain a URL with a given prefix.
     *
     * @param collection
     *          A {@code String} with the collection identifier.
     * @param auid
     *          A {@code String} with the Archival Unit identifier.
     * @param prefix
     *          A {@code String} with the URL prefix.
     * @return An {@code Iterator<Artifact>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain a URL with the given prefix.
     */
    @Override
    public Iterator<Artifact> getArtifactsInAUWithURL(String collection, String auid, String prefix) throws IOException {
        return index.getArtifactsInAUWithURL(collection, auid, prefix);
    }

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain an exact match of a URL.
     *
     * @param collection
     *          A {@code String} with the collection identifier.
     * @param auid
     *          A {@code String} with the Archival Unit identifier.
     * @param url
     *          A {@code String} with the URL to be matched.
     * @return An {@code Iterator<Artifact>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain an exact match of a URL.
     */
    @Override
    public Iterator<Artifact> getArtifactsInAUWithURLMatch(String collection, String auid, String url) throws IOException {
        return index.getArtifactsInAUWithURLMatch(collection, auid, url);
    }

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain a URL with a given prefix and that match a
     * given version.
     *
     * @param collection
     *          A {@code String} with the collection identifier.
     * @param auid
     *          A {@code String} with the Archival Unit identifier.
     * @param prefix
     *          A {@code String} with the URL prefix.
     * @param version
     *          A {@code String} with the version.
     * @return an {@code Iterator<Artifact>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain a URL with the given prefix and that match the given
     *         version.
     */
    @Override
    public Iterator<Artifact> getArtifactsInAUWithURL(String collection, String auid, String prefix, String version) throws IOException {
        return index.getArtifactsInAUWithURL(collection, auid, prefix, version);
    }

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain an exact match of a URL and that match a
     * given version.
     *
     * @param collection
     *          A {@code String} with the collection identifier.
     * @param auid
     *          A {@code String} with the Archival Unit identifier.
     * @param url
     *          A {@code String} with the URL to be matched.
     * @param version
     *          A {@code String} with the version.
     * @return an {@code Iterator<Artifact>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain an exact match of a URL and that match the given
     *         version.
     */
    @Override
    public Iterator<Artifact> getArtifactsInAUWithURLMatch(String collection, String auid, String url, String version) throws IOException {
        return index.getArtifactsInAUWithURLMatch(collection, auid, url, version);
    }
}
