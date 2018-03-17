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

package org.lockss.laaws.rs.io.storage;

import org.lockss.laaws.rs.model.*;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * ArtifactData storage interface.
 *
 * @param <ID> extends {@code ArtifactIdentifier}
 *            Implementation of ArtifactIdentifier to parameterize this interface with.
 * @param <A> extends {@code ArtifactData}
 *            Implementation of ArtifactData to parameterize this interfac with.
 * @param <MD> extends {@code RepositoryArtifactMetadata}
 *            Implementation of RepositoryArtifactMetadata to parameterize this interface with.
 */
public interface ArtifactStore<ID extends ArtifactIdentifier, A extends ArtifactData, MD extends RepositoryArtifactMetadata> {
    /**
     * Adds an artifact to this artifact store.
     *
     * @param artifact
     *          An {@code ArtifactData} to add to this artifact store.
     * @return Returns the {@code ArtifactData} as it is now recorded in this artifact store.
     * @throws IOException
     */
    A addArtifact(ArtifactData artifact) throws IOException;

    /**
     * Retrieves an artifact from this artifact store.
     *
     * @param indexedData
     *          An {@code Artifact} containing a reference to the artifact to receive from storage.
     * @return An {@code ArtifactData} retrieved from this artifact store.
     * @throws IOException
     * @throws URISyntaxException
     */
    A getArtifact(Artifact indexedData) throws IOException, URISyntaxException;

    /**
     * Updates an artifact's associated metadata in this artifact store.
     *
     * @param artifactId
     *          An {@code Artifact} containing a reference to the artifact to update in storage.
     * @param metadata
     *          An updated {@code ArtifactMetadata} write to this artifact store, for the referenced artifact.
     * @return ArtifactData metadata as it is now recorded in this artifact store.
     * @throws IOException
     */
    MD updateArtifactMetadata(ID artifactId, MD metadata) throws IOException;

    /**
     * Commits an artifact to this artifact store.
     *
     * @param artifactId
     *          An {@code Artifact} containing a reference to the artifact to update in storage.
     * @return A {@code RepositoryArtifactMetadata} representing the updated state of this artifact's repository metadata.
     * @throws IOException
     * @throws URISyntaxException
     */
    RepositoryArtifactMetadata commitArtifact(Artifact artifactId) throws IOException, URISyntaxException;

    /**
     * Permanently removes an artifact from this artifact store.
     *
     * @param indexedData
     *          An {@code Artifact} containing a reference to the artifact to remove from this artifact store.
     * @return A {@code RepositoryArtifactMetadata} with the final state of the removed artifact's repository metadata.
     * @throws IOException
     * @throws URISyntaxException
     */
    RepositoryArtifactMetadata deleteArtifact(Artifact indexedData) throws IOException, URISyntaxException;
}

