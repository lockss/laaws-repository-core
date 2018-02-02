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

package org.lockss.laaws.rs.model;

/**
 * This class represents the LOCKSS Repository -specific metadata of an artifact, implemented as a JSON object.
 *
 * E.g., whether an artifact is committed.
 *
 */
public class RepositoryArtifactMetadata extends ArtifactMetadata {
    public static String LOCKSS_METADATA_ID = "lockss-repo";

    public static String REPOSITORY_COMMITTED_KEY = "committed";
    public static String REPOSITORY_DELETED_KEY = "deleted";

    @Override
    public String getMetadataId() {
        return this.LOCKSS_METADATA_ID;
    }

    /**
     * No-parameter constructor that uses default values.
     */
    public RepositoryArtifactMetadata(ArtifactIdentifier artifactId) {
        super(artifactId);

        // Set defaults
        setCommitted(false);
        setDeleted(false);
    }

    /**
     * Parameterized constructor.
     *
     * @param artifactId Artifact identifier for this metadata
     * @param committed Boolean indicating whether this artifact is committed
     * @param deleted Boolean indicating whether this artifact is deleted
     */
    public RepositoryArtifactMetadata(ArtifactIdentifier artifactId, boolean committed, boolean deleted) {
        this(artifactId);

        setCommitted(committed);
        setDeleted(deleted);
    }

    /**
     * See getCommitted().
     *
     * @return boolean representing whether the artifact is committed to the repository.
     */
    public boolean isCommitted() {
        return getCommitted();
    }

    /**
     * Returns a boolean representing whether the artifact is committed to the repository.
     *
     * @return boolean
     */
    public boolean getCommitted() {
        return this.getBoolean(REPOSITORY_COMMITTED_KEY);
    }

    /**
     * Sets the committed status in the internal JSON structure.
     *
     * @param committed Committed status of the artifact this metadata is associated to.
     */
    public void setCommitted(boolean committed) {
        this.put(REPOSITORY_COMMITTED_KEY, committed);
    }

    /**
     * See getDeleted().
     *
     * @return boolean representing whether the artifact is deleted from the repository.
     */
    public boolean isDeleted() {
        return getDeleted();
    }

    /**
     * Returns a boolean representing whether the artifact is deleted from the repository.
     *
     * @return boolean
     */
    public boolean getDeleted() {
        return this.getBoolean(REPOSITORY_DELETED_KEY);
    }

    /**
     * Sets the deleted status in the internal JSON structure.
     *
     * @param deleted Deleted status of the artifact this metadata is associated to.
     */
    public void setDeleted(boolean deleted) {
        this.put(REPOSITORY_DELETED_KEY, deleted);
    }

}
