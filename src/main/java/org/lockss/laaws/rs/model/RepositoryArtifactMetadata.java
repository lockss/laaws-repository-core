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

package org.lockss.laaws.rs.model;

import org.json.JSONObject;

/**
 * Encapsulates the LOCKSS repository -specific metadata of an artifact. E.g., whether an artifact is committed.
 *
 */
public class RepositoryArtifactMetadata {
    public static String LOCKSS_METADATA_ID = "lockss-repo";

    public static final String LOCKSS_MD_ARTIFACTID_KEY = "artifactId";
    public static final String REPOSITORY_COMMITTED_KEY = "committed";
    public static final String REPOSITORY_DELETED_KEY = "deleted";

    private String artifactId;
    private boolean committed;
    private boolean deleted;

    /**
     * Constructor that takes JSON formatted as a String object.
     *
     * @param s JSON string
     */
    public RepositoryArtifactMetadata(String s) {
      JSONObject json = new JSONObject(s);

      artifactId = json.getString(LOCKSS_MD_ARTIFACTID_KEY);
      committed = json.getBoolean(REPOSITORY_COMMITTED_KEY);
      deleted = json.getBoolean(REPOSITORY_DELETED_KEY);
    }

    /**
     * Returns the metadata ID for this class of metadata.
     *
     * @return A {@code String} containing the metadata ID.
     */
//    @Override
    public String getMetadataId() {
        return LOCKSS_METADATA_ID;
    }

    /**
     * Constructor that uses default values.
     * 
     * @param artifactId
     *          An ArtifactIdentifier with the artifact identifying information.
     */
    public RepositoryArtifactMetadata(ArtifactIdentifier artifactId) {
        this.artifactId = artifactId.getId();
    }

    /**
     * Parameterized constructor.
     *
     * @param artifactId ArtifactData identifier for this metadata
     * @param committed Boolean indicating whether this artifact is committed
     * @param deleted Boolean indicating whether this artifact is deleted
     */
    public RepositoryArtifactMetadata(ArtifactIdentifier artifactId, boolean committed, boolean deleted) {
        this(artifactId);

        this.committed = committed;
        this.deleted = deleted;
    }

    /**
     * Returns the artifact ID this metadata belongs to.
     *
     * @return ArtifactData ID
     */
    public String getArtifactId() {
        return artifactId;
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
        return committed;
    }

    /**
     * Sets the committed status in the internal JSON structure.
     *
     * @param committed Committed status of the artifact this metadata is associated to.
     */
    public void setCommitted(boolean committed) {
        this.committed = committed;
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
        return deleted;
    }

    /**
     * Sets the deleted status in the internal JSON structure.
     *
     * @param deleted Deleted status of the artifact this metadata is associated to.
     */
    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    /**
     * Provides this object as a JSON object.
     * 
     * @return a JSONObject with the JSON version of this object.
     */
    public JSONObject toJson() {
      JSONObject json = new JSONObject();
      json.put(LOCKSS_MD_ARTIFACTID_KEY, artifactId);
      json.put(REPOSITORY_COMMITTED_KEY, committed);
      json.put(REPOSITORY_DELETED_KEY, deleted);

      return json;
    }
}
