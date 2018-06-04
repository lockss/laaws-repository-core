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

import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.StatusLine;
import org.springframework.http.HttpHeaders;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * An {@code ArtifactData} serves as an atomic unit of data archived in the LOCKSS Repository.
 */
public class ArtifactData implements Comparable<ArtifactData> {
    private final static Log log = LogFactory.getLog(ArtifactData.class);
    protected static final String DEFAULT_DIGEST_ALGORITHM = "SHA-256";

    // Core artifact attributes
    private ArtifactIdentifier identifier;

    // Artifact data stream
    private InputStream artifactStream;
    private final CountingInputStream cis;
    private final DigestInputStream dis;


    // Artifact data properties
    private HttpHeaders artifactMetadata; // TODO: Switch from Spring to Apache?
    private StatusLine httpStatus;
    private long contentLength;
    private String contentDigest;

    // Internal repository metadata
    private RepositoryArtifactMetadata repositoryMetadata;
    private String storageUrl;

    /**
     * Constructor for artifact data that is not (yet) part of a LOCKSS repository.
     *
     * @param artifactMetadata
     *          A {@code HttpHeaders} containing additional key-value properties associated with this artifact data.
     * @param inputStream
     *          An {@code InputStream} containing the byte stream of this artifact.
     * @param responseStatus
     *          A {@code StatusLine} representing the HTTP response status if the data originates from a web server.
     */
    public ArtifactData(HttpHeaders artifactMetadata, InputStream inputStream, StatusLine responseStatus) {
        this(null, artifactMetadata, inputStream, responseStatus, null, null);
    }

    /**
     * Constructor for artifact data that has an identity relative to a LOCKSS repository, but has not yet been added to
     * an artifact store.
     *
     * @param identifier
     *          An {@code ArtifactIdentifier} for this artifact data.
     * @param artifactMetadata
     *          A {@code HttpHeaders} containing additional key-value properties associated with this artifact data.
     * @param inputStream
     *          An {@code InputStream} containing the byte stream of this artifact.
     * @param httpStatus
     *          A {@code StatusLine} representing the HTTP response status if the data originates from a web server.
     */
    public ArtifactData(ArtifactIdentifier identifier,
                        HttpHeaders artifactMetadata,
                        InputStream inputStream,
                        StatusLine httpStatus) {
        this(identifier, artifactMetadata, inputStream, httpStatus, null, null);
    }

    /**
     * Full constructor for artifact data.
     *
     * @param identifier
     *          An {@code ArtifactIdentifier} for this artifact data.
     * @param artifactMetadata
     *          A {@code HttpHeaders} containing additional key-value properties associated with this artifact data.
     * @param inputStream
     *          An {@code InputStream} containing the byte stream of this artifact.
     * @param httpStatus
     *          A {@code StatusLine} representing the HTTP response status if the data originates from a web server.
     * @param storageUrl
     *          A {@code String} URL pointing to the storage of this artifact data.
     * @param repoMetadata
     *          A {@code RepositoryArtifactMetadata} containing repository state information for this artifact data.
     */
    public ArtifactData(ArtifactIdentifier identifier,
                        HttpHeaders artifactMetadata,
                        InputStream inputStream,
                        StatusLine httpStatus,
                        String storageUrl,
                        RepositoryArtifactMetadata repoMetadata) {
        this.identifier = identifier;
        this.httpStatus = httpStatus;
        this.storageUrl = storageUrl;
        this.repositoryMetadata = repoMetadata;

        this.artifactMetadata = Objects.nonNull(artifactMetadata) ? artifactMetadata : new HttpHeaders();

        try {
            // Wrap the stream in a DigestInputStream
            cis = new CountingInputStream(inputStream);
            dis = new DigestInputStream(cis, MessageDigest.getInstance(DEFAULT_DIGEST_ALGORITHM));
            artifactStream = dis;
        } catch (NoSuchAlgorithmException e) {
            String errMsg = String.format(
                    "Unknown digest algorithm: %s; could not instantiate a MessageDigest", DEFAULT_DIGEST_ALGORITHM
            );

            log.error(errMsg);
            throw new RuntimeException(errMsg);
        }
    }

    /**
     * Returns additional key-value properties associated with this artifact.
     *
     * @return A {@code HttpHeaders} containing this artifact's additional properties.
     */
    public HttpHeaders getMetadata() {
        return artifactMetadata;
    }

    /**
     * Returns this artifact's byte stream in a one-time use {@code InputStream}.
     *
     * @return An {@code InputStream} containing this artifact's byte stream.
     */
    public InputStream getInputStream() {
        return artifactStream;
    }

    /**
     * Returns this artifact's HTTP response status if it originated from a web server.
     *
     * @return A {@code StatusLine} containing this artifact's HTTP response status.
     */
    public StatusLine getHttpStatus() {
        return this.httpStatus;
    }

    /**
     * Return this artifact data's artifact identifier.
     *
     * @return An {@code ArtifactIdentifier}.
     */
    public ArtifactIdentifier getIdentifier() {
        return this.identifier;
    }

    /**
     * Sets an artifact identifier for this artifact data.
     *
     * @param identifier
     *          An {@code ArtifactIdentifier} for this artifact data.
     * @return This {@code ArtifactData} with its identifier set to the one provided.
     */
    public ArtifactData setIdentifier(ArtifactIdentifier identifier) {
        this.identifier = identifier;
        return this;
    }

    /**
     * Returns the repository state information for this artifact data.
     *
     * @return A {@code RepositoryArtifactMetadata} containing the repository state information for this artifact data.
     */
    public RepositoryArtifactMetadata getRepositoryMetadata() {
        return repositoryMetadata;
    }

    /**
     * Sets the repository state information for this artifact data.
     *
     * @param metadata
     *          A {@code RepositoryArtifactMetadata} containing the repository state information for this artifact.
     * @return
     */
    public ArtifactData setRepositoryMetadata(RepositoryArtifactMetadata metadata) {
        this.repositoryMetadata = metadata;
        return this;
    }

    /**
     * Returns the location where the byte stream for this artifact data can be found.
     *
     * @return A {@code String} containing the storage of this artifact data.
     */
    public String getStorageUrl() {
      return storageUrl;
    }

    /**
     * Sets the location where the byte stream for this artifact data can be found.
     * @param storageUrl
     *          A {@code String} containing the location of this artifact data.
     */
    public void setStorageUrl(String storageUrl) {
      this.storageUrl = storageUrl;
    }

    /**
     * Implements {@code Comparable<ArtifactData>} so that sets of {@code ArtifactData} can be ordered.
     *
     * There may be a better canonical order but for now, this defers to implementations of ArtifactIdentifier.
     *
     * @param other
     *          Another {@code ArtifactData} to compare against.
     * @return An {@code int} denoting the order of this artifact, relative to another.
     */
    @Override
    public int compareTo(ArtifactData other) {
        return this.getIdentifier().compareTo(other.getIdentifier());
    }

    public String getContentDigest() {
        return contentDigest;
    }

    public void setContentDigest(String contentDigest) {
        this.contentDigest = contentDigest;
    }

    public long getContentLength() {
        return contentLength;
    }

    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }

    @Override
    public String toString() {
        return "[ArtifactData identifier=" + identifier + ", artifactMetadata="
            + artifactMetadata + ", httpStatus=" + httpStatus
            + ", repositoryMetadata=" + repositoryMetadata + ", storageUrl="
            + storageUrl + ", contentDigest=" + getContentDigest()
            + ", contentLength=" + getContentLength() + "]";
    }

    public long getBytesRead() {
        return cis.getByteCount();
    }

    public MessageDigest getMessageDigest() {
        return dis.getMessageDigest();
    }
}
