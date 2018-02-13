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

import org.apache.http.StatusLine;
import org.springframework.http.HttpHeaders;

import java.io.*;

public class Artifact implements Comparable<Artifact> {
    // Core artifact attributes
    private ArtifactIdentifier identifier;
    private InputStream artifactStream;

    // Metadata
    private HttpHeaders artifactMetadata; // TODO: Switch from Spring to Apache?
    private StatusLine httpStatus;
    private RepositoryArtifactMetadata repositoryMetadata;

    public Artifact(HttpHeaders artifactMetadata, InputStream inputStream, StatusLine responseStatus) {
        this(null, artifactMetadata, inputStream, responseStatus, null);
    }

    public Artifact(ArtifactIdentifier identifier, HttpHeaders artifactMetadata, InputStream inputStream, StatusLine httpStatus) {
        this(identifier, artifactMetadata, inputStream, httpStatus, null);
    }

    public Artifact(ArtifactIdentifier identifier, HttpHeaders artifactMetadata, InputStream inputStream, StatusLine httpStatus, RepositoryArtifactMetadata repoMetadata) {
        this.identifier = identifier;
        this.artifactMetadata = artifactMetadata;
        this.artifactStream = inputStream;
        this.httpStatus = httpStatus;
        this.repositoryMetadata = repoMetadata;
    }

    public HttpHeaders getMetadata() {
        return artifactMetadata;
    }

    public InputStream getInputStream() {
        return artifactStream;
    }

    public StatusLine getHttpStatus() {
        return this.httpStatus;
    }

    public ArtifactIdentifier getIdentifier() {
        return this.identifier;
    }

    public Artifact setIdentifier(ArtifactIdentifier identifier) {
        this.identifier = identifier;
        return this;
    }

    public RepositoryArtifactMetadata getRepositoryMetadata() {
        return repositoryMetadata;
    }

    public Artifact setRepositoryMetadata(RepositoryArtifactMetadata metadata) {
        this.repositoryMetadata = metadata;
        return this;
    }

    @Override
    public int compareTo(Artifact other) {
        // TODO: Need to discuss a canonical order with team - for now, defer to artifacts identifiers
        return this.getIdentifier().compareTo(other.getIdentifier());
    }
}
