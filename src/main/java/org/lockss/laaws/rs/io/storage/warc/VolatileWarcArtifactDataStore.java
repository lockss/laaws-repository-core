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

package org.lockss.laaws.rs.io.storage.warc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.archive.io.warc.WARCRecord;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.ArtifactDataFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.*;

/**
 * A volatile ("in-memory") implementation of WarcArtifactDataStore.
 */
public class VolatileWarcArtifactDataStore extends WarcArtifactDataStore<ArtifactIdentifier, ArtifactData, RepositoryArtifactMetadata> {
    private final static Log log = LogFactory.getLog(VolatileWarcArtifactDataStore.class);
    private Map<String, Map<String, Map<String, byte[]>>> repository;
    private Map<String, RepositoryArtifactMetadata> repositoryMetadata = new HashMap<>();

    /**
     * Constructor.
     */
    public VolatileWarcArtifactDataStore() {
        this.repository = new HashMap<>();
    }

    /**
     * Adds an artifact to this artifact store.
     *
     * @param artifactData
     *          The {@code ArtifactData} to add to this artifact store.
     * @return A representation of the artifact as it is now stored.
     * @throws IOException
     */
    @Override
    public Artifact addArtifactData(ArtifactData artifactData) throws IOException {
        if (artifactData == null)
            throw new IllegalArgumentException("Cannot add a null artifact");

        // Get artifact identifier
        ArtifactIdentifier artifactId = artifactData.getIdentifier();

        // Get the collection
        Map<String, Map<String, byte[]>> collection = repository.getOrDefault(artifactId.getCollection(), new HashMap<>());

        // Get the AU
        Map<String, byte[]> au = collection.getOrDefault(artifactId.getAuid(), new HashMap<>());

        try {
            // ByteArrayOutputStream to capture the WARC record
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            // Set unique artifactId
            artifactData.getIdentifier().setId(UUID.randomUUID().toString());

            // Create and set the artifact's repository metadata
            RepositoryArtifactMetadata repoMetadata = new RepositoryArtifactMetadata(artifactId, false, false);
            repositoryMetadata.put(artifactId.getId(), repoMetadata);
            artifactData.setRepositoryMetadata(repoMetadata);

            // Write artifact as a WARC record stream to the OutputStream
            long bytesWritten = writeArtifactData(artifactData, baos);

            // Store artifact
            au.put(artifactId.getId(), baos.toByteArray());
            collection.put(artifactId.getAuid(), au);
            repository.put(artifactId.getCollection(), collection);
        } catch (HttpException e) {
            log.error(String.format("Caught an HttpException while attempt to write an ArtifactData to an OutputStream: %s", e.getMessage()));
            throw new IOException(e);
        }

        // Construct volatile storage URL for this WARC record
        String storageUrl = String.format(
                "volatile:///%s/%s/%s/%s/%s",
                artifactId.getCollection(),
                artifactId.getAuid(),
                URLEncoder.encode(artifactId.getUri(), "UTF-8"),
                artifactId.getVersion(),
                artifactId.getId()
        );

        // Set the artifact's storage URL
        artifactData.setStorageUrl(storageUrl);

        // Create an Artifact to return
        Artifact artifact = new Artifact(
                artifactId,
                false,
                storageUrl,
                artifactData.getContentLength(),
                artifactData.getContentDigest()
        );

        return artifact;
    }

    /**
     * Retrieves an artifact from this artifact store.
     *
     * @param artifact
     *          An ArtifactIndex that encodes information about the artifact to retrieve from this store.
     * @return The {@code ArtifactData} referred to by the Artifact.
     * @throws IOException
     */
    @Override
    public ArtifactData getArtifactData(Artifact artifact) throws IOException {
        // Cannot work with a null Artifact
        if (artifact == null)
            throw new IllegalArgumentException("Artifact used to reference artifact cannot be null");

        // ArtifactData to return; defaults to null if one could not be found
        ArtifactData artifactData = null;

        // Get the map representing an artifact collection
        if (repository.containsKey(artifact.getCollection())) {
            // Get the collection of artifacts
            Map<String, Map<String, byte[]>> collection = repository.get(artifact.getCollection());

            // Get the map representing an AU from collection
            if (collection.containsKey(artifact.getAuid())) {
                Map<String, byte[]> au = collection.getOrDefault(artifact.getAuid(), new HashMap<>());

                // Retrieve the artifact's byte stream (artifact is encoded as a WARC record stream here)
                byte[] artifactBytes = au.get(artifact.getId());

                // Adapt byte array to ArtifactData
                if (artifactBytes != null) {
                    InputStream warcRecordStream = new ByteArrayInputStream(artifactBytes);

                    // Assemble a WARCRecord object using the WARC record bytestream in memory
                    WARCRecord record = new WARCRecord(
                            warcRecordStream,
                            null,
                            0,
                            true,
                            true
                    );

                    // Generate an artifact from the HTTP response stream
                    artifactData = ArtifactDataFactory.fromHttpResponseStream(record);

                    // Set ArtifactData properties
                    artifactData.setIdentifier(artifact.getIdentifier());
                    artifactData.setStorageUrl(artifact.getStorageUrl());
                    artifactData.setContentLength(artifact.getContentLength());
                    artifactData.setContentDigest(artifact.getContentDigest());
                    artifactData.setRepositoryMetadata(repositoryMetadata.get(artifact.getId()));
                }
            }
        }

        return artifactData;
    }

    /**
     * Updates and writes associated metadata of an artifact to this store.
     *
     * @param artifactId
     *          A (@code ArtifactIdentifier) that identifies the artifact to update.
     * @param artifactMetadata
     *          RepositoryArtifactMetadata update the artifact with, and write to the store.
     * @return A representation of the RepositoryArtifactMetadata as it is now stored.
     */
    public RepositoryArtifactMetadata updateArtifactMetadata(ArtifactIdentifier artifactId, RepositoryArtifactMetadata artifactMetadata) {
        repositoryMetadata.replace(artifactId.getId(), artifactMetadata);
        return repositoryMetadata.get(artifactId.getId());
    }

    /**
     * Commits an artifact to this artifact store.
     *
     * @param indexData
     *          A (@code ArtifactIdentifier) that identifies the artifact to commit and store permanently.
     * @return A {@code RepositoryArtifactMetadata} updated to indicate the new commit status as it is now stored.
     */
    @Override
    public RepositoryArtifactMetadata commitArtifactData(Artifact indexData) {
        if (indexData == null)
            throw new IllegalArgumentException("indexData cannot be null");

        RepositoryArtifactMetadata metadata = repositoryMetadata.get(indexData.getId());
        metadata.setCommitted(true);

        // TODO: Use updateArtifactMetadata
        repositoryMetadata.replace(indexData.getId(), metadata);
        return metadata;
    }

    /**
     * Removes an artifact from this store.
     *
     * @param artifactInfo
     *          A {@code Artifact} referring to the artifact to remove from this store.
     * @return A {@code RepositoryArtifactMetadata} updated to indicate the deleted status of this artifact.
     */
    @Override
    public RepositoryArtifactMetadata deleteArtifactData(Artifact artifactInfo) {
        if (artifactInfo == null)
            throw new IllegalArgumentException("artifactInfo cannot be null");

        Map<String, Map<String, byte[]>> collection = repository.get(artifactInfo.getCollection());
        Map<String, byte[]> au = collection.get(artifactInfo.getAuid());
        au.remove(artifactInfo.getId());

        // TODO: Use updateArtifactMetadata
        RepositoryArtifactMetadata metadata = repositoryMetadata.get(artifactInfo.getId());
        metadata.setDeleted(true);
        repositoryMetadata.replace(artifactInfo.getId(), metadata);

        repositoryMetadata.remove(artifactInfo.getId());
        return metadata;
    }
}
