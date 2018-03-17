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
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.lockss.laaws.rs.core.VolatileLockssRepository;
import org.lockss.laaws.rs.io.storage.ArtifactStore;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.ArtifactIndexData;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;
import org.lockss.laaws.rs.util.ArtifactFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Test class for {org.lockss.laaws.rs.io.storage.warc.VolatileWarcArtifactStore}.
 */
public class TestVolatileWarcArtifactStore {
    private final static Log log = LogFactory.getLog(TestVolatileWarcArtifactStore.class);

    private ArtifactIdentifier aid1;
    private ArtifactIdentifier aid2;
    private RepositoryArtifactMetadata md1;
    private RepositoryArtifactMetadata md2;
    private Artifact artifact1;
    private Artifact artifact2;

    private UUID uuid;
    private StatusLine httpStatus;

    private ArtifactStore store;

    @Before
    public void setUp() throws Exception {
        uuid = UUID.randomUUID();

        httpStatus = new BasicStatusLine(
                new ProtocolVersion("HTTP", 1,1),
                200,
                "OK"
        );

        aid1 = new ArtifactIdentifier("id1", "coll1", "auid1", "uri1", "v1");
        aid2 = new ArtifactIdentifier(uuid.toString(), "coll2", "auid2", "uri2", "v2");

        md1 = new RepositoryArtifactMetadata(aid1, false, false);
        md2 = new RepositoryArtifactMetadata(aid2, true, false);

        artifact1 = new Artifact(aid1, null, new ByteArrayInputStream("bytes1".getBytes()), httpStatus, "surl1", md1);
        artifact2 = new Artifact(aid2, null, new ByteArrayInputStream("bytes2".getBytes()), httpStatus, "surl2", md2);

        store = new VolatileWarcArtifactStore();
    }

    @Test
    public void addArtifact() {
        String errorMsg = "Cannot add a null artifact";

        try {
            store.addArtifact(null);
            fail("Expected to IOException to be thrown");
        } catch (IllegalArgumentException e){
            assertEquals(errorMsg, e.getMessage());
        } catch (IOException e) {
            fail("Expected to IOException to be thrown");
        }

        try {
            Artifact artifact = store.addArtifact(artifact1);
            assertNotNull(artifact);
            assertEquals(artifact1, artifact);
        } catch (IOException e) {
            fail("Unexpected IOException caught");
        }
    }

    @Test
    public void getArtifact() {
        String errMsg = "ArtifactIndexData used to reference artifact cannot be null";

        try {
            // Attempt retrieving the artifact with a null argument
            store.getArtifact(null);
            fail("Expected IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(errMsg, e.getMessage());
        } catch (IOException e) {
            fail("Unexpected IOException caught");
        } catch (URISyntaxException e) {
            fail("Unexpected URISyntaxException caught");
        }

        try {
            // Attempt retrieving an artifact that doesn't exist
            ArtifactIndexData indexData = new ArtifactIndexData(
                    "badArtifactId",
                    "coll3",
                    "auid3",
                    "uri",
                    "1",
                    false,
                    "fake"
            );

            Artifact artifact = store.getArtifact(indexData);
            assertNull(artifact);
        } catch (IOException e) {
            fail("Unexpected IOException caught");
        } catch (URISyntaxException e) {
            fail("Unexpected URISyntaxException caught");
        }

        try {
            // Attempt a successful retrieval
            Artifact artifact = store.addArtifact(artifact1);
            assertNotNull(artifact);
            assertEquals(artifact1, artifact);

            ArtifactIndexData indexData = new ArtifactIndexData(
                    aid1.getId(),
                    aid1.getCollection(),
                    aid1.getAuid(),
                    aid1.getUri(),
                    aid1.getVersion(),
                    false,
                    artifact.getStorageUrl()
            );

            artifact = store.getArtifact(indexData);
            assertNotNull(artifact);
            assertEquals(artifact1.getIdentifier().getId(), artifact.getIdentifier().getId());
        } catch (IOException e) {
            fail("Unexpected IOException caught");
        } catch (URISyntaxException e) {
            fail("Unexpected URISyntaxException caught");
        }
    }

    @Test
    public void updateArtifactMetadata() {
        try {
            store.addArtifact(artifact1);
            RepositoryArtifactMetadata md1updated = new RepositoryArtifactMetadata(aid1, true, false);
            RepositoryArtifactMetadata metadata = store.updateArtifactMetadata(aid1, md1updated);
            assertNotNull(metadata);

            assertEquals(md1updated.getArtifactId(), metadata.getArtifactId());
            assertTrue(metadata.isCommitted());

        } catch (IOException e) {
            fail("Unexpected IOException caught");
        }
    }

    @Test
    public void commitArtifact() {
        String errMsg = "indexData cannot be null";

        try {
            store.commitArtifact(null);
            fail("Expected IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(errMsg, e.getMessage());
        } catch (IOException e) {
            fail("Unexpected IOException caught");
        } catch (URISyntaxException e) {
            fail("Unexpected URISyntaxException caught");
        }

        try {
            // Add an uncommitted artifact
            Artifact artifact = store.addArtifact(artifact1);
            assertNotNull(artifact);
            assertFalse(artifact.getRepositoryMetadata().isCommitted());

            // Create an ArtifactIndexData to simulate one retrieved for this artifact from an artifact index
            ArtifactIndexData indexData = new ArtifactIndexData(
                    aid1.getId(),
                    aid1.getCollection(),
                    aid1.getAuid(),
                    aid1.getUri(),
                    aid1.getVersion(),
                    false,
                    artifact.getStorageUrl()
            );

            // Commit artifact
            RepositoryArtifactMetadata metadata = store.commitArtifact(indexData);

            // Verify that the store has recorded it as committed
            assertTrue(metadata.isCommitted());
            assertTrue(store.getArtifact(indexData).getRepositoryMetadata().isCommitted());
        } catch (IOException e) {
            fail("Unexpected IOException caught");
        } catch (URISyntaxException e) {
            fail("Unexpected URISyntaxException caught");
        }
    }

    @Test
    public void deleteArtifact() {
        String errMsg = "artifactInfo cannot be null";

        try {
            store.deleteArtifact(null);
            fail("Expected IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(errMsg, e.getMessage());
        } catch (IOException e) {
            fail("Unexpected IOException caught");
        } catch (URISyntaxException e) {
            fail("Unexpected URISyntaxException caught");
        }

        try {
            // Add an artifact
            Artifact artifact = store.addArtifact(artifact1);
            assertNotNull(artifact);
            assertFalse(artifact.getRepositoryMetadata().isDeleted());

            // Create an ArtifactIndexData to simulate one retrieved for this artifact from an artifact index
            ArtifactIndexData indexData = new ArtifactIndexData(
                    aid1.getId(),
                    aid1.getCollection(),
                    aid1.getAuid(),
                    aid1.getUri(),
                    aid1.getVersion(),
                    false,
                    artifact.getStorageUrl()
            );

            // Delete the artifact from the artifact store
            RepositoryArtifactMetadata lastMetadata = store.deleteArtifact(indexData);

            // Verify that the repository metadata reflects the artifact is deleted
            assertTrue(lastMetadata.isDeleted());
            // And verify we get a null when trying to retrieve it after delete
            assertNull(store.getArtifact(indexData));
        } catch (IOException e) {
            fail("Unexpected IOException caught");
        } catch (URISyntaxException e) {
            fail("Unexpected URISyntaxException caught");
        }
    }
}