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
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Test class for {@code org.lockss.laaws.rs.core.VolatileLockssRepository}
 */
public class TestVolatileLockssRepository {
    private final static Log log = LogFactory.getLog(TestVolatileLockssRepository.class);

    private ArtifactIdentifier aid1;
    private ArtifactIdentifier aid2;
    private ArtifactIdentifier aid3;
    private RepositoryArtifactMetadata md1;
    private RepositoryArtifactMetadata md2;
    private RepositoryArtifactMetadata md3;
    private ArtifactData artifactData1;
    private ArtifactData artifactData2;
    private ArtifactData artifactData3;

    private UUID uuid;
    private VolatileLockssRepository repo;
    private StatusLine httpStatus;

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
        aid3 = new ArtifactIdentifier("id3", "coll1", "auid1", "uri2", "v1");

        md1 = new RepositoryArtifactMetadata(aid1, false, false);
        md2 = new RepositoryArtifactMetadata(aid2, true, false);
        md3 = new RepositoryArtifactMetadata(aid1, false, false);

        artifactData1 = new ArtifactData(aid1, null, new ByteArrayInputStream("bytes1".getBytes()), httpStatus, "surl1", md1);
        artifactData2 = new ArtifactData(aid2, null, new ByteArrayInputStream("bytes2".getBytes()), httpStatus, "surl2", md2);
        artifactData3 = new ArtifactData(aid3, null, new ByteArrayInputStream("bytes3".getBytes()), httpStatus, "surl3", md3);

        repo = new VolatileLockssRepository();
    }

    @Test
    public void addArtifact() {
        try {
            // Attempt adding a null artifact and expect IllegalArgumentException to the thrown
            repo.addArtifact(null);
            fail("Attempted to add a null artifact and was expecting IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            String expectedErrMsg = "ArtifactData is null";
            assertEquals(expectedErrMsg, e.getMessage());
        } catch (IOException e) {
            fail("Expected IllegalArgumentException but got IOException");
        }

        try {
            // Add an artifact to the repository
            Artifact artifact = repo.addArtifact(artifactData1);
            assertNotNull(artifact);
            String artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertFalse(repo.isArtifactCommitted(artifactId));
            assertTrue(repo.artifactExists(artifactId));

//            ArtifactData artifact = repo.getArtifact("coll1", artifactId);
//            assertNotNull(artifact);
//            assertEquals(artifactData1.getIdentifier().getId(), artifact.getIdentifier().getId());
//
//            assertFalse(repo.getCollectionIds().hasNext());
//            assertFalse(repo.getAuIds("coll1").hasNext());
//
//            repo.commitArtifact("coll1", artifactId);
//            assertFalse(repo.getAuIds("coll2").hasNext());
//            assertTrue(repo.getAuIds("coll1").hasNext());
//            Iterator<String> collectionIds = repo.getCollectionIds();
//            assertEquals("coll1", collectionIds.next());
//            assertFalse(collectionIds.hasNext());
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void getArtifact() {
        try {
            // Add the artifact and verify we get back an artifact ID
            Artifact artifact = repo.addArtifact(artifactData1);

            assertNotNull(artifact);
            String artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertTrue(repo.artifactExists(artifactId));

            // Retrieve the artifact and verify we get back the same artifact
            ArtifactData artifactData = repo.getArtifactData("coll1", artifactId);
            assertNotNull(artifactData);
            assertEquals(artifactData1.getIdentifier().getId(), artifactData.getIdentifier().getId());
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void commitArtifact() {
        try {
            // Attempt to commit to a null collection
            repo.commitArtifact(null, null);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            // OK
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            // Attempt to commit to a null collection
            repo.commitArtifact(null, "doesntMatter");
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            // OK
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            // Attempt to commit to a null artifact id
            repo.commitArtifact("doesntMatter", null);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            // OK
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            // Add an artifact and verify that it is not committed
            Artifact artifact = repo.addArtifact(artifactData1);
            assertNotNull(artifact);
            String artifactId = artifact.getId();
            assertFalse(repo.isArtifactCommitted(artifactId));

            // Commit the artifact and verify that it is committed
            repo.commitArtifact(artifactData1.getIdentifier().getCollection(), artifactId);
            assertTrue(repo.isArtifactCommitted(artifactId));
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void deleteArtifact() {
        final String expectedErrMsg = "Null collection ID or artifact ID";

        try {
            repo.deleteArtifact(null, null);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(expectedErrMsg, e.getMessage());
        } catch (IOException e) {
            fail("Expected IllegalArgumentException but got IOException");
        }

        try {
            repo.deleteArtifact(artifactData1.getIdentifier().getCollection(), null);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(expectedErrMsg, e.getMessage());
        } catch (IOException e) {
            fail("Expected IllegalArgumentException but got IOException");
        }

        String artifactId = null;

        try {
            // Attempt to add an artifact and verify it exists
            Artifact artifact = repo.addArtifact(artifactData1);
            assertNotNull(artifact);
            artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertTrue(repo.artifactExists(artifactId));
        } catch (IOException e) {
            fail("Expected IllegalArgumentException but got IOException");
        }

        try {
            repo.deleteArtifact(null, artifactId);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(expectedErrMsg, e.getMessage());
        } catch (IOException e) {
            fail("Expected IllegalArgumentException but got IOException");
        }

        try {
            // Delete the artifact and check that it doesn't exist
            repo.deleteArtifact(artifactData1.getIdentifier().getCollection(), artifactId);
            assertFalse(repo.artifactExists(artifactId));
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void artifactExists() {
        String expectedErrMsg = "Null or empty identifier";

        try {
            // Attempt to invoke an IllegalArgumentException
            repo.artifactExists(null);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(expectedErrMsg, e.getMessage());
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            // Attempt to invoke an IllegalArgumentException
            repo.artifactExists("");
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(expectedErrMsg, e.getMessage());
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            // Check for something that doesn't exist
            assertFalse(repo.artifactExists("nonExistentId"));

            // Add an artifact and verify it exists
            Artifact artifact = repo.addArtifact(artifactData1);
            assertNotNull(artifact);
            String artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertTrue(repo.artifactExists(artifactId));
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void isArtifactCommitted() {
        try {
            repo.isArtifactCommitted(null);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            // OK
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            repo.isArtifactCommitted("");
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            // OK
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        String artifactId = null;

        try {
            Artifact artifact = repo.addArtifact(artifactData1);
            assertNotNull(artifact);
            artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertTrue(repo.artifactExists(artifactId));
            assertFalse(repo.isArtifactCommitted(artifactId));

            repo.commitArtifact(artifactData1.getIdentifier().getCollection(), artifactId);
            assertTrue(repo.isArtifactCommitted(artifactId));
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void getCollectionIds() {
        try {
            // Nothing added yet
            Iterator<String> collectionIds = repo.getCollectionIds();
            assertNotNull(collectionIds);
            assertFalse(collectionIds.hasNext());

            // Add an artifact
            Artifact artifact = repo.addArtifact(artifactData1);
            assertNotNull(artifact);
            String artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertTrue(repo.artifactExists(artifactId));

            // ArtifactData is uncommitted so getCollectionIds() should return nothing
            collectionIds = repo.getCollectionIds();
            assertNotNull(collectionIds);
            assertFalse(repo.getCollectionIds().hasNext());

            // Commit artifact and check again
            repo.commitArtifact(artifactData1.getIdentifier().getCollection(), artifactId);
            assertTrue(repo.isArtifactCommitted(artifactId));
            assertTrue(repo.getCollectionIds().hasNext());
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void getAuIds() {
        try {
            Iterator<String> auids = repo.getAuIds(null);
            assertNotNull(auids);
            assertFalse(auids.hasNext());

            Artifact artifact = repo.addArtifact(artifactData1);
            assertNotNull(artifact);
            String artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertTrue(repo.artifactExists(artifactId));
            assertFalse(repo.isArtifactCommitted(artifactId));

            auids = repo.getAuIds(artifactData1.getIdentifier().getCollection());
            assertNotNull(auids);
            assertFalse(auids.hasNext());

            repo.commitArtifact(artifactData1.getIdentifier().getCollection(), artifactId);

            auids = repo.getAuIds(artifactData1.getIdentifier().getCollection());
            assertNotNull(auids);
            assertTrue(auids.hasNext());
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void getArtifactsInAU() {
        try {
            Iterator<Artifact> result = null;

            result = repo.getAllArtifactsAllVersions(null, null);
            assertNotNull(result);
            assertFalse(result.hasNext());

            result = repo.getAllArtifactsAllVersions(null, "unknown");
            assertNotNull(result);
            assertFalse(result.hasNext());

            repo.getAllArtifactsAllVersions("unknown", null);
            assertNotNull(result);
            assertFalse(result.hasNext());

            repo.getAllArtifactsAllVersions("unknown", "unknown");
            assertNotNull(result);
            assertFalse(result.hasNext());
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            assertNotNull(repo.addArtifact(artifactData1));
            assertNotNull(repo.addArtifact(artifactData2));

            Iterator<Artifact> result = null;

            result = repo.getAllArtifactsAllVersions(aid1.getCollection(), aid1.getAuid());
            assertNotNull(result);
            assertFalse(result.hasNext());

            repo.commitArtifact(aid1.getCollection(), aid1.getId());

            result = repo.getAllArtifactsAllVersions(aid1.getCollection(), aid1.getAuid());
            assertNotNull(result);
            assertTrue(result.hasNext());

            Artifact indexData = result.next();
            assertNotNull(indexData);
            assertFalse(result.hasNext());
            assertEquals(aid1.getId(), indexData.getIdentifier().getId());

        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void getArtifactsInAUWithURL() {

        try {
            assertNotNull(repo.addArtifact(artifactData1));
            assertNotNull(repo.addArtifact(artifactData2));
            assertNotNull(repo.addArtifact(artifactData3));

            Iterator<Artifact> result = null;

//            repo.commitArtifact(aid1.getCollection(), aid1.getId());

            result = repo.getAllArtifactsWithPrefixAllVersions(null, null, null);
            assertNotNull(result);
            assertFalse(result.hasNext());

            result = repo.getAllArtifactsWithPrefixAllVersions(aid1.getCollection(), null, null);
            assertNotNull(result);
            assertFalse(result.hasNext());

            result = repo.getAllArtifactsWithPrefixAllVersions(null, aid1.getAuid(), null);
            assertNotNull(result);
            assertFalse(result.hasNext());

            result = repo.getAllArtifactsWithPrefixAllVersions(null, null, "url");
            assertNotNull(result);
            assertFalse(result.hasNext());

            result = repo.getAllArtifactsWithPrefixAllVersions(aid1.getCollection(), aid1.getAuid(), null);
            assertNotNull(result);
            assertFalse(result.hasNext());

            result = repo.getAllArtifactsWithPrefixAllVersions(aid1.getCollection(), null,  "url");
            assertNotNull(result);
            assertFalse(result.hasNext());

            result = repo.getAllArtifactsWithPrefixAllVersions(null, aid1.getAuid(),  "url");
            assertNotNull(result);
            assertFalse(result.hasNext());

            result = repo.getAllArtifactsWithPrefixAllVersions(aid1.getCollection(), aid1.getAuid(),  "url");
            assertNotNull(result);
            assertFalse(result.hasNext());

        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            assertNotNull(repo.addArtifact(artifactData1));
            assertNotNull(repo.addArtifact(artifactData2));
            assertNotNull(repo.addArtifact(artifactData3));

            Iterator<Artifact> result = null;

            result = repo.getAllArtifactsWithPrefixAllVersions(aid1.getCollection(), aid1.getAuid(), aid1.getUri());
            assertNotNull(result);
            assertFalse(result.hasNext());

            repo.commitArtifact(aid1.getCollection(), aid1.getId());

            result = repo.getAllArtifactsWithPrefixAllVersions(aid1.getCollection(), aid1.getAuid(), aid1.getUri());
            assertNotNull(result);
            assertTrue(result.hasNext());

            Artifact indexData = result.next();
            assertNotNull(indexData);
            assertFalse(result.hasNext());
            assertEquals(aid1.getId(), indexData.getIdentifier().getId());
            assertEquals(aid1.getUri(), indexData.getIdentifier().getUri());

        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void getArtifactsInAUWithURLMatch() {
    }

}