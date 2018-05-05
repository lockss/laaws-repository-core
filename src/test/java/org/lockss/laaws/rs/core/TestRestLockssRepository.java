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

package org.lockss.laaws.rs.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.util.ArtifactConstants;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.net.URL;

import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import org.lockss.util.test.LockssTestCase5;

public class TestRestLockssRepository extends LockssTestCase5 {
    private final static Log log = LogFactory.getLog(TestRestLockssRepository.class);
    private final static String BASEURL = "http://localhost:32640";
    protected LockssRepository repository;
    protected MockRestServiceServer mockServer;

    @Before
    public void makeLockssRepository() throws Exception {
        RestTemplate restTemplate = new RestTemplate();
        mockServer = MockRestServiceServer.createServer(restTemplate);
        repository = new RestLockssRepository(new URL(BASEURL), restTemplate);
    }

    @Test
    public void testGetCollectionIds_empty() throws Exception {
        // Test with empty result
        mockServer.expect(requestTo(String.format("%s/collections", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("[]", MediaType.APPLICATION_JSON));

        Iterable<String> collectionIds = repository.getCollectionIds();
        mockServer.verify();

        assertNotNull(collectionIds);
        assertFalse(collectionIds.iterator().hasNext());
    }

    @Test
    public void testGetCollectionIds_success() throws Exception {
        // Test with valid result
        mockServer.expect(requestTo(String.format("%s/collections", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("[\"collection1\"]", MediaType.APPLICATION_JSON));

        Iterable<String> collectionIds = repository.getCollectionIds();
        mockServer.verify();

        assertNotNull(collectionIds);
        assertTrue(collectionIds.iterator().hasNext());
        assertEquals("collection1", collectionIds.iterator().next());
        assertFalse(collectionIds.iterator().hasNext());
    }

    @Test
    public void testGetCollectionIds_failure() throws Exception {
        // Test with server error.
        mockServer.expect(requestTo(String.format("%s/collections", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

        Iterable<String> collectionIds = repository.getCollectionIds();
        mockServer.verify();

        assertNull(collectionIds);
    }

    @Test
    public void testArtifactExists_false() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Boolean artifactExists = repository.artifactExists("collection1", "artifact1");
        mockServer.verify();

        assertNotNull(artifactExists);
        assertFalse(artifactExists);
    }

    @Test
    public void testArtifactExists_true() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withSuccess());

        Boolean artifactExists = repository.artifactExists("collection1", "artifact1");
        mockServer.verify();

        assertNotNull(artifactExists);
        assertTrue(artifactExists);
    }

    @Test
    public void testArtifactExists_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withServerError());

        Boolean artifactExists = repository.artifactExists("collection1", "artifact1");
        mockServer.verify();

        assertNotNull(artifactExists);
        assertFalse(artifactExists);
    }

    @Test
    public void testIsArtifactCommitted_missingheader() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withSuccess().headers(new HttpHeaders()));

        Boolean result = repository.isArtifactCommitted("collection1", "artifact1");
        mockServer.verify();
        assertNull(result);
    }

    @Test
    public void testIsArtifactCommitted_success() throws Exception {
        HttpHeaders mockHeaders = new HttpHeaders();
        mockHeaders.add(ArtifactConstants.ARTIFACT_STATE_COMMITTED, "true");

        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withSuccess().headers(mockHeaders));

        Boolean result = repository.isArtifactCommitted("collection1", "artifact1");
        mockServer.verify();
        assertTrue(result);
    }

    @Test
    public void testGetArtifact_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=badUrl&version=latest", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("[]", MediaType.APPLICATION_JSON));

        Artifact result = repository.getArtifact("collection1", "auid1", "badUrl");
        mockServer.verify();

        assertNull(result);
    }

    public void testGetArtifact_404() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=latest", BASEURL)))
                .andExpect(method(HttpMethod.GET))
	  .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Artifact result = repository.getArtifact("collection1", "auid1", "badUrl");
        mockServer.verify();

        assertNull(result);
    }
}
