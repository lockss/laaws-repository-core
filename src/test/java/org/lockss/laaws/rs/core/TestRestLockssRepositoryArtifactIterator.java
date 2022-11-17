/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
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

import org.junit.Before;
import org.junit.Test;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.util.rest.RestUtil;
import org.lockss.util.test.LockssTestCase5;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.NoSuchElementException;

import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * Test class for org.lockss.laaws.rs.core.RestLockssRepositoryArtifactIterator.
 *
 * @author Fernando García-Loygorri
 */
public class TestRestLockssRepositoryArtifactIterator extends LockssTestCase5 {
  private final static String BASEURL = "http://localhost:24610";
  private final static String NS1 = "ns1";
  private final static String AUID1 = "auId";
  private RestTemplate restTemplate;
  private MockRestServiceServer mockServer;
  private String endpoint;
  private UriComponentsBuilder builder;
  private RestLockssRepositoryArtifactIterator repoIterator;

  /**
   * Set up code to be run before each test.
   */
  @Before
  public void makeRepoIterator() {
    restTemplate = RestUtil.getRestTemplate();
    mockServer = MockRestServiceServer.createServer(restTemplate);
    endpoint = String.format("%s/aus/%s/artifacts", BASEURL, AUID1);
    builder = UriComponentsBuilder.fromHttpUrl(endpoint)
        .queryParam("namespace", NS1);
  }

  /**
   * Runs the tests for an empty repository.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testEmptyRepository() throws Exception {
    mockServer.expect(requestTo(endpoint + "?namespace=" + NS1)).andExpect(method(HttpMethod.GET))
    .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}",
	MediaType.APPLICATION_JSON));

    repoIterator =
	new RestLockssRepositoryArtifactIterator(restTemplate, builder);
    mockServer.verify();

    assertFalse(repoIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> {repoIterator.next();});
  }

  /**
   * Runs the tests for a populated repository.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testPopulatedRepository() throws Exception {
    mockServer.expect(requestTo(endpoint + "?namespace="+ NS1)).andExpect(method(HttpMethod.GET))
    .andRespond(withSuccess("{\"artifacts\":["
	+ "{\"uuid\":\"1\",\"version\":3},{\"uuid\":\"2\",\"version\":2},"
	+ "{\"uuid\":\"3\",\"version\":1}], \"pageInfo\":{}}",
	MediaType.APPLICATION_JSON));

    repoIterator =
	new RestLockssRepositoryArtifactIterator(restTemplate, builder);
    mockServer.verify();

    assertTrue(repoIterator.hasNext());
    Artifact artifact = repoIterator.next();
    assertEquals("1", artifact.getUuid());
    assertEquals(3, artifact.getVersion().intValue());
    assertTrue(repoIterator.hasNext());
    artifact = repoIterator.next();
    assertEquals("2", artifact.getUuid());
    assertEquals(2, artifact.getVersion().intValue());
    assertTrue(repoIterator.hasNext());
    artifact = repoIterator.next();
    assertEquals("3", artifact.getUuid());
    assertEquals(1, artifact.getVersion().intValue());
    assertFalse(repoIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> {repoIterator.next();});
  }

  /**
   * Runs pagination tests.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testPagination() throws Exception {
    // First server call.
    mockServer.expect(requestTo(endpoint + "?namespace="+ NS1 +"&limit=2"))
    .andExpect(method(HttpMethod.GET))
    .andRespond(withSuccess("{\"artifacts\":["
	+ "{\"uuid\":\"1\",\"uri\":\"uriA\",\"version\":3},"
	+ "{\"uuid\":\"2\",\"uri\":\"uriB\",\"version\":2}], "
	+ "\"pageInfo\":{\"continuationToken\":\"ns1:auId:uriB:2:123456\"}}",
	MediaType.APPLICATION_JSON));

    // Second server call.
    mockServer.expect(requestTo(endpoint
	+ "?namespace="+ NS1 +"&limit=2&continuationToken=ns1:auId:uriB:2:123456"))
    .andExpect(method(HttpMethod.GET))
    .andRespond(withSuccess("{\"artifacts\":["
	+ "{\"uuid\":\"3\",\"uri\":\"uriC\",\"version\":9},"
	+ "{\"uuid\":\"4\",\"uri\":\"uriC\",\"version\":1}], "
	+ "\"pageInfo\":{}}",
	MediaType.APPLICATION_JSON));

    repoIterator =
	new RestLockssRepositoryArtifactIterator(restTemplate, builder, 2);

    assertTrue(repoIterator.hasNext());
    Artifact artifact = repoIterator.next();
    assertEquals("1", artifact.getUuid());
    assertEquals("uriA", artifact.getUri());
    assertEquals(3, artifact.getVersion().intValue());
    assertTrue(repoIterator.hasNext());
    artifact = repoIterator.next();
    assertEquals("2", artifact.getUuid());
    assertEquals("uriB", artifact.getUri());
    assertEquals(2, artifact.getVersion().intValue());
    assertTrue(repoIterator.hasNext());

    // Both server calls have happened.
    mockServer.verify();

    artifact = repoIterator.next();
    assertEquals("3", artifact.getUuid());
    assertEquals("uriC", artifact.getUri());
    assertEquals(9, artifact.getVersion().intValue());
    assertTrue(repoIterator.hasNext());
    artifact = repoIterator.next();
    assertEquals("4", artifact.getUuid());
    assertEquals("uriC", artifact.getUri());
    assertEquals(1, artifact.getVersion().intValue());
    assertFalse(repoIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> {repoIterator.next();});
  }
}
