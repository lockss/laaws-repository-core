/*

Copyright (c) 2019-2020 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
package org.lockss.laaws.rs.core;

import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import java.util.NoSuchElementException;
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

/**
 * Test class for org.lockss.laaws.rs.core.RestLockssRepositoryArtifactIterator.
 * 
 * @author Fernando García-Loygorri
 */
public class TestRestLockssRepositoryArtifactIterator extends LockssTestCase5 {
  private final static String BASEURL = "http://localhost:24610";
  private final static String collectionId = "collId";
  private final static String auId = "auId";
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
    restTemplate = RestUtil.getSimpleFactoryRestTemplate(true);
    mockServer = MockRestServiceServer.createServer(restTemplate);
    endpoint = String.format("%s/collections/%s/aus/%s/artifacts", BASEURL,
	collectionId, auId);
    builder = UriComponentsBuilder.fromHttpUrl(endpoint);
  }

  /**
   * Runs the tests for an empty repository.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testEmptyRepository() throws Exception {
    mockServer.expect(requestTo(endpoint)).andExpect(method(HttpMethod.GET))
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
    mockServer.expect(requestTo(endpoint)).andExpect(method(HttpMethod.GET))
    .andRespond(withSuccess("{\"artifacts\":["
	+ "{\"id\":\"1\",\"version\":3},{\"id\":\"2\",\"version\":2},"
	+ "{\"id\":\"3\",\"version\":1}], \"pageInfo\":{}}",
	MediaType.APPLICATION_JSON));

    repoIterator =
	new RestLockssRepositoryArtifactIterator(restTemplate, builder);
    mockServer.verify();

    assertTrue(repoIterator.hasNext());
    Artifact artifact = repoIterator.next();
    assertEquals("1", artifact.getId());
    assertEquals(3, artifact.getVersion().intValue());
    assertTrue(repoIterator.hasNext());
    artifact = repoIterator.next();
    assertEquals("2", artifact.getId());
    assertEquals(2, artifact.getVersion().intValue());
    assertTrue(repoIterator.hasNext());
    artifact = repoIterator.next();
    assertEquals("3", artifact.getId());
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
    mockServer.expect(requestTo(endpoint + "?limit=2"))
    .andExpect(method(HttpMethod.GET))
    .andRespond(withSuccess("{\"artifacts\":["
	+ "{\"id\":\"1\",\"uri\":\"uriA\",\"version\":3},"
	+ "{\"id\":\"2\",\"uri\":\"uriB\",\"version\":2}], "
	+ "\"pageInfo\":{\"continuationToken\":\"collId:auId:uriB:2:123456\"}}",
	MediaType.APPLICATION_JSON));

    // Second server call.
    mockServer.expect(requestTo(endpoint
	+ "?limit=2&continuationToken=collId:auId:uriB:2:123456"))
    .andExpect(method(HttpMethod.GET))
    .andRespond(withSuccess("{\"artifacts\":["
	+ "{\"id\":\"3\",\"uri\":\"uriC\",\"version\":9},"
	+ "{\"id\":\"4\",\"uri\":\"uriC\",\"version\":1}], "
	+ "\"pageInfo\":{}}",
	MediaType.APPLICATION_JSON));

    repoIterator =
	new RestLockssRepositoryArtifactIterator(restTemplate, builder, 2);

    assertTrue(repoIterator.hasNext());
    Artifact artifact = repoIterator.next();
    assertEquals("1", artifact.getId());
    assertEquals("uriA", artifact.getUri());
    assertEquals(3, artifact.getVersion().intValue());
    assertTrue(repoIterator.hasNext());
    artifact = repoIterator.next();
    assertEquals("2", artifact.getId());
    assertEquals("uriB", artifact.getUri());
    assertEquals(2, artifact.getVersion().intValue());
    assertTrue(repoIterator.hasNext());

    // Both server calls have happened.
    mockServer.verify();

    artifact = repoIterator.next();
    assertEquals("3", artifact.getId());
    assertEquals("uriC", artifact.getUri());
    assertEquals(9, artifact.getVersion().intValue());
    assertTrue(repoIterator.hasNext());
    artifact = repoIterator.next();
    assertEquals("4", artifact.getId());
    assertEquals("uriC", artifact.getUri());
    assertEquals(1, artifact.getVersion().intValue());
    assertFalse(repoIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> {repoIterator.next();});
  }
}
