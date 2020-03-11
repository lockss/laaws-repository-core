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
import org.lockss.util.rest.RestUtil;
import org.lockss.util.test.LockssTestCase5;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Test class for org.lockss.laaws.rs.core.RestLockssRepositoryAuidIterator.
 * 
 * @author Fernando García-Loygorri
 */
public class TestRestLockssRepositoryAuidIterator extends LockssTestCase5 {
  private final static String BASEURL = "http://localhost:24610";
  private final static String collectionId = "collId";
  private RestTemplate restTemplate;
  private MockRestServiceServer mockServer;
  private String endpoint;
  private UriComponentsBuilder builder;
  private RestLockssRepositoryAuidIterator repoIterator;

  /**
   * Set up code to be run before each test.
   */
  @Before
  public void makeRepoIterator() {
    restTemplate = RestUtil.getSimpleFactoryRestTemplate(true);
    mockServer = MockRestServiceServer.createServer(restTemplate);
    endpoint = String.format("%s/collections/%s/aus", BASEURL, collectionId);
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
    .andRespond(withSuccess("{\"auids\":[],\"pageInfo\":{}}",
	MediaType.APPLICATION_JSON));

    repoIterator = new RestLockssRepositoryAuidIterator(restTemplate, builder);
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
    .andRespond(withSuccess(
	"{\"auids\":[\"auid1\",\"auid2\",\"auid3\"],\"pageInfo\":{}}",
	MediaType.APPLICATION_JSON));

    repoIterator = new RestLockssRepositoryAuidIterator(restTemplate, builder);
    mockServer.verify();

    assertTrue(repoIterator.hasNext());
    assertEquals("auid1", repoIterator.next());
    assertTrue(repoIterator.hasNext());
    assertEquals("auid2", repoIterator.next());
    assertTrue(repoIterator.hasNext());
    assertEquals("auid3", repoIterator.next());
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
    .andRespond(withSuccess("{\"auids\":[\"auid1\",\"auid2\"],"
	+ "\"pageInfo\":{\"continuationToken\":\"auid2:1234567\"}}",
	MediaType.APPLICATION_JSON));

    // Second server call.
    mockServer.expect(requestTo(endpoint
	+ "?limit=2&continuationToken=auid2:1234567"))
    .andExpect(method(HttpMethod.GET))
    .andRespond(withSuccess("{\"auids\":[\"auid3\",\"auid4\"],\"pageInfo\":{}}",
	MediaType.APPLICATION_JSON));

    repoIterator =
	new RestLockssRepositoryAuidIterator(restTemplate, builder, 2);

    assertTrue(repoIterator.hasNext());
    assertEquals("auid1", repoIterator.next());
    assertTrue(repoIterator.hasNext());
    assertEquals("auid2", repoIterator.next());
    assertTrue(repoIterator.hasNext());

    // Both server calls have happened.
    mockServer.verify();

    assertEquals("auid3", repoIterator.next());
    assertTrue(repoIterator.hasNext());
    assertEquals("auid4", repoIterator.next());
    assertFalse(repoIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> {repoIterator.next();});
  }
}
