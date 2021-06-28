/*

Copyright (c) 2019 Board of Trustees of Leland Stanford Jr. University,
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.lockss.laaws.rs.model.AuidPageInfo;
import org.lockss.log.L4JLogger;
import org.lockss.util.LockssUncheckedIOException;
import org.lockss.util.rest.RestUtil;
import org.lockss.util.rest.exception.LockssRestException;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Auid iterator that wraps a REST Lockss repository service response.
 * 
 * @author Fernando García-Loygorri
 */
public class RestLockssRepositoryAuidIterator implements Iterator<String> {
  private final static L4JLogger log = L4JLogger.getLogger();

  // The REST service template.
  private final RestTemplate restTemplate;

  // The REST service URI builder.
  private final UriComponentsBuilder builder;

  // The value of the Authorization header to be used when calling the REST
  // service.
  private String authHeaderValue = null;

  // The internal buffer used to store locally the auids provided by the REST
  // service.
  private List<String> auidBuffer = null;

  // An iterator to the internal buffer.
  private Iterator<String> auidBufferIterator = null;

  // Continuation token for the REST service request.
  private String continuationToken = null;

  /**
   * Constructor with default batch size and no Authorization header.
   * 
   * @param restTemplate A RestTemplate with the REST service template.
   * @param builder      An UriComponentsBuilder with the REST service URI
   *                     builder.
   */
  public RestLockssRepositoryAuidIterator(RestTemplate restTemplate,
      UriComponentsBuilder builder) {
    this(restTemplate, builder, null, null);
  }

  /**
   * Constructor with default batch size.
   * 
   * @param restTemplate    A RestTemplate with the REST service template.
   * @param builder         An UriComponentsBuilder with the REST service URI
   *                        builder.
   * @param authHeaderValue A String with the Authorization header to be used
   *                        when calling the REST service.
   */
  public RestLockssRepositoryAuidIterator(RestTemplate restTemplate,
      UriComponentsBuilder builder, String authHeaderValue) {
    this(restTemplate, builder, authHeaderValue, null);
  }

  /**
   * Constructor with no Authorization header.
   * 
   * @param restTemplate A RestTemplate with the REST service template.
   * @param builder      An UriComponentsBuilder with the REST service URI
   *                     builder.
   * @param limit        An Integer with the number of auids to request on each
   *                     REST service request.
   */
  public RestLockssRepositoryAuidIterator(RestTemplate restTemplate,
      UriComponentsBuilder builder, Integer limit) {
    this(restTemplate, builder, null, limit);
  }

  /**
   * Full constructor.
   * 
   * @param restTemplate    A RestTemplate with the REST service template.
   * @param builder         An UriComponentsBuilder with the REST service URI
   *                        builder.
   * @param authHeaderValue A String with the Authorization header to be used
   *                        when calling the REST service.
   * @param limit           An Integer with the number of auids to request on
   *                        each REST service request.
   */
  public RestLockssRepositoryAuidIterator(RestTemplate restTemplate,
      UriComponentsBuilder builder, String authHeaderValue, Integer limit) {

    // Validation.
    if (restTemplate == null) {
      throw new IllegalArgumentException(
	  "REST service template cannot be null");
    }

    if (builder == null) {
      throw new IllegalArgumentException(
	  "REST service URI builder cannot be null");
    }

    if (limit != null && limit.intValue() < 1) {
      throw new IllegalArgumentException("Limit must be at least 1");
    }

    // Initialization.
    this.restTemplate = restTemplate;

    if (limit != null) {
      builder = builder.replaceQueryParam("limit", limit);
    }

    this.builder = builder;
    this.authHeaderValue = authHeaderValue;

    fillAuidBuffer();
  }

  /**
   * Returns {@code true} if the there are still auids provided by the REST
   * repository service that have not been returned to the client already.
   *
   * @return a boolean with {@code true} if there are more auids to be returned,
   *         {@code false} otherwise.
   */
  @Override
  public boolean hasNext() throws RuntimeException {
    log.debug2("Invoked");
    log.trace("auidBufferIterator.hasNext() = {}",
	auidBufferIterator.hasNext());
    log.trace("isLastBatch() = {}", isLastBatch());

    boolean hasNext = false;

    // Check whether the internal buffer still has auids.
    if (auidBufferIterator.hasNext()) {
      // Yes: The answer is {@code true}.
      hasNext = true;
      // No: Check whether the current batch is the last one.
    } else if (isLastBatch()) {
      // Yes: The answer is {@code false}.
      hasNext = false;
    } else {
      // No: Keep filling the internal buffer with another batch from the REST
      // service while the REST service provides no auids but indicates that the
      // last batch has not been provided yet.
      do {
	fillAuidBuffer();
      } while (!auidBufferIterator.hasNext() && !isLastBatch());

      // The answer is determined by the contents of the internal buffer.
      hasNext = auidBufferIterator.hasNext();
    }

    log.debug2("hasNext = {}", hasNext);
    return hasNext;
  }

  /**
   * Provides the next auid.
   *
   * @return a String with the next auid.
   * @throws NoSuchElementException if there are no more auids to return.
   */
  @Override
  public String next() throws NoSuchElementException {
    if (hasNext()) {
      return auidBufferIterator.next();
    } else {
      throw new NoSuchElementException();
    }
  }

  /**
   * Fills the internal buffer with the next batch of auids from the REST
   * service.
   */
  private void fillAuidBuffer() {
    log.debug2("Invoked");

    // Check whether a previous response provided a continuation token.
    if (continuationToken != null) {
      // Yes: Incorporate it to the next request.
      builder.replaceQueryParam("continuationToken", continuationToken);
    }

    // Build the URI to make a request to the REST service.
    URI uri = builder.build().encode().toUri();
    log.trace("uri = {}", uri);

    // Build the HttpEntity to include in the request to the REST service.
    HttpEntity<Void> httpEntity = null;

    // Check whether there is an Authorization header to be used when calling
    // the REST service.
    if (authHeaderValue != null) {
      // Yes: Set up the HTTP headers.
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set("Authorization", authHeaderValue);
      httpEntity = new HttpEntity<>(null, httpHeaders);
    }

    ResponseEntity<String> response = null;

    try {
      // Make the request and get the response.
      response = RestUtil.callRestService(restTemplate, uri, HttpMethod.GET,
          httpEntity, String.class, "fillAuidBuffer");
    } catch (LockssRestHttpException e) {
      if (e.getHttpStatus().equals(HttpStatus.NOT_FOUND)) {
        log.trace("Could not fetch auids: Exception caught", e);
        auidBuffer = new ArrayList<String>();
        continuationToken = null;
        auidBufferIterator = auidBuffer.iterator();
        log.debug2("Done");

        // Q: Should this throw a LRSE instead?
        return;
      }

      log.error("Could not fetch auids: Exception caught", e);

      throw new LockssUncheckedIOException(e);
    } catch (LockssRestException e) {
      log.error("Could not fetch auids: Exception caught", e);
      throw new LockssUncheckedIOException(e);
    }

    // Determine the response status.
    HttpStatus status = response.getStatusCode();
    log.trace("status = {}", status);

    // Check whether the response status indicates success.
    if (status.is2xxSuccessful()) {
      // Yes: Initialize the response body parser.
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
	  false);

      try {
	// Get the returned auid page information.
	log.trace("response.getBody() = {}", (String)response.getBody());

	AuidPageInfo api =
	    mapper.readValue((String)response.getBody(), AuidPageInfo.class);
	log.trace("api = {}", api);

	// Get the auids in the response.
	auidBuffer = api.getAuids();
	log.trace("auidBuffer = {}", auidBuffer);

	// Get the continuation token in the response.
	continuationToken = api.getPageInfo().getContinuationToken();
	log.trace("continuationToken = {}", continuationToken);
      } catch (Exception e) {
	// Report the problem.
	log.error("Could not fetch auids: Exception caught", e);
	auidBuffer = new ArrayList<String>();
	continuationToken = null;
      }
    } else {
      // No: Report the problem.
      log.error("Could not fetch auids: REST service status: {} {}",
	  status.toString(), status.getReasonPhrase());
      auidBuffer = new ArrayList<String>();
      continuationToken = null;
    }

    // Create the iterator to the list of auids to be provided.
    auidBufferIterator = auidBuffer.iterator();
    log.debug2("Done");
  }

  /**
   * Provide an indication of whether the REST service has returned all the
   * requested results already.
   * 
   * @return a boolean with the indication.
   */
  private boolean isLastBatch() {
    return continuationToken == null;
  }
}
