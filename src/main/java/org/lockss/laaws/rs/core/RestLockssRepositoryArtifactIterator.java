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
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactPageInfo;
import org.lockss.log.L4JLogger;
import org.lockss.util.LockssUncheckedIOException;
import org.lockss.util.rest.RestUtil;
import org.lockss.util.rest.exception.LockssRestException;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Artifact iterator that wraps a REST Lockss repository service response.
 * 
 * @author Fernando García-Loygorri
 */
public class RestLockssRepositoryArtifactIterator
implements Iterator<Artifact> {
  private final static L4JLogger log = L4JLogger.getLogger();

  // The REST service template.
  private final RestTemplate restTemplate;

  // The REST service URI builder.
  private final UriComponentsBuilder builder;

  // The internal buffer used to store locally the artifacts provided by the
  // REST service.
  private List<Artifact> artifactBuffer = null;

  // An iterator to the internal buffer.
  private Iterator<Artifact> artifactBufferIterator = null;

  // Continuation token for the REST service request.
  private String continuationToken = null;

  /**
   * Constructor with default batch size.
   * 
   * @param restTemplate A RestTemplate with the REST service template.
   * @param builder      An UriComponentsBuilder with the REST service URI
   *                     builder.
   */
  public RestLockssRepositoryArtifactIterator(RestTemplate restTemplate,
      UriComponentsBuilder builder) {
    this(restTemplate, builder, null);
  }

  /**
   * Full constructor.
   * 
   * @param restTemplate A RestTemplate with the REST service template.
   * @param builder      An UriComponentsBuilder with the REST service URI
   *                     builder.
   * @param limit        An Integer with the number of artifacts to request on
   *                     each REST service request.
   */
  public RestLockssRepositoryArtifactIterator(RestTemplate restTemplate,
      UriComponentsBuilder builder, Integer limit) {
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
      builder = builder.queryParam("limit", limit);
    }

    this.builder = builder;

    fillArtifactBuffer();
  }

  /**
   * Returns {@code true} if the there are still artifacts provided by the REST
   * repository service that have not been returned to the client already.
   *
   * @return a boolean with {@code true} if there are more artifacts to be
   *         returned, {@code false} otherwise.
   */
  @Override
  public boolean hasNext() throws RuntimeException {
    log.debug2("Invoked");
    log.trace("artifactBufferIterator.hasNext() = {}",
	artifactBufferIterator.hasNext());
    log.trace("isLastBatch() = {}", isLastBatch());

    boolean hasNext = false;

    // Check whether the internal buffer still has artifacts.
    if (artifactBufferIterator.hasNext()) {
      // Yes: The answer is {@code true}.
      hasNext = true;
      // No: Check whether the current batch is the last one.
    } else if (isLastBatch()) {
      // Yes: The answer is {@code false}.
      hasNext = false;
    } else {
      // No: Keep filling the internal buffer with another batch from the REST
      // service unless the REST service indicates that the last batch has
      // already been provided.
      do {
	fillArtifactBuffer();
      } while (!artifactBufferIterator.hasNext() && !isLastBatch());

      // The answer is determined by the contents of the internal buffer.
      hasNext = artifactBufferIterator.hasNext();
    }

    log.debug2("hasNext = {}", hasNext);
    return hasNext;
  }

  /**
   * Provides the next artifact.
   *
   * @return an Artifact with the next artifact.
   * @throws NoSuchElementException if there are no more artifacts to return.
   */
  @Override
  public Artifact next() throws NoSuchElementException {
    if (hasNext()) {
      return artifactBufferIterator.next();
    } else {
      throw new NoSuchElementException();
    }
  }

  /**
   * Fills the internal buffer with the next batch of artifacts from the REST
   * service.
   */
  private void fillArtifactBuffer() {
    log.debug2("Invoked");

    // Check whether a previous response provided a continuation token.
    if (continuationToken != null) {
      // Yes: Incorporate it to the next request.
      builder.queryParam("continuationToken", continuationToken);
    }

    // Build the URI to make a request to the REST service.
    URI uri = builder.build().encode().toUri();
    log.trace("uri = {}", uri);

    ResponseEntity<String> response = null;

    try {
      // Make the request and get the response.
      response = RestUtil.callRestService(restTemplate, uri, HttpMethod.GET,
	  null, String.class, "fillArtifactBuffer");
    } catch (LockssRestHttpException e) {
      if (e.getHttpStatus().equals(HttpStatus.NOT_FOUND)) {
	log.trace("Could not fetch artifacts: Exception caught", e);
	artifactBuffer = new ArrayList<Artifact>();
	continuationToken = null;
	artifactBufferIterator = artifactBuffer.iterator();
	log.debug2("Done");
	return;
      }
      log.error("Could not fetch artifacts: Exception caught", e);
      throw new LockssUncheckedIOException(e);
    } catch (LockssRestException e) {
      log.error("Could not fetch artifacts: Exception caught", e);
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
	// Get the returned artifact page information.
	log.trace("response.getBody() = {}", (String)response.getBody());

	ArtifactPageInfo api = mapper.readValue((String)response.getBody(),
	    ArtifactPageInfo.class);
	log.trace("api = {}", api);

	// Get the artifacts in the response.
	artifactBuffer = api.getArtifacts();
	log.trace("artifactBuffer = {}", artifactBuffer);

	// Get the continuation token in the response.
	continuationToken = api.getPageInfo().getContinuationToken();
	log.trace("continuationToken = {}", continuationToken);
      } catch (Exception e) {
	// Report the problem.
	log.error("Could not fetch artifacts: Exception caught", e);
	artifactBuffer = new ArrayList<Artifact>();
	continuationToken = null;
      }
    } else {
      // No: Report the problem.
      log.error("Could not fetch artifacts: REST service status: {} {}",
	  status.toString(), status.getReasonPhrase());
      artifactBuffer = new ArrayList<Artifact>();
      continuationToken = null;
    }

    // Create the iterator to the list of artifacts to be provided.
    artifactBufferIterator = artifactBuffer.iterator();
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
