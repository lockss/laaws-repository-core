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
import org.apache.http.HttpException;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.util.ArtifactFactory;
import org.lockss.laaws.rs.util.ArtifactUtil;
import org.lockss.laaws.rs.model.ArtifactIndexData;
import org.lockss.laaws.rs.util.NamedInputStreamResource;
import org.lockss.laaws.rs.model.Artifact;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.Resource;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * REST client implementation of the LOCKSS Repository API; makes REST calls to a remote LOCKSS Repository REST server.
 */
public class RestLockssRepository implements LockssRepository {
    private final static Log log = LogFactory.getLog(RestLockssRepository.class);

    private final String SEPERATOR = "/";
    private final String COLLECTION_BASE = SEPERATOR + "repos";
    private final String ARTIFACT_BASE = SEPERATOR + "artifacts";

    private RestTemplate restTemplate;
    private URL repositoryUrl;

    /**
     * Constructor that takes a base URL to a remote LOCKSS Repository service, and uses an unmodified Spring REST
     * template client.
     *
     * @param repositoryUrl
     *          Base URL of the remote LOCKSS Repository service.
     */
    public RestLockssRepository(URL repositoryUrl) {
        this(repositoryUrl, new RestTemplate());
    }

    /**
     * Constructor that takes a base URL to a remote LOCKSS Repository service, and an instance of Spring's
     * {@code RestTemplate}. Used for mainly for testing.
     *
     * @param repositoryUrl
     *          Base URL of the remote LOCKSS Repository service.
     * @param restTemplate
     *          Instance of {@code RestTemplate} to use internally for remote REST calls.
     */
    public RestLockssRepository(URL repositoryUrl, RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
        this.repositoryUrl = repositoryUrl;

        // Set the buffer to false for streaming - still needed?
        //SimpleClientHttpRequestFactory factory = (SimpleClientHttpRequestFactory) this.restTemplate.getRequestFactory();
        //factory.setBufferRequestBody(false);
    }

    /**
     * Builds a remote REST endpoint for a specific collection.
     *
     * @param collectionId
     *          A String with the collection ID.
     * @return The REST endpoint of this collection.
     */
    private String buildEndpoint(String collectionId) {
        StringBuilder endpoint = new StringBuilder();
        endpoint.append(repositoryUrl);
        endpoint.append(COLLECTION_BASE).append(SEPERATOR).append(collectionId).append(ARTIFACT_BASE);

        return endpoint.toString();
    }

    /**
     * Builds a remote REST endpoint for a specific artifact, provided its artifact ID.
     *
     * @param collectionId
     *          A String with the collection ID.
     * @param artifactId
     *          A String with the Artifact ID.
     * @return The REST endpoint to this artifact.
     */
    private String buildEndpoint(String collectionId, String artifactId) {
        StringBuilder endpoint = new StringBuilder();
        endpoint.append(buildEndpoint(collectionId));
        endpoint.append(SEPERATOR).append(artifactId);

        return endpoint.toString();
    }

    /**
     * Adds an instance of {@code Artifact} to the remote REST LOCKSS Repository server.
     *
     * Encodes an {@code Artifact} and its constituent parts into a multipart/form-data HTTP POST request for
     * transmission to a remote repository.
     *
     * @param artifact
     *          The instance of {@code Artifact} to add to the remote repository.
     * @return A String with an artifact ID; handle to the newly added artifact.
     */
    @Override
    public String addArtifact(Artifact artifact)
            throws IOException {

        // Get artifact identifier
        ArtifactIdentifier identifier = artifact.getIdentifier();

        // Create a multivalue map to contain the multipart parts
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        parts.add("auid", identifier.getAuid());
        parts.add("uri", identifier.getUri());
        parts.add("version", 1); // TODO: Set to artifact version

        // Prepare artifact multipart headers
        HttpHeaders artifactPartHeaders = new HttpHeaders();

        // This must be set or else AbstractResource#contentLength will read the entire InputStream to determine the
        // content length, which will exhaust the InputStream.
        artifactPartHeaders.setContentLength(0); // TODO: Should be set to the length of the multipart body.
        artifactPartHeaders.setContentType(MediaType.valueOf("application/http;msgtype=response"));

        // Prepare artifact multipart body
        try {
            Resource artifactPartResource = new NamedInputStreamResource(
                    "artifact",
                    ArtifactUtil.getHttpResponseStreamFromArtifact(artifact)
            );

            // Add artifact multipart to multiparts list
            parts.add("artifact", new HttpEntity<>(artifactPartResource, artifactPartHeaders));
        } catch (HttpException e) {
            throw new IOException(e);
        }

        // TODO: Create an attach optional artifact aspects
//        parts.add("aspects", new NamedByteArrayResource("aspect1", "metadata bytes1".getBytes()));
//        parts.add("aspects", new NamedByteArrayResource("aspect2", "metadata bytes2".getBytes()));
//        parts.add("aspects", new NamedByteArrayResource("aspect3", "metadata bytes3".getBytes()));

        // POST body entity
        HttpEntity<MultiValueMap<String, Object>> multipartEntity = new HttpEntity<>(parts, null);

        // POST the multipart entity to the  Repository Service and return result
        return restTemplate.exchange(
                buildEndpoint(identifier.getCollection()),
                HttpMethod.POST,
                multipartEntity,
                String.class
        ).getBody();
    }

    /**
     * Retrieves an artifact from a remote REST LOCKSS Repository server.
     *
     * @param artifactId
     *          A String with the Artifact ID of the artifact to retrieve from the remote repository.
     * @return The {@code Artifact} referenced by the artifact ID.
     * @throws IOException
     */
    @Override
    public Artifact getArtifact(String collection, String artifactId) throws IOException {
        ResponseEntity<Resource> response = restTemplate.exchange(
                buildEndpoint(collection, artifactId),
                HttpMethod.GET,
                null,
                Resource.class);

        // Is this InputStream backed by memory? Or over a threshold, is it backed by disk?
        return ArtifactFactory.fromHttpResponseStream(response.getBody().getInputStream());
    }

    /**
     * Commits an artifact to a remote LOCKSS repository for permanent storage and inclusion in LOCKSS repository
     * queries.
     *
     * @param artifactId
     *          A String with the Artifact ID of the artifact to commit to the repository.
     * @return TODO
     * @throws IOException
     */
    @Override
    public ArtifactIndexData commitArtifact(String collection, String artifactId) throws IOException {
        // Create a multivalue map to contain the multipart parts
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        parts.add("committed", true);

        return updateArtifact(collection, artifactId, parts);
    }

    /**
     * Permanently removes an artifact from the remote LOCKSS repository.
     *
     * @param artifactId
     *          A String with the Artifact ID of the artifact to remove from the LOCKSS repository.
     * @throws IOException
     */
    @Override
    public void deleteArtifact(String collection, String artifactId) throws IOException {
        ResponseEntity<Integer> response = restTemplate.exchange(
                buildEndpoint(collection, artifactId),
                HttpMethod.DELETE,
                null,
                Integer.class
        );
    }

    /**
     *
     * @param collection
     * @param artifactId
     * @param parts
     * @return
     */
    private ArtifactIndexData updateArtifact(String collection, String artifactId, MultiValueMap parts) {
        // Create PUT request entity
        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(parts, null);

        // Submit PUT request and return artifact index data
        return restTemplate.exchange(
                buildEndpoint(collection, artifactId),
                HttpMethod.PUT,
                requestEntity,
                ArtifactIndexData.class
        ).getBody();
    }

    /**
     * Returns a boolean indicating whether an artifact by an artifact ID exists in this LOCKSS repository.
     *
     * @param artifactId
     *          A String with the Artifact ID of the artifact to check for existence.
     * @return A boolean indicating whether an artifact exists in this repository.
     */
    @Override
    public boolean artifactExists(String artifactId) {
        return false;
    }

    /**
     * Returns a boolean indicating whether an artifact is committed in this LOCKSS repository.
     *
     * @param artifactId
     *          Artifact ID of the artifact to check committed status.
     * @return A boolean indicating whether the artifact is committed.
     */
    @Override
    public boolean isArtifactCommitted(String artifactId) {
        return false;
    }

    /**
     * Provides the collection identifiers of the committed artifacts in the
     * index.
     *
     * @return an {@code Iterator<String>} with the index committed artifacts
     * collection identifiers.
     */
    @Override
    public Iterator<String> getCollectionIds() {
        ResponseEntity<List> response = restTemplate.exchange(
                repositoryUrl.toString() + "/repos",
                HttpMethod.GET,
                null,
                List.class
        );

        return response.getBody().iterator();
    }

    /**
     * Returns an iterator over the Archival Unit IDs (AUIDs) in this collection.
     *
     * @param collection A String with the collection identifier.
     * @return A {@code Iterator<String>} with the AUIDs in the collection.
     */
    @Override
    public Iterator<String> getAuIds(String collection) throws IOException {
        // TODO: Need to create an appropriate REST endpoint
        return null;
    }

    private Iterator<ArtifactIndexData> queryArtifacts(UriComponentsBuilder builder) {
        ResponseEntity<List<ArtifactIndexData>> response = restTemplate.exchange(
                builder.build().encode().toUri(),
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<ArtifactIndexData>>() {
                }
        );

        List<ArtifactIndexData> responseBody = response.getBody();

        return responseBody.iterator();
    }

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     *         artifacts in the collection that belong to the Archival Unit.
     */
    @Override
    public Iterator<ArtifactIndexData> getArtifactsInAU(String collection, String auid) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(buildEndpoint(collection))
                .queryParam("auid", auid);

        return queryArtifacts(builder);
    }

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain a URL with a given prefix.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param prefix
     *          A String with the URL prefix.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain a URL with the given prefix.
     */
    @Override
    public Iterator<ArtifactIndexData> getArtifactsInAUWithURL(String collection, String auid, String prefix) {
        return null;
    }

    @Override
    public Iterator<ArtifactIndexData> getArtifactsInAUWithURLMatch(String collection, String auid, String url) {
        return null;
    }

    @Override
    public Iterator<ArtifactIndexData> getArtifactsInAUWithURL(String collection, String auid, String prefix, String version) {
        return null;
    }

    @Override
    public Iterator<ArtifactIndexData> getArtifactsInAUWithURLMatch(String collection, String auid, String url, String version) {
        return null;
    }

    public Iterator<ArtifactIndexData> getArtifactsWithUriPrefix(String collection, String uri) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(buildEndpoint(collection))
                .queryParam("uri", uri);

        return queryArtifacts(builder);
    }

    public Iterator<ArtifactIndexData> getArtifactsWithUriPrefix(String collection, String auid, String prefix) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(buildEndpoint(collection))
                .queryParam("uri", prefix)
                .queryParam("auid", auid);

        return queryArtifacts(builder);
    }

    // List<ArtifactIndexData> getLatestArtifactsWithUri(String collection, String uri); // return latest version per AU
    // List<ArtifactIndexData> getLatestArtifactsWithUriPrefixFromAu(String collection, String auid, String uri);

    // Higher level repository operations: Implement as util methods?
    //public Page<URI> getUrisByCollectionAndAuid(String collection, String auid); // Get URLs, given AUID
    //public Page<String> getAuidByUri(String collection, String uri);
    //public Page<String> getAuidByRepository(String collection, String repo);
}