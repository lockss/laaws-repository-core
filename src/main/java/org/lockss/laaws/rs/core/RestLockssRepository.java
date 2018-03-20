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

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.util.ArtifactDataFactory;
import org.lockss.laaws.rs.util.ArtifactDataUtil;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.util.NamedInputStreamResource;
import org.lockss.laaws.rs.model.ArtifactData;
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

    private static final String SEPARATOR = "/";
    private static final String COLLECTION_BASE = SEPARATOR + "repos";
    private static final String ARTIFACT_BASE = SEPARATOR + "artifacts";

    private RestTemplate restTemplate;
    private URL repositoryUrl;

    /**
     * Constructor that takes a base URL to a remote LOCKSS Repository service, and uses an unmodified Spring REST
     * template client.
     *
     * @param repositoryUrl
     *          A {@code URL} containing the base URL of the remote LOCKSS Repository service.
     */
    public RestLockssRepository(URL repositoryUrl) {
        this(repositoryUrl, new RestTemplate());
    }

    /**
     * Constructor that takes a base URL to a remote LOCKSS Repository service, and an instance of Spring's
     * {@code RestTemplate}. Used for mainly for testing.
     *
     * @param repositoryUrl
     *          A {@code URL} containing the base URL of the remote LOCKSS Repository service.
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
     *          A {@code String} containing the collection ID.
     * @return A {@code String} containing the REST endpoint of this collection.
     */
    private String buildEndpoint(String collectionId) {
        StringBuilder endpoint = new StringBuilder();
        endpoint.append(repositoryUrl);
        endpoint.append(COLLECTION_BASE).append(SEPARATOR).append(collectionId).append(ARTIFACT_BASE);

        return endpoint.toString();
    }

    /**
     * Builds a remote REST endpoint for a specific artifact, provided its artifact ID.
     *
     * @param collectionId
     *          A {@code String} containing the collection ID.
     * @param artifactId
     *          A {@code String} containing the artifact ID.
     * @return A {@code String} containing the REST endpoint of this artifact.
     */
    private String buildEndpoint(String collectionId, String artifactId) {
        StringBuilder endpoint = new StringBuilder();
        endpoint.append(buildEndpoint(collectionId));
        endpoint.append(SEPARATOR).append(artifactId);

        return endpoint.toString();
    }

    /**
     * Adds an instance of {@code ArtifactData} to the remote REST LOCKSS Repository server.
     *
     * Encodes an {@code ArtifactData} and its constituent parts into a multipart/form-data HTTP POST request for
     * transmission to a remote LOCKSS repository.
     *
     * @param artifactData
     *          An {@code ArtifactData} to add to the remote LOCKSS repository.
     * @return A {@code String} containing the artifact ID of the newly added artifact.
     */
    @Override
    public Artifact addArtifact(ArtifactData artifactData) throws IOException {
        // Get artifact identifier
        ArtifactIdentifier identifier = artifactData.getIdentifier();

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
                    ArtifactDataUtil.getHttpResponseStreamFromArtifact(artifactData)
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

        // POST the multipart entity to the remote LOCKSS repository and return the result
        return restTemplate.exchange(
                buildEndpoint(identifier.getCollection()),
                HttpMethod.POST,
                multipartEntity,
                Artifact.class
        ).getBody();
    }

    /**
     * Retrieves an artifact from a remote REST LOCKSS Repository server.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param artifactId
     *          A {@code String} containing the artifact ID of the artifact to retrieve from the remote repository.
     * @return The {@code ArtifactData} referenced by the artifact ID.
     * @throws IOException
     */
    @Override
    public ArtifactData getArtifactData(String collection, String artifactId) throws IOException {
        ResponseEntity<Resource> response = restTemplate.exchange(
                buildEndpoint(collection, artifactId),
                HttpMethod.GET,
                null,
                Resource.class);

        // Is this InputStream backed by memory? Or over a threshold, is it backed by disk?
        return ArtifactDataFactory.fromHttpResponseStream(response.getBody().getInputStream());
    }

    /**
     * Commits an artifact to this LOCKSS repository for permanent storage and inclusion in LOCKSS repository queries.
     *
     * @param collection
     *          A {code String} containing the collection ID containing the artifact to commit.
     * @param artifactId
     *          A {@code String} with the artifact ID of the artifact to commit to the repository.
     * @return An {@code Artifact} containing the updated artifact state information.
     * @throws IOException
     */
    @Override
    public Artifact commitArtifact(String collection, String artifactId) throws IOException {
        // Create a multivalue map to contain the multipart parts
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        parts.add("committed", true);

        return updateArtifact(collection, artifactId, parts);
    }

    /**
     * Permanently removes an artifact from this LOCKSS repository.
     *
     * @param collection
     *          A {code String} containing the collection ID of the collection containing the artifact to delete.
     * @param artifactId
     *          A {@code String} with the artifact ID of the artifact to remove from this LOCKSS repository.
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
    private Artifact updateArtifact(String collection, String artifactId, MultiValueMap<String, Object> parts) {
        // Create PUT request entity
        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(parts, null);

        // Submit PUT request and return artifact index data
        return restTemplate.exchange(
                buildEndpoint(collection, artifactId),
                HttpMethod.PUT,
                requestEntity,
                Artifact.class
        ).getBody();
    }

    /**
     * Returns a boolean indicating whether an artifact by an artifact ID exists in this LOCKSS repository.
     *
     * @param artifactId
     *          A String with the ArtifactData ID of the artifact to check for existence.
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
     *          ArtifactData ID of the artifact to check committed status.
     * @return A boolean indicating whether the artifact is committed.
     */
    @Override
    public boolean isArtifactCommitted(String artifactId) {
        return false;
    }

    /**
     * Provides the collection identifiers of the committed artifacts in the index.
     *
     * @return An {@code Iterator<String>} with the index committed artifacts
     * collection identifiers.
     */
    @Override
    public Iterable<String> getCollectionIds() {
        ResponseEntity<List<String>> response = restTemplate.exchange(
                repositoryUrl.toString() + "/repos",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<String>>() {}
        );

        return IteratorUtils.asIterable(response.getBody().iterator());
    }

    /**
     * Returns a list of Archival Unit IDs (AUIDs) in this LOCKSS repository collection.
     *
     * @param collection
     *          A {@code String} containing the LOCKSS repository collection ID.
     * @return A {@code Iterator<String>} iterating over the AUIDs in this LOCKSS repository collection.
     * @throws IOException
     */
    @Override
    public Iterable<String> getAuIds(String collection) throws IOException {
        // TODO: Need to create an appropriate REST endpoint
        return null;
    }

    /**
     * Returns the committed artifacts of the latest version of all URLs, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @return An {@code Iterator<Artifact>} containing the latest version of all URLs in an AU.
     * @throws IOException
     */
    @Override
    public Iterable<Artifact> getAllArtifacts(String collection, String auid) throws IOException {
        return null;
    }

    private Iterator<Artifact> queryArtifacts(UriComponentsBuilder builder) {
        ResponseEntity<List<Artifact>> response = restTemplate.exchange(
                builder.build().encode().toUri(),
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<Artifact>>() {
                }
        );

        List<Artifact> responseBody = response.getBody();

        return responseBody.iterator();
    }

    /**
     * Returns the committed artifacts of all versions of all URLs, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all version of all URLs in an AU.
     */
    @Override
    public Iterable<Artifact> getAllArtifactsAllVersions(String collection, String auid) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(buildEndpoint(collection))
                .queryParam("auid", auid);

        return IteratorUtils.asIterable(queryArtifacts(builder));
    }

    /**
     * Returns the committed artifacts of the latest version of all URLs matching a prefix, from a specified Archival
     * Unit and collection.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @param prefix
     *          A {@code String} containing a URL prefix.
     * @return An {@code Iterator<Artifact>} containing the latest version of all URLs matching a prefix in an AU.
     * @throws IOException
     */
    @Override
    public Iterable<Artifact> getAllArtifactsWithPrefix(String collection, String auid, String prefix) throws IOException {
        return null;
    }

    /**
     * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified Archival Unit and
     * collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param prefix
     *          A String with the URL prefix.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matchign a
     *         prefix from an AU.
     */
    @Override
    public Iterable<Artifact> getAllArtifactsWithPrefixAllVersions(String collection, String auid, String prefix) {
        return null;
    }

    /**
     * Returns the committed artifacts of all versions of a given URL, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A {@code String} with the collection identifier.
     * @param auid
     *          A {@code String} with the Archival Unit identifier.
     * @param url
     *          A {@code String} with the URL to be matched.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL from an
     *         Archival Unit.
     */
    @Override
    public Iterable<Artifact> getArtifactAllVersions(String collection, String auid, String url) {
        return null;
    }

    /**
     * Returns the artifact of the latest version of given URL, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A {@code String} containing the collection ID.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @param url
     *          A {@code String} containing a URL.
     * @return The {@code Artifact} representing the latest version of the URL in the AU.
     * @throws IOException
     */
    @Override
    public Artifact getArtifact(String collection, String auid, String url) throws IOException {
        return null;
    }

    /**
     * Returns the artifact of a given version of a URL, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param url
     *          A String with the URL to be matched.
     * @param version
     *          A String with the version.
     * @return The {@code Artifact} of a given version of a URL, from a specified AU and collection.
     */
    @Override
    public Artifact getArtifactVersion(String collection, String auid, String url, Integer version) {
        return null;
    }

    public Iterator<Artifact> getArtifactsWithUriPrefix(String collection, String uri) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(buildEndpoint(collection))
                .queryParam("uri", uri);

        return queryArtifacts(builder);
    }

    public Iterator<Artifact> getArtifactsWithUriPrefix(String collection, String auid, String prefix) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(buildEndpoint(collection))
                .queryParam("uri", prefix)
                .queryParam("auid", auid);

        return queryArtifacts(builder);
    }

    // List<Artifact> getLatestArtifactsWithUri(String collection, String uri); // return latest version per AU
    // List<Artifact> getLatestArtifactsWithUriPrefixFromAu(String collection, String auid, String uri);

    // Higher level repository operations: Implement as util methods?
    //public Page<URI> getUrisByCollectionAndAuid(String collection, String auid); // Get URLs, given AUID
    //public Page<String> getAuidByUri(String collection, String uri);
    //public Page<String> getAuidByRepository(String collection, String repo);
}