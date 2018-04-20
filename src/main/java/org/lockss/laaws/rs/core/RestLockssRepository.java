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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.util.ArtifactConstants;
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
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * REST client implementation of the LOCKSS Repository API; makes REST calls to a remote LOCKSS Repository REST server.
 */
public class RestLockssRepository implements LockssRepository {
    private final static Log log = LogFactory.getLog(RestLockssRepository.class);

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
     * Constructs a REST endpoint to an artifact in the repository.
     *
     * @param collection A {@code String} containing the collection ID.
     * @param artifactId A {@code String} containing the artifact ID.
     * @return A {@code URI} containing the REST endpoint to an artifact in the repository.
     */
    private URI artifactEndpoint(String collection, String artifactId) {
        String endpoint = String.format("%s/collections/%s/artifacts/%s", repositoryUrl, collection, artifactId);
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(endpoint);
        return builder.build().encode().toUri();
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
        if (artifactData == null)
            throw new IllegalArgumentException("ArtifactData is null");

        // Get artifact identifier
        ArtifactIdentifier artifactId = artifactData.getIdentifier();

        log.info(String.format(
                "Adding artifact to remote repository (Collection: %s, AU: %s, URI: %s)",
                artifactId.getCollection(),
                artifactId.getAuid(),
                artifactId.getUri()
        ));

        // Create a multivalue map to contain the multipart parts
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        parts.add("auid", artifactId.getAuid());
        parts.add("uri", artifactId.getUri());

        // Prepare artifact multipart headers
        HttpHeaders contentPartHeaders = new HttpHeaders();

        // This must be set or else AbstractResource#contentLength will read the entire InputStream to determine the
        // content length, which will exhaust the InputStream.
        contentPartHeaders.setContentLength(0); // TODO: Should be set to the length of the multipart body.
        contentPartHeaders.setContentType(MediaType.valueOf("application/http; msgtype=response"));

        // Prepare artifact multipart body
        try {
            Resource artifactPartResource = new NamedInputStreamResource(
                    "content",
                    ArtifactDataUtil.getHttpResponseStreamFromArtifact(artifactData)
            );

            // Add artifact multipart to multiparts list
            parts.add("content", new HttpEntity<>(artifactPartResource, contentPartHeaders));
        } catch (HttpException e) {
            String errMsg = String.format("Error generating HTTP response stream from artifact data: %s", e);
            log.error(errMsg);
            throw new IOException(errMsg);
        }

        // TODO: Create an attach optional artifact aspects
//        parts.add("aspectsParts", new NamedByteArrayResource("aspect1", "metadata bytes1".getBytes()));
//        parts.add("aspectsParts", new NamedByteArrayResource("aspect2", "metadata bytes2".getBytes()));
//        parts.add("aspectsParts", new NamedByteArrayResource("aspect3", "metadata bytes3".getBytes()));

        // POST body entity
        HttpEntity<MultiValueMap<String, Object>> multipartEntity = new HttpEntity<>(parts, null);

        // Construct REST endpoint to collection
        String endpoint = String.format("%s/collections/%s/artifacts", repositoryUrl, artifactId.getCollection());
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(endpoint);

        // POST the multipart entity to the remote LOCKSS repository and return the result
        try {
            return restTemplate.exchange(
                    builder.build().encode().toUri(),
                    HttpMethod.POST,
                    multipartEntity,
                    Artifact.class
            ).getBody();
        } catch (HttpServerErrorException e) {
            String errMsg = String.format("Could not add artifact; remote server responded with: %s", e.getMessage());
            log.error(errMsg);
            throw new IOException(errMsg);
        }
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
                artifactEndpoint(collection, artifactId),
                HttpMethod.GET,
                null,
                Resource.class);

        // TODO: Is this InputStream backed by memory? Or over a threshold, is it backed by disk?
        return ArtifactDataFactory.fromHttpResponseStream(response.getHeaders(), response.getBody().getInputStream());
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
        if ((collection == null) || (artifactId == null))
            throw new IllegalArgumentException("Null collection or artifactId");

        UriComponentsBuilder builder = UriComponentsBuilder.fromUri(artifactEndpoint(collection, artifactId))
                .queryParam("committed", "true");

        // Required by REST API specification
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("multipart/form-data"));

        return restTemplate.exchange(
                builder.build().encode().toUri(),
                HttpMethod.PUT,
                new HttpEntity<>(null, headers),
                Artifact.class
        ).getBody();
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
        if ((collection == null) || (artifactId == null))
            throw new IllegalArgumentException("Null collection ID or artifact ID");

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            restTemplate.exchange(
                    artifactEndpoint(collection, artifactId),
                    HttpMethod.DELETE,
                    new HttpEntity<>(null, headers),
                    Integer.class
            );
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() != HttpStatus.NOT_FOUND) {
                throw new IOException(e);
            }
        } catch (HttpServerErrorException e) {
            if (e.getStatusCode().is5xxServerError()) {
                throw new IOException(String.format(
                        "Could not remove artifact from remote server: %s",
                        e.getMessage()
                ));
            }
        }
    }

    /**
     *
     * @param collection
     * @param artifactId
     * @param parts
     * @return
     */
    private Artifact updateArtifactProperties(String collection, String artifactId, MultiValueMap<String, Object> parts) {
        // Create PUT request entity
        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(parts, null);

        // Submit PUT request and return artifact index data
        try {
            return restTemplate.exchange(
                    artifactEndpoint(collection, artifactId),
                    HttpMethod.PUT,
                    requestEntity,
                    Artifact.class
            ).getBody();
        } catch (HttpClientErrorException e) {
            return null;
        }
    }

    /**
     * Returns a boolean indicating whether an artifact by an artifact ID exists in this LOCKSS repository.
     *
     * @param artifactId
     *          A String with the ArtifactData ID of the artifact to check for existence.
     * @return A boolean indicating whether an artifact exists in this repository.
     */
    @Override
    public Boolean artifactExists(String collection, String artifactId) {
        if (StringUtils.isEmpty(artifactId)) {
            throw new IllegalArgumentException("Null or empty identifier");
        }

        try {
            HttpHeaders headers = restTemplate.headForHeaders(artifactEndpoint(collection, artifactId));
            return headers != null;
        } catch (HttpClientErrorException e) {
            HttpStatus status = e.getStatusCode();

            if (status.value() == 404) {
                // Artifact not found
                return false;
            }

            if (status.is5xxServerError()) {
                log.warn(String.format(
                        "Cannot determine if artifacts exists; remote server responded with: %s",
                        status
                ));
            }
        }

        return null;
    }

    /**
     * Returns a boolean indicating whether an artifact is committed in this LOCKSS repository.
     *
     * @param artifactId
     *          ArtifactData ID of the artifact to check committed status.
     * @return A boolean indicating whether the artifact is committed.
     */
    @Override
    public Boolean isArtifactCommitted(String collection, String artifactId) {
        if (StringUtils.isEmpty(artifactId)) {
            throw new IllegalArgumentException("Null or empty identifier");
        }

        try {
            HttpHeaders headers = restTemplate.headForHeaders(artifactEndpoint(collection, artifactId));
            String committedValue = headers.getFirst(ArtifactConstants.ARTIFACT_STATE_COMMITTED);

            if (committedValue == null) {
                String errMsg = String.format(
                        "Remote repository did not return %s header for artifact (Collection: %s, Artifact: %s)",
                        ArtifactConstants.ARTIFACT_STATE_COMMITTED,
                        collection,
                        artifactId
                );

                log.error(errMsg);
                return null;
            }

            return committedValue.toLowerCase().equals("true");
        } catch (HttpClientErrorException e) {
            HttpStatus status = e.getStatusCode();
            log.warn(String.format("Could not determine artifact commit status from server response: %s", status));
        }

        return null;
    }

    /**
     * Provides the collection identifiers of the committed artifacts in the index.
     *
     * @return An {@code Iterator<String>} with the index committed artifacts
     * collection identifiers.
     */
    @Override
    public Iterable<String> getCollectionIds() {
        String endpoint = String.format("%s/collections", repositoryUrl);

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(endpoint);

        ResponseEntity<List<String>> response = restTemplate.exchange(
                builder.build().encode().toUri(),
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
        String endpoint = String.format("%s/collections/%s/aus", repositoryUrl, collection);

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(endpoint);

        try {
            ResponseEntity<List<String>> response = restTemplate.exchange(
                    builder.build().encode().toUri(),
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<List<String>>() {
                    }
            );

            return IteratorUtils.asIterable(response.getBody().iterator());
        } catch (HttpClientErrorException e) {
            log.error(e);
        }

        return (Iterable)new ArrayList<Artifact>();
    }

    /**
     * Returns an iterable object over artifacts, given a REST endpoint that returns artifacts.
     *
     * @param builder A {@code UriComponentsBuilder} containing a REST endpoint that returns artifacts.
     * @return An {@code Iterable<Artifact>} containing artifacts.
     */
    private Iterable<Artifact> getArtifacts(UriComponentsBuilder builder) {
        return getArtifacts(builder.build().encode().toUri());
    }

    /**
     * Returns an iterable object over artifacts, given a REST endpoint that returns artifacts.
     *
     * @param endpoint A {@code URI} containing a REST endpoint that returns artifacts.
     * @return An {@code Iterable<Artifact>} containing artifacts.
     */
    private Iterable<Artifact> getArtifacts(URI endpoint) {
        try {
            ResponseEntity<List<Artifact>> response = restTemplate.exchange(
                    endpoint,
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<List<Artifact>>() {
                    }
            );

            return IteratorUtils.asIterable(response.getBody().iterator());
        } catch (HttpClientErrorException e) {
            log.warn("Could not fetch artifacts; remote server responded with: %s", e);
        }

        return new ArrayList<Artifact>();
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
    public Iterable<Artifact> getAllArtifacts(String collection, String auid) {
        String endpoint = String.format("%s/collections/%s/aus/%s/artifacts", repositoryUrl, collection, auid);

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(endpoint)
                .queryParam("version", "latest");

        return getArtifacts(builder);
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
        String endpoint = String.format("%s/collections/%s/aus/%s/artifacts", repositoryUrl, collection, auid);

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(endpoint)
                .queryParam("version", "latest");

        return getArtifacts(builder);
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
    public Iterable<Artifact> getAllArtifactsWithPrefix(String collection, String auid, String prefix) {
        String endpoint = String.format("%s/collections/%s/aus/%s/artifacts", repositoryUrl, collection, auid);

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(endpoint)
                .queryParam("uriPrefix", prefix);

        return getArtifacts(builder);
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
        String endpoint = String.format("%s/collections/%s/aus/%s/artifacts", repositoryUrl, collection, auid);

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(endpoint)
                .queryParam("uriPrefix", prefix);

        return getArtifacts(builder);
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
        String endpoint = String.format("%s/collections/%s/aus/%s/artifacts", repositoryUrl, collection, auid);

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(endpoint)
                .queryParam("uri", url)
                .queryParam("version", "all");

        return getArtifacts(builder);
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
        String endpoint = String.format("%s/collections/%s/aus/%s/artifacts", repositoryUrl, collection, auid);

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(endpoint)
                .queryParam("version", "latest");

        ResponseEntity<List<Artifact>> response = restTemplate.exchange(
                builder.build().encode().toUri(),
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<Artifact>>() {}
        );

        List<Artifact> artifacts = response.getBody();

        if (artifacts.size() > 1) {
            log.warn(String.format(
                    "Expected one or no artifacts for latest version but got %d (Collection: %s, AU: %s, URL: %s)",
                    artifacts.size(),
                    collection,
                    url,
                    auid
            ));
        }

        return artifacts.get(0);
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
     *          An Integer with the version.
     * @return The {@code Artifact} of a given version of a URL, from a specified AU and collection.
     */
    @Override
    public Artifact getArtifactVersion(String collection, String auid, String url, Integer version) {
        String endpoint = String.format("%s/collections/%s/aus/%s/artifacts", repositoryUrl, collection, auid);

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(endpoint)
                .queryParam("url", url)
                .queryParam("version", version);

        ResponseEntity<List<Artifact>> response = restTemplate.exchange(
                builder.build().encode().toUri(),
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<Artifact>>() {}
        );

        List<Artifact> artifacts = response.getBody();

        if (!artifacts.isEmpty()) {
            // Warn if the server returned more than one ar
            if (artifacts.size() > 1) {
                log.warn(String.format(
                        "Expected one or no artifacts but got %d (Collection: %s, AU: %s, URL: %s, Version: %s)",
                        artifacts.size(),
                        collection,
                        auid,
                        url,
                        version
                ));
            }

            //
            return artifacts.get(0);
        }

        // No artifact found
        return null;
    }

    /**
     * Returns the size, in bytes, of AU in a collection.
     *
     * @param collection A {@code String} containing the collection ID.
     * @param auid       A {@code String} containing the Archival Unit ID.
     * @return A {@code Long} with the total size of the specified AU in bytes.
     */
    @Override
    public Long auSize(String collection, String auid) {
        String endpoint = String.format("%s/collections/%s/aus/%s/size", repositoryUrl, collection, auid);

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(endpoint)
                .queryParam("version", "all");

        ResponseEntity<Long> response = null;

        try {
            response = restTemplate.exchange(
                    builder.build().encode().toUri(),
                    HttpMethod.GET,
                    null,
                    Long.class
            );
        } catch (HttpClientErrorException e) {
           HttpStatus status = e.getStatusCode();

           if (status.value() == 404) {
               return new Long(0);
           }

           log.error(String.format("Could not determine AU size: %s", status));
           return null;
        }

        return response.getBody();
    }
}