/*
 * Copyright (c) 2019-2020, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.io.index.solr;

import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.lockss.laaws.rs.core.SemaphoreMap;
import org.lockss.laaws.rs.io.index.AbstractArtifactIndex;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.ArtifactComparators;
import org.lockss.log.L4JLogger;
import org.lockss.util.storage.StorageInfo;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An Apache Solr implementation of ArtifactIndex.
 */
public class SolrArtifactIndex extends AbstractArtifactIndex {
  private final static L4JLogger log = L4JLogger.getLogger();

  private final static String DEFAULT_COLLECTION_NAME = "lockss-repo";

  private static final SolrQuery.SortClause SORTURI_ASC = new SolrQuery.SortClause("sortUri", SolrQuery.ORDER.asc);
  private static final SolrQuery.SortClause VERSION_DESC = new SolrQuery.SortClause("version", SolrQuery.ORDER.desc);
  private static final SolrQuery.SortClause AUID_ASC = new SolrQuery.SortClause("auid", SolrQuery.ORDER.asc);

  /**
   * Label to describe type of SolrArtifactIndex
   */
  public static String ARTIFACT_INDEX_TYPE = "Solr";

  // TODO: Currently only used for getStorageInfo()
  private static final Pattern SOLR_COLLECTION_ENDPOINT_PATTERN = Pattern.compile("/solr(/(?<collection>[^/]+)?)?$");

  /**
   * The Solr client implementation to use to talk to a Solr server.
   */
  private final SolrClient solrClient;

  /**
   * Two element list containing [username, password].
   */
  private List<String> solrCredentials;

  /**
   * Name of the Solr collection to use.
   */
  private String solrCollection = DEFAULT_COLLECTION_NAME;

  /**
   * This boolean is used to indicate whether a HttpSolrClient was created internally
   * with a provided Solr URL.
   */
  private final boolean isInternalClient;

  /**
   * Map from artifact stem to semaphore. Used for artifact version locking.
   */
  private SemaphoreMap<ArtifactIdentifier.ArtifactStem> versionLock = new SemaphoreMap<>();

  /**
   * Constructor. Creates and uses an internal {@link HttpSolrClient} from the provided Solr collection endpoint.
   *
   * @param solrBaseUrl A {@link String} containing the URL to a Solr collection or core.
   */
  public SolrArtifactIndex(String solrBaseUrl) {
    this(solrBaseUrl, null, null);
  }

  public SolrArtifactIndex(String solrBaseUrl, List<String> solrCredentials) {
    this(solrBaseUrl, null, solrCredentials);
  }

  /**
   * Constructor. Creates and uses an internal {@link HttpSolrClient} from the provided Solr base URL and Solr
   * collection or core name.
   *
   * @param solrBaseUrl A {@link String} containing the Solr base URL.
   * @param collection  @ {@link String} containing the name of the Solr collection to use.
   */
  public SolrArtifactIndex(String solrBaseUrl, String collection) {
    this(solrBaseUrl, collection, null);
  }

  /**
   * Constructor.
   *
   * @param solrBaseUrl A {@link String} containing the Solr base URL.
   * @param collection  @ {@link String} containing the name of the Solr collection to use.
   * @param solrCredentials A {@link List<String>} containing the username and password for Solr.
   */
  public SolrArtifactIndex(String solrBaseUrl, String collection, List<String> solrCredentials) {
    // Convert provided Solr base URL to URI object
    URI baseUrl = URI.create(solrBaseUrl);

    // Set Solr collection to use
    this.solrCollection = collection;

    // Determine Solr collection/core name from base URL if collection is null or empty
    if (solrCollection == null || solrCollection.isEmpty()) {
      String restEndpoint = baseUrl.normalize().getPath();
      Matcher m = SOLR_COLLECTION_ENDPOINT_PATTERN.matcher(restEndpoint);

      if (m.find()) {
        // Yes: Get the name of the core from the match results
        solrCollection = m.group("collection");

        // Q: I don't think the regex allows these possibilities?
        if (solrCollection == null || solrCollection.isEmpty()) {
          log.error("Solr collection not specified explicitly or in the Solr base URL");
          throw new IllegalArgumentException("Missing Solr collection name");
        }
      } else {
        // No: Did not match expected pattern
        log.error("Unexpected Solr base URL [solrBaseUrl: {}]", solrBaseUrl);
        throw new IllegalArgumentException("Unexpected Solr base URL");
      }
    }

    // Set the Solr credentials
    this.solrCredentials = solrCredentials;

    // Resolve Solr base REST endpoint
    URI solrUrl = baseUrl.resolve("/solr");

    // Build a HttpSolrClient instance using the base URL
    this.solrClient = new HttpSolrClient.Builder()
        .withBaseSolrUrl(solrUrl.toString())
        .build();

    this.isInternalClient = true;
  }

  /**
   * Adds Solr BasicAuth credentials to a {@link SolrRequest}.
   *
   * @param request
   */
  private void addSolrCredentials(SolrRequest request) {
    // Add Solr BasicAuth credentials if present
    if (solrCredentials != null) {
      request.setBasicAuthCredentials(
          /* Username */ solrCredentials.get(0),
          /* Password */ solrCredentials.get(1)
      );
    }
  }

  /**
   * Constructor taking the name of the Solr core to use and an  {@link SolrClient}.
   *
   * @param solrClient The {@link SolrClient} to perform operations to the Solr core through.
   * @param collection A {@link String} containing name of the Solr core to use.
   */
  public SolrArtifactIndex(SolrClient solrClient, String collection) {
    this.solrCollection = collection;
    this.solrClient = solrClient;
    this.isInternalClient = false;
  }

  /**
   * Modifies the schema of a collection pointed to by a SolrClient, to support artifact indexing.
   */
  @Override
  public synchronized void initIndex() {
    if (getState() == ArtifactIndexState.UNINITIALIZED) {

      // TODO: Check that the core exists?
//      CoreAdminResponse response = CoreAdminRequest.getStatus(coreName, solrClient);
//      response.getCoreStatus(coreName).size() > 0;

      setState(ArtifactIndexState.INITIALIZED);
    }
  }

  /**
   * Returns information about the storage size and free space.
   *
   * @return A {@link StorageInfo}
   */
  @Override
  public StorageInfo getStorageInfo() {
    try {
      // Create new StorageInfo object
      StorageInfo info = new StorageInfo(ARTIFACT_INDEX_TYPE);

      // Retrieve Solr core metrics
      MetricsRequest.CoreMetricsRequest req = new MetricsRequest.CoreMetricsRequest();
      addSolrCredentials(req);
      MetricsResponse.CoreMetricsResponse res = req.process(solrClient);

      MetricsResponse.CoreMetrics metrics = res.getCoreMetrics(solrCollection);

      // Populate StorageInfo from Solr core metrics
      info.setName(metrics.getIndexDir());
      info.setSize(metrics.getTotalSpace());
      info.setUsed(metrics.getIndexSizeInBytes());
      info.setAvail(metrics.getUsableSpace());
      info.setPercentUsed((double) info.getUsed() / (double) info.getSize());
      info.setPercentUsedString(Math.round(100 * info.getPercentUsed()) + "%");

      // Return populated StorageInfo
      return info;
    } catch (SolrServerException | IOException e) {
      // Q: Throw or return null?
      log.error("Could not retrieve metrics from Solr", e);
      return null;
    }
  }

  public String getSolrCollection() {
    return solrCollection;
  }

  public void setSolrCollection(String collection) {
    this.solrCollection = collection;
  }

  /**
   * Returns a Solr response unchanged, if it has a zero status; throws,
   * otherwise.
   *
   * @param solrResponse A SolrResponseBase with the Solr response tobe handled.
   * @param errorMessage A String with a custom error message to be included in
   *                     the thrown exception, if necessary.
   * @return a SolrResponseBase with the passed Solr response unchanged, if it
   * has a zero status.
   * @throws SolrResponseErrorException if the passed Solr response has a
   *                                    non-zero status.
   */
  static <T extends SolrResponseBase> T handleSolrResponse(T solrResponse,
                                                           String errorMessage) throws SolrResponseErrorException {

    log.debug2("solrResponse = {}", solrResponse);

    NamedList<Object> solrResponseResponse = solrResponse.getResponse();

    // Check whether the response does indicate success.
    if (solrResponse.getStatus() == 0
        && solrResponseResponse.get("error") == null
        && solrResponseResponse.get("errors") == null) {
      // Yes: Return the successful response.
      log.debug2("solrResponse indicates success");
      return solrResponse;
    }

    // No: Report the problem.
    log.trace("solrResponse indicates failure");

    SolrResponseErrorException snzse =
        new SolrResponseErrorException(errorMessage, solrResponse);
    log.error(snzse);
    throw snzse;
  }

  @Override
  public void stop() {
    try {
      if (isInternalClient) {
        solrClient.close();
      }

      setState(ArtifactIndexState.STOPPED);
    } catch (IOException e) {
      log.error("Could not close Solr client connection", e);
    }
  }

  /**
   * Checks whether the Solr cluster is alive by calling {@code SolrClient#ping()}.
   *
   * @return
   */
  private boolean checkAlive() {
    try {
      // Create new SolrPing request and process it
      SolrPing ping = new SolrPing();
      addSolrCredentials(ping);
      SolrPingResponse response = ping.process(solrClient, solrCollection);

      // Check response for success
      handleSolrResponse(response, "Problem pinging Solr");
      return true;

    } catch (Exception e) {
      log.warn("Could not ping Solr", e);
      return false;
    }
  }

  /**
   * Returns a boolean indicating whether this artifact index is ready.
   *
   * @return a boolean with the indication.
   */
  @Override
  public boolean isReady() {
    return getState() == ArtifactIndexState.RUNNING
        || getState() == ArtifactIndexState.INITIALIZED && checkAlive();
  }

  @Override
  public void acquireVersionLock(ArtifactIdentifier.ArtifactStem stem) throws IOException {
    // Acquire the lock for this artifact stem
    try {
      versionLock.getLock(stem);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted while waiting to acquire artifact version lock");
    }

  }

  @Override
  public void releaseVersionLock(ArtifactIdentifier.ArtifactStem stem) {
    // Release the lock for the artifact stem
    versionLock.releaseLock(stem);
  }

  /**
   * Adds an artifact to the artifactIndex.
   *
   * @param artifactData An ArtifactData with the artifact to be added to the artifactIndex,.
   * @return an Artifact with the artifact indexing data.
   */
  @Override
  public Artifact indexArtifact(ArtifactData artifactData) throws IOException {
    if (artifactData == null) {
      throw new IllegalArgumentException("Null artifact data");
    }

    ArtifactIdentifier artifactId = artifactData.getIdentifier();

    if (artifactId == null) {
      throw new IllegalArgumentException("ArtifactData has null identifier");
    }

    ArtifactRepositoryState state = artifactData.getArtifactRepositoryState();

    // Create an instance of Artifact to represent the artifact
    Artifact artifact = new Artifact(
        artifactId,
        state == null ? false : state.isCommitted(),
        artifactData.getStorageUrl().toString(),
        artifactData.getContentLength(),
        artifactData.getContentDigest()
    );

    // Save the artifact collection date.
    artifact.setCollectionDate(artifactData.getCollectionDate());

    // Add the Artifact to Solr as a bean
    try {
      // Convert Artifact to SolrInputDocument using the SolrClient's DocumentObjectBinder
      SolrInputDocument doc = solrClient.getBinder().toSolrInputDocument(artifact);

      // Create an UpdateRequest to add the Solr input document
      UpdateRequest req = new UpdateRequest();
      req.add(doc);
      addSolrCredentials(req);

      handleSolrResponse(req.process(solrClient, solrCollection),
          "Problem adding artifact '" + artifact + "' to Solr");

      handleSolrResponse(handleSolrCommit(false), "Problem committing addition of "
          + "artifact '" + artifact + "' to Solr");

    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }

    // Return the Artifact added to the Solr collection
    log.debug2("Added artifact to index: {}", artifact);

    return artifact;
  }

  /**
   * Performs a Solr query against the configured Solr collection.
   *
   * @param q The {@link SolrQuery} to submit.
   * @return A {@link QueryResponse} with the response from the Solr server.
   * @throws IOException
   * @throws SolrServerException
   */
  private QueryResponse handleSolrQuery(SolrQuery q) throws IOException, SolrServerException {
    // Create a QueryRequest from the SolrQuery
    QueryRequest request = new QueryRequest(q);

    // Add Solr BasicAuth credentials if present
    addSolrCredentials(request);

    // Perform the query and return the response
    return request.process(solrClient, solrCollection);
  }

  /**
   * Provides the artifactIndex data of an artifact with a given text artifactIndex
   * identifier.
   *
   * @param artifactId A String with the artifact artifactIndex identifier.
   * @return an Artifact with the artifact indexing data.
   */
  @Override
  public Artifact getArtifact(String artifactId) throws IOException {
    if (StringUtils.isEmpty(artifactId)) {
      throw new IllegalArgumentException("Null or empty artifact ID");
    }

    // Solr query to perform
    SolrQuery q = new SolrQuery();
    q.setQuery(String.format("id:%s", artifactId));
//        q.addFilterQuery(String.format("{!term f=id}%s", artifactId));
//        q.addFilterQuery(String.format("committed:%s", true));

    try {
      // Submit the Solr query and get results as Artifact objects
      QueryResponse response =
          handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

      // Deserialize results into a list of Artifacts
      List<Artifact> artifacts = response.getBeans(Artifact.class);

      switch (artifacts.size()) {
        case 0:
          // Expected at least one match
          log.debug("Artifact not found [artifactId: {}]", artifactId);
          return null;

        case 1:
          // Return the artifact
          return artifacts.get(0);

        default:
          // This should never happen
          log.warn("Unexpected number of Solr documents in response: {}", artifacts.size());
          throw new RuntimeException("Unexpected number of Solr documents in response");
      }

    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException("Solr error", e);
    }
  }

  /**
   * Provides the artifactIndex data of an artifact with a given artifactIndex identifier
   * UUID.
   *
   * @param artifactId An UUID with the artifact artifactIndex identifier.
   * @return an Artifact with the artifact indexing data.
   */
  @Override
  public Artifact getArtifact(UUID artifactId) throws IOException {
    if (artifactId == null) {
      throw new IllegalArgumentException("Null UUID");
    }

    return this.getArtifact(artifactId.toString());
  }

  /**
   * Commits to the artifactIndex an artifact with a given text artifactIndex identifier.
   *
   * @param artifactId A String with the artifact artifactIndex identifier.
   * @return an Artifact with the committed artifact indexing data.
   */
  @Override
  public Artifact commitArtifact(String artifactId) throws IOException {
    if (!artifactExists(artifactId)) {
      log.debug("Artifact does not exist [artifactId: {}]", artifactId);
      return null;
    }

    // Partial document to perform Solr document update
    SolrInputDocument document = new SolrInputDocument();
    document.addField("id", artifactId);

    // Perform an atomic update (see https://lucene.apache.org/solr/guide/6_6/updating-parts-of-documents.html) by
    // setting the type of field modification and its replacement value
    Map<String, Object> fieldModifier = new HashMap<>();
    fieldModifier.put("set", true);
    document.addField("committed", fieldModifier);

    try {
      // Create an update request for this document
      UpdateRequest request = new UpdateRequest();
      request.add(document);
      addSolrCredentials(request);

      // Update the artifact.
      handleSolrResponse(request.process(solrClient, solrCollection), "Problem adding document '"
          + document + "' to Solr");

      // Commit changes
      handleSolrResponse(handleSolrCommit(false), "Problem committing Solr changes");
    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException("Solr error", e);
    }

    // Return updated Artifact
    return getArtifact(artifactId);
  }

  /**
   * Performs a commit on the Solr collection used by this artifact index.
   *
   * @return An {@link UpdateResponse} containing the response from the Solr server.
   * @throws IOException
   * @throws SolrServerException
   */
  private UpdateResponse handleSolrCommit(boolean hardCommit) throws IOException, SolrServerException {
    // Update request to commit
    UpdateRequest req = new UpdateRequest();
    req.setAction(UpdateRequest.ACTION.COMMIT, true, true, !hardCommit);

    // Add Solr credentials if present
    addSolrCredentials(req);

    // Perform commit and return response
    return req.process(solrClient, solrCollection);
  }

  /**
   * Commits to the artifactIndex an artifact with a given artifactIndex identifier UUID.
   *
   * @param artifactId An UUID with the artifact artifactIndex identifier.
   * @return an Artifact with the committed artifact indexing data.
   */
  @Override
  public Artifact commitArtifact(UUID artifactId) throws IOException {
    if (artifactId == null) {
      throw new IllegalArgumentException("Null UUID");
    }

    return commitArtifact(artifactId.toString());
  }

  /**
   * Removes from the artifactIndex an artifact with a given text artifactIndex identifier.
   *
   * @param artifactId A String with the artifact artifactIndex identifier.
   * @throws IOException if Solr reports problems.
   */
  @Override
  public boolean deleteArtifact(String artifactId) throws IOException {
    if (StringUtils.isEmpty(artifactId)) {
      throw new IllegalArgumentException("Null or empty identifier");
    }

    if (artifactExists(artifactId)) {
      try {
        // Create an Solr update request
        UpdateRequest request = new UpdateRequest();
        request.deleteById(artifactId);
        addSolrCredentials(request);

        // Remove Solr document for this artifact
        handleSolrResponse(request.process(solrClient, solrCollection), "Problem deleting "
            + "artifact '" + artifactId + "' from Solr");

        // Commit changes
        handleSolrResponse(handleSolrCommit(false), "Problem committing deletion of "
            + "artifact '" + artifactId + "' from Solr");

        // Return true to indicate success
        return true;
      } catch (SolrResponseErrorException | SolrServerException e) {
        log.error("Could not remove artifact from Solr index [artifactId: {}]", artifactId, e);
        throw new IOException(
            String.format("Could not remove artifact from Solr index [artifactId: %s]", artifactId), e
        );
      }
    } else {
      // Artifact not found in index; nothing deleted
      log.debug("Artifact not found [artifactId: {}]", artifactId);
      return false;
    }
  }

  /**
   * Removes from the artifactIndex an artifact with a given artifactIndex identifier UUID.
   *
   * @param artifactId A String with the artifact artifactIndex identifier.
   * @return <code>true</code> if the artifact was removed from in the artifactIndex,
   * <code>false</code> otherwise.
   */
  @Override
  public boolean deleteArtifact(UUID artifactId) throws IOException {
    if (artifactId == null) {
      throw new IllegalArgumentException("Null UUID");
    }

    return deleteArtifact(artifactId.toString());
  }

  /**
   * Provides an indication of whether an artifact with a given text artifactIndex
   * identifier exists in the artifactIndex.
   *
   * @param artifactId A String with the artifact identifier.
   * @return <code>true</code> if the artifact exists in the artifactIndex,
   * <code>false</code> otherwise.
   */
  @Override
  public boolean artifactExists(String artifactId) throws IOException {
    return getArtifact(artifactId) != null;
  }

  @Override
  public Artifact updateStorageUrl(String artifactId, String storageUrl) throws IOException {
    if (StringUtils.isEmpty(artifactId)) {
      throw new IllegalArgumentException("Invalid artifact ID");
    }

    if (StringUtils.isEmpty(storageUrl)) {
      throw new IllegalArgumentException("Invalid storage URL: Must not be null or empty");
    }

    if (!artifactExists(artifactId)) {
      log.debug("Artifact does not exist [artifactId: {}]", artifactId);
      return null;
    }

    // Perform a partial update of an existing Solr document
    SolrInputDocument document = new SolrInputDocument();
    document.addField("id", artifactId);

    // Specify type of field modification, field name, and replacement value
    Map<String, Object> fieldModifier = new HashMap<>();
    fieldModifier.put("set", storageUrl);
    document.addField("storageUrl", fieldModifier);

    try {
      // Create an update request for this document
      UpdateRequest request = new UpdateRequest();
      request.add(document);
      addSolrCredentials(request);

      // Update the field
      handleSolrResponse(request.process(solrClient, solrCollection), "Problem adding document '"
          + document + "' to Solr");

      handleSolrResponse(handleSolrCommit(false), "Problem committing addition of "
          + "document '" + document + "' to Solr");
    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }

    // Return updated Artifact
    return getArtifact(artifactId);
  }

  /**
   * Returns a boolean indicating whether the Solr index is empty.
   *
   * @return A boolean indicating whether the Solr index is empty.
   * @throws IOException
   * @throws SolrServerException
   */
  private boolean isEmptySolrIndex()
      throws SolrResponseErrorException, IOException, SolrServerException {

    // Match all documents but set the number of documents to be returned to zero
    SolrQuery q = new SolrQuery();
    q.setQuery("*:*");
    q.setRows(0);

    // Perform the query
    QueryResponse result =
        handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

    // Check whether we matched zero Solr documents
    return result.getResults().getNumFound() == 0;
  }

  /**
   * Provides the collection identifiers of the committed artifacts in the artifactIndex.
   *
   * @return An {@code Iterator<String>} with the artifactIndex committed artifacts
   * collection identifiers.
   */
  @Override
  public Iterable<String> getCollectionIds() throws IOException {
    try {
      // Cannot perform facet field query on an empty Solr index
      if (isEmptySolrIndex()) {
        return IterableUtils.emptyIterable();
      }

      // Perform a Solr facet query on the collection ID field
      SolrQuery q = new SolrQuery();
      q.setQuery("*:*");
      q.setRows(0);

      q.addFacetField("collection");
      q.setFacetLimit(-1); // Unlimited

      // Get the facet field from the result
      QueryResponse result =
          handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

      FacetField ff = result.getFacetField("collection");

      if (log.isDebug2Enabled()) {
        log.debug2(
            "FacetField: [getName: {}, getValues: {}, getValuesCount: {}]",
            ff.getName(),
            ff.getValues(),
            ff.getValueCount()
        );
      }

      // Transform facet field value names into iterable
      return IteratorUtils.asIterable(
          ff.getValues().stream()
              .filter(x -> x.getCount() > 0)
              .map(FacetField.Count::getName)
              .sorted()
              .iterator()
      );

    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }
  }

  /**
   * Returns a list of Archival Unit IDs (AUIDs) in this LOCKSS repository collection.
   *
   * @param collection A {@code String} containing the LOCKSS repository collection ID.
   * @return A {@code Iterator<String>} iterating over the AUIDs in this LOCKSS repository collection.
   * @throws IOException if Solr reports problems.
   */
  @Override
  public Iterable<String> getAuIds(String collection) throws IOException {
    // We use a Solr facet query but another option is Solr groups. I believe faceting is better in this case,
    // because we are not actually interested in the Solr documents - only aggregate information about them.
    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    q.addFilterQuery(String.format("{!term f=collection}%s", collection));

    q.setRows(0);

    q.addFacetField("auid");
    q.setFacetLimit(-1); // Unlimited

    try {
      QueryResponse response =
          handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

      FacetField ff = response.getFacetField("auid");

      return IteratorUtils.asIterable(
          ff.getValues().stream()
              .filter(x -> x.getCount() > 0)
              .map(FacetField.Count::getName)
              .sorted()
              .iterator()
      );
    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }
  }

  /**
   * Returns the committed artifacts of the latest version of all URLs, from a specified Archival Unit and collection.
   *
   * @param collection A {@code String} containing the collection ID.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @return An {@code Iterator<Artifact>} containing the latest version of all URLs in an AU.
   * @throws IOException if Solr reports problems.
   */
  @Override
  public Iterable<Artifact> getArtifacts(String collection, String auid, boolean includeUncommitted) throws IOException {
    // Create Solr query
    SolrQuery q = new SolrQuery();

    // Match everything
    q.setQuery("*:*");

    // Filter by committed status equal to true?
    if (!includeUncommitted) {
      q.addFilterQuery(String.format("committed:%s", true));
    }

    // Additional filter queries
    q.addFilterQuery(String.format("{!term f=collection}%s", collection));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addSort(SORTURI_ASC);
    q.addSort(VERSION_DESC);

    // Ensure the result is not empty for the collapse filter query
    if (isEmptyResult(q)) {
      log.debug2("Solr returned null result set after filtering by [committed: true, collection: {}, auid: {}]",
          collection,
          auid
      );

      return IterableUtils.emptyIterable();
    }

    // Add a collapse filter
    q.addFilterQuery("{!collapse field=uri max=version}");

    // Perform collapse filter query and return result
    return IteratorUtils.asIterable(
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q));
  }

  /**
   * Returns the artifacts of all versions of all URLs, from a specified Archival Unit and collection.
   *
   * @param collection         A String with the collection identifier.
   * @param auid               A String with the Archival Unit identifier.
   * @param includeUncommitted A {@code boolean} indicating whether to return all the versions among both committed and uncommitted
   *                           artifacts.
   * @return An {@code Iterator<Artifact>} containing the artifacts of all version of all URLs in an AU.
   */
  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String collection, String auid, boolean includeUncommitted) throws IOException {
    // Create a Solr query
    SolrQuery q = new SolrQuery();

    // Match everything
    q.setQuery("*:*");

    // Add filter by committed status equal to true if we do *not* want to include uncommitted
    if (!includeUncommitted) {
      q.addFilterQuery(String.format("committed:%s", true));
    }

    // Additional filter queries
    q.addFilterQuery(String.format("{!term f=collection}%s", collection));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addSort(SORTURI_ASC);
    q.addSort(VERSION_DESC);

    return IteratorUtils.asIterable(
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q));
  }

  /**
   * Returns the committed artifacts of the latest version of all URLs matching a prefix, from a specified Archival
   * Unit and collection.
   *
   * @param collection A {@code String} containing the collection ID.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @param prefix     A {@code String} containing a URL prefix.
   * @return An {@code Iterator<Artifact>} containing the latest version of all URLs matching a prefix in an AU.
   * @throws IOException if Solr reports problems.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithPrefix(String collection, String auid, String prefix) throws IOException {

    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=collection}%s", collection));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addFilterQuery(String.format("{!prefix f=uri}%s", prefix));

    q.addSort(SORTURI_ASC);
    q.addSort(VERSION_DESC);

    // Ensure the result is not empty for the collapse filter query
    if (isEmptyResult(q)) {
      log.debug2(
          "Solr returned null result set after filtering by (committed: true, collection: {}, auid:{}, uri (prefix): {})",
          collection, auid, prefix
      );

      return IterableUtils.emptyIterable();
    }

    // Add collapse filter query
    q.addFilterQuery("{!collapse field=uri max=version}");

    // Perform collapse filter query and return result
    return IteratorUtils.asIterable(
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q));
  }

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified Archival Unit and
   * collection.
   *
   * @param collection A String with the collection identifier.
   * @param auid       A String with the Archival Unit identifier.
   * @param prefix     A String with the URL prefix.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix from an AU.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithPrefixAllVersions(String collection, String auid, String prefix) throws IOException {

    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=collection}%s", collection));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addFilterQuery(String.format("{!prefix f=uri}%s", prefix));

    q.addSort(SORTURI_ASC);
    q.addSort(VERSION_DESC);

    return IteratorUtils.asIterable(
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q));
  }

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified collection.
   *
   * @param collection A String with the collection identifier.
   * @param urlPrefix     A String with the URL prefix.
   * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
   *                   versions of an artifact.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String collection, String urlPrefix,
                                                                ArtifactVersions versions) throws IOException {

    if (!(versions == ArtifactVersions.ALL ||
        versions == ArtifactVersions.LATEST)) {
      throw new IllegalArgumentException("Versions must be ALL or LATEST");
    }

    if (collection == null) {
      throw new IllegalArgumentException("Collection is null");
    }

    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=collection}%s", collection));

    if (urlPrefix != null) {
      q.addFilterQuery(String.format("{!prefix f=uri}%s", urlPrefix));
    }

//    // This gives us the right results but does not work with CursorMark-based
//    // pagination despite group.main=true. It is left here as a reminder that
//    // we already tried this approach.
//    q.set("group", true);
//    q.set("group.func", "concat(collection,auid,uri)");
//    q.set("group.sort", "version desc");
//    q.set("group.limit", 1);
//    q.set("group.main", true);

    q.addSort(SORTURI_ASC);
    q.addSort(AUID_ASC);
    q.addSort(VERSION_DESC);

    Iterator<Artifact> allVersionsIterator =
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q);

    if (versions == ArtifactVersions.LATEST) {
      // Convert Iterator<Artifact> to Stream<Artifact>
      Stream<Artifact> allVersions = StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(allVersionsIterator, Spliterator.ORDERED), false);

      // Group by (Collection, AUID, URL) then pick highest version from each group
      Stream<Artifact> latestVersions = allVersions
          .collect(Collectors.groupingBy(
              artifact -> artifact.getIdentifier().getArtifactStem(),
              Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))))
          .values()
          .stream()
          .filter(Optional::isPresent)
          .map(Optional::get);

      // Sort artifacts and return as Iterable<Artifact>
      return IteratorUtils.asIterable(latestVersions
          .sorted(ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION)
          .iterator());
    }

    return IteratorUtils.asIterable(allVersionsIterator);
  }

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified Archival Unit and collection.
   *
   * @param collection A {@code String} with the collection identifier.
   * @param auid       A {@code String} with the Archival Unit identifier.
   * @param url        A {@code String} with the URL to be matched.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL from an
   * Archival Unit.
   */
  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String collection, String auid, String url) throws IOException {
    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=collection}%s", collection));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addFilterQuery(String.format("{!term f=uri}%s", url));

    q.addSort(SORTURI_ASC);
    q.addSort(VERSION_DESC);

    return IteratorUtils.asIterable(
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q));
  }

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified collection.
   *
   * @param collection A {@code String} with the collection identifier.
   * @param url        A {@code String} with the URL to be matched.
   * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
   *                   versions of an artifact.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithUrlFromAllAus(String collection, String url, ArtifactVersions versions)
      throws IOException {

    if (!(versions == ArtifactVersions.ALL ||
        versions == ArtifactVersions.LATEST)) {
      throw new IllegalArgumentException("Versions must be ALL or LATEST");
    }

    if (collection == null || url == null) {
      throw new IllegalArgumentException("Collection or URL is null");
    }

    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=collection}%s", collection));
    q.addFilterQuery(String.format("{!term f=uri}%s", url));

//    // This gives us the right results but does not work with CursorMark-based
//    // pagination despite group.main=true. It is left here as a reminder that
//    // we already tried this approach.
//    q.set("group", true);
//    q.set("group.func", "concat(collection,auid,uri)");
//    q.set("group.sort", "version desc");
//    q.set("group.limit", 1);
//    q.set("group.main", true);

    q.addSort(SORTURI_ASC);
    q.addSort(AUID_ASC);
    q.addSort(VERSION_DESC);

    Iterator<Artifact> allVersionsIterator =
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q);

    if (versions == ArtifactVersions.LATEST) {
      // Convert Iterator<Artifact> to Stream<Artifact>
      Stream<Artifact> allVersions = StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(allVersionsIterator, Spliterator.ORDERED), false);

      // Group by (Collection, AUID, URL) then pick highest version from each group
      Stream<Artifact> latestVersions = allVersions
          .collect(Collectors.groupingBy(
              artifact -> artifact.getIdentifier().getArtifactStem(),
              Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))))
          .values()
          .stream()
          .filter(Optional::isPresent)
          .map(Optional::get);

      // Sort artifacts and return as Iterable<Artifact>
      return IteratorUtils.asIterable(latestVersions
          .sorted(ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION)
          .iterator());
    }

    return IteratorUtils.asIterable(allVersionsIterator);
  }

  /**
   * Returns the artifact of the latest version of given URL, from a specified Archival Unit and collection.
   *
   * @param collection         A {@code String} containing the collection ID.
   * @param auid               A {@code String} containing the Archival Unit ID.
   * @param url                A {@code String} containing a URL.
   * @param includeUncommitted A {@code boolean} indicating whether to return the latest version among both committed and uncommitted
   *                           artifacts of a URL.
   * @return An {@code Artifact} representing the latest version of the URL in the AU.
   * @throws IOException if Solr reports problems.
   */
  @Override
  public Artifact getArtifact(String collection, String auid, String url, boolean includeUncommitted) throws IOException {
    // Solr query to perform
    SolrQuery q = new SolrQuery();
    q.setQuery("*:*");

    if (!includeUncommitted) {
      // Restrict to only committed artifacts
      q.addFilterQuery(String.format("committed:%s", true));
    }

    q.addFilterQuery(String.format("{!term f=collection}%s", collection));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addFilterQuery(String.format("{!term f=uri}%s", url));

    // Ensure the result is not empty for the collapse filter query
    if (isEmptyResult(q)) {
      log.debug2(
          "Solr returned null result set after filtering by [collection: {}, auid: {}, uri: {}]",
          collection, auid, url
      );

      return null;
    }

    // FIXME: Is there potential for a race condition between
    //  isEmptyResult() call and performing the collapse filter query?

    // Perform a collapse filter query (must have documents in result set to operate on)
    q.addFilterQuery("{!collapse field=uri max=version}");

    // Get results as an Iterator
    Iterator<Artifact> result =
        new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q);

    // Return the latest artifact
    if (result.hasNext()) {
      Artifact artifact = result.next();

      if (result.hasNext()) {
        // This should never happen if Solr is working correctly
        String errMsg = "More than one artifact returned for the latest version of (Collection, AUID, URL)!";
        log.error(errMsg);
        throw new RuntimeException(errMsg);
      }

      return artifact;
    }

    return null;
  }

  /**
   * Returns the artifact of a given version of a URL, from a specified Archival Unit and collection.
   *
   * @param collection         A String with the collection identifier.
   * @param auid               A String with the Archival Unit identifier.
   * @param url                A String with the URL to be matched.
   * @param version            A String with the version.
   * @param includeUncommitted A boolean with the indication of whether an uncommitted artifact may be returned.
   * @return The {@code Artifact} of a given version of a URL, from a specified AU and collection.
   */
  @Override
  public Artifact getArtifactVersion(String collection, String auid, String url, Integer version, boolean includeUncommitted) throws IOException {
    SolrQuery q = new SolrQuery();

    q.setQuery("*:*");

    // Only filter by commit status when no uncommitted artifact is to be returned.
    if (!includeUncommitted) {
      q.addFilterQuery(String.format("committed:%s", true));
    }

    q.addFilterQuery(String.format("{!term f=collection}%s", collection));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addFilterQuery(String.format("{!term f=uri}%s", url));
    q.addFilterQuery(String.format("version:%s", version));

    Iterator<Artifact> result = new SolrQueryArtifactIterator(solrCollection, solrClient, solrCredentials, q);

    if (result.hasNext()) {
      Artifact artifact = result.next();

      if (result.hasNext()) {
        log.warn("More than one artifact found having same (Collection, AUID, URL, Version)");
      }

      return artifact;
    }

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
  public Long auSize(String collection, String auid) throws IOException {
    // Create Solr query
    SolrQuery q = new SolrQuery();
    q.setQuery("*:*");
    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=collection}%s", collection));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));

    // Ensure the result is non-empty for the collapse filter query next
    if (isEmptyResult(q)) {
      log.debug2(
          "No artifacts in AU [committed: true, collection: {}, auid: {}]",
          collection,
          auid
      );

      return 0L;
    }

    // FIXME: Is there potential for a race condition here between checking
    //  the results of the query and performing the collapse filter?

    // Setup the collapse filter query
    q.addFilterQuery("{!collapse field=uri max=version}");
    q.setGetFieldStatistics(true);
    q.setGetFieldStatistics("contentLength");
    q.setRows(0);

    try {
      // Query Solr and get
      QueryResponse response =
          handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

      // Get the contentLength field stats
      FieldStatsInfo contentLengthStats = response.getFieldStatsInfo().get("contentLength");

      // Sum the contentLengths and return
      return ((Double) contentLengthStats.getSum()).longValue();
    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException("Solr error", e);
    }
  }

  private boolean isEmptyResult(SolrQuery q) throws IOException {
    try {
      // Set the number of matched rows to return to zero
      q.setRows(0);

      // Perform query and find the number of documents
      QueryResponse response =
          handleSolrResponse(handleSolrQuery(q), "Problem performing Solr query");

      // Check that the query matched nothing
      return response.getResults().getNumFound() == 0;
    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException(String.format("Caught SolrServerException attempting to execute Solr query: %s", q));
    }
  }
}
