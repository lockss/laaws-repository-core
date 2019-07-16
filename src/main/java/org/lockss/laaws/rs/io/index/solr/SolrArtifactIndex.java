/*
 * Copyright (c) 2017-2019, Board of Trustees of Leland Stanford Jr. University,
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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.log.L4JLogger;

import java.io.IOException;
import java.util.*;

/**
 * An Apache Solr implementation of ArtifactIndex.
 */
public class SolrArtifactIndex implements ArtifactIndex {
  private final static L4JLogger log = L4JLogger.getLogger();

  // The prefix of the name of the field that holds the LOCKSS version of the
  // Solr schema.
  private static final String lockssSolrSchemaVersionFieldNamePrefix =
      "solrSchemaLockssVersion_";

    private static final long DEFAULT_TIMEOUT = 10;
    private final SolrClient solr;

    private static final SolrQuery.SortClause SORTURI_ASC = new SolrQuery.SortClause("sortUri", SolrQuery.ORDER.asc);
    private static final SolrQuery.SortClause VERSION_DESC = new SolrQuery.SortClause("version", SolrQuery.ORDER.desc);
    private static final SolrQuery.SortClause AUID_ASC = new SolrQuery.SortClause("auid", SolrQuery.ORDER.asc);
    private boolean initialized = false;

    // The version of the schema to be targeted by this indexer.
    //
    // After this indexer has started successfully, this is the version of the
    // schema that will be in place, as long as the schema version prior to
    // starting the indexer was not higher already.
    private final int targetSchemaVersion = 2;

    /**
     * Constructor. Creates and uses a HttpSolrClient from a Solr collection URL.
     *
     * @param solrCollectionUrl
     *          A {@code String} containing the URL to a Solr collection or core.
     */
    public SolrArtifactIndex(String solrCollectionUrl) {
        // Get a handle to the Solr collection
        this(new HttpSolrClient.Builder(solrCollectionUrl).build());
    }

    /**
     * Constructor that uses a given SolrClient.
     *
     * @param client
     *          A {@code SolrClient} to use to artifactIndex artifacts.
     */
    public SolrArtifactIndex(SolrClient client) {
        this.solr = client;
        init();
    }

    /**
     * Modifies the schema of a collection pointed to by a SolrClient, to support artifact indexing.
     *
     */
    private synchronized void init() {
      if (!initialized) {
	// Update the schema, if necessary.
	updateIfNecessary(targetSchemaVersion);

	initialized = true;
      }
    }

  /**
   * Updates the schema, if necessary.
   * 
   * @param targetSchemaVersion An int with the LOCKSS version of the Solr
   *                            schema to be updated.
   */
  private void updateIfNecessary(int targetSchemaVersion) {
    log.debug2("targetSchemaVersion = " + targetSchemaVersion);

    // Find the current schema version.
    int existingSchemaVersion = getExistingLockssSchemaVersion();
    log.trace("existingSchemaVersion = {}", existingSchemaVersion);

    // Check whether the existing schema is newer than what this version of
    // the service expects.
    if (targetSchemaVersion < existingSchemaVersion) {
      // Yes: Report the problem.
      throw new RuntimeException("Existing LOCKSS Solr schema version is "
	  + existingSchemaVersion
	  + ", which is higher than the target schema version "
	  + targetSchemaVersion
	  + " for this service. Possibly caused by service downgrade.");
    }

    // Check whether the schema needs to be updated beyond the existing schema
    // version.
    if (targetSchemaVersion > existingSchemaVersion) {
      // Yes.
      log.trace("Schema needs to be updated from existing version "
	  + existingSchemaVersion + " to new version " + targetSchemaVersion);

      // Update the schema and get the last updated version.
      int lastUpdatedVersion =
	  updateSchema(existingSchemaVersion, targetSchemaVersion);
      log.trace("lastRecordedVersion = {}", lastUpdatedVersion);

      // Record it in the field with the schema version.
      updateSolrLockssSchemaVersion(lastUpdatedVersion);

      log.info("Schema has been updated to LOCKSS version {}",
	  lastUpdatedVersion);
    } else {
      // No.
      log.info("Schema is up-to-date at LOCKSS version {}",
	  existingSchemaVersion);
    }

    log.debug2("Done.");
  }

  /**
   * Provides the LOCKSS version of the current Solr schema.
   *
   * @return an int with the LOCKSS version of the current Solr schema.
   */
  private int getExistingLockssSchemaVersion() {
    log.debug2("Invoked");
    int result = getSolrSchemaLockssVersionFromField();
    log.trace("result = {}", result);

    // Check whether the schema is at version 1.
    if (result == 0 && hasSolrField("collection", "string")
	&& hasSolrField("auid", "string")
	&& hasSolrField("uri", "string")
	&& hasSolrField("committed", "boolean")
	&& hasSolrField("storageUrl", "string")
	&& hasSolrField("contentLength", "plong")
	&& hasSolrField("contentDigest", "string")
	&& hasSolrField("version", "pint")
	&& hasSolrField("collectionDate", "long")) {
      // Yes: The schema is at version 1.
      result = 1;
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Provides the LOCKSS version of the current Solr schema from the Solr field.
   *
   * @return an int with the LOCKSS version of the current Solr schema.
   */
  private int getSolrSchemaLockssVersionFromField() {
    log.debug2("Invoked");
    int result = 0;

    // Loop through all the Solr schema fields.
    for (Map<String, Object> field : getSolrSchemaFields()) {
      String fieldName = (String)field.get("name");
      log.trace("fieldName = {}", fieldName);

      // Check whether the name of the field matches the name of the Solr schema
      // LOCKSS version field.
      if (fieldName.startsWith(lockssSolrSchemaVersionFieldNamePrefix)) {
	// Yes: Get the version.
	result = Integer.valueOf(fieldName.substring(
	    lockssSolrSchemaVersionFieldNamePrefix.length()));
	break;
      }
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Provides the definitions of the Solr schema fields.
   *
   * @return a List<Map<String, Object>> with the definitions of the Solr schema
   *         fields.
   */
  private List<Map<String, Object>> getSolrSchemaFields() {
    log.debug2("Invoked");
    List<Map<String, Object>> result = null;

    try {
      SchemaRequest.Fields fieldsSchemaRequest = new SchemaRequest.Fields();
      SchemaResponse.FieldsResponse fieldsResponse =
	  fieldsSchemaRequest.process(solr);
      log.trace("fieldsResponse = {}", fieldsResponse);

      result = fieldsResponse.getFields();
    } catch (SolrServerException sse) {
      String errorMessage = "Exception caught locating Solr schema field";
      log.error(errorMessage, sse);
      throw new RuntimeException(errorMessage, sse);
    } catch (IOException ioe) {
      String errorMessage = "Exception caught locating Solr schema field";
      log.error(errorMessage, ioe);
      throw new RuntimeException(errorMessage, ioe);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Provides the attributes of a new field.
   * 
   * @param fieldName       A String with the name of the field.
   * @param fieldType       A String with the type of the field.
   * @param fieldAttributes A Map<String,Object> with overriding field
   *                        attributes, if any.
   * @return a Map<String, Object> with the attributes of the new field.
   */
  private Map<String, Object> getNewFieldAttributes(String fieldName,
      String fieldType, Map<String,Object> fieldAttributes) {
    // https://lucene.apache.org/solr/guide/7_2/defining-fields.html#DefiningFields-OptionalFieldTypeOverrideProperties
    Map<String, Object> newFieldAttributes = new LinkedHashMap<>();
    newFieldAttributes.put("name", fieldName);
    newFieldAttributes.put("type", fieldType);
    newFieldAttributes.put("indexed", true);
    newFieldAttributes.put("stored", true);
    newFieldAttributes.put("multiValued", false);
    newFieldAttributes.put("required", true);

    // Allow default attributes to be overridden if field attributes were
    // provided.
    if (fieldAttributes != null) {
      newFieldAttributes.putAll(fieldAttributes);
    }

    return newFieldAttributes;
  }

  /**
   * Provides an indication of whether a Solr schema field exists.
   *
   * @param fieldName       A String with the name of the field.
   * @param fieldType       A String with the type of the field.
   * @param required        A boolean with the indication of whether the field
   *                        is required.
   * @return a boolean with the indication.
     */
  private boolean hasSolrField(String fieldName, String fieldType) {
    Map<String, Object> newFieldAttributes =
	getNewFieldAttributes(fieldName, fieldType, null);

    // Get a list of fields from the Solr schema.
    SchemaRequest.Fields fieldsReq = new SchemaRequest.Fields();

    try {
      SchemaResponse.FieldsResponse fieldsResponse = fieldsReq.process(solr);
      log.trace("fieldsResponse = {}", fieldsResponse);

      return fieldsResponse.getFields().contains(newFieldAttributes);
    } catch (SolrServerException sse) {
      String errorMessage = "Exception caught locating Solr schema field";
      log.error(errorMessage, sse);
      throw new RuntimeException(errorMessage, sse);
    } catch (IOException ioe) {
      String errorMessage = "Exception caught locating Solr schema field";
      log.error(errorMessage, ioe);
      throw new RuntimeException(errorMessage, ioe);
    }
  }

  /**
   * Updates the schema to the target version.
   * 
   * @param existingVersion An int with the existing schema version.
   * @param finalVersion    An int with the version of the schema to which the
   *                        schema is to be updated.
   * @return an int with the highest update version recorded in the schema.
   */
  private int updateSchema(int existingVersion, int finalVersion) {
    log.debug2("existingVersion = {}", existingVersion);
    log.debug2("finalVersion = {}", finalVersion);

    int lastUpdatedVersion = existingVersion;

    // Loop through all the versions to be updated to reach the targeted
    // version.
    for (int from = existingVersion; from < finalVersion; from++) {
      log.trace("Updating from version {}...", from);

      // Perform the appropriate update of the Solr schema for this version.
      updateSchemaToVersion(from + 1);

      // Remember the current schema version.
      lastUpdatedVersion = from + 1;
      log.debug("Solr Schema updated to LOCKSS version {}", lastUpdatedVersion);
    }

    log.debug2("lastRecordedVersion = {}", lastUpdatedVersion);
    return lastUpdatedVersion;
  }

  /**
   * Updates the Solr schema to a given LOCKSS version.
   * 
   * @param schemaVersion An int with the LOCKSS version of the schema to which
   *                      the Solr schema is to be updated.
   */
  private void updateSchemaToVersion(int schemaVersion) {
    log.debug2("schemaVersion = {}", schemaVersion);

    // Perform the appropriate Solr schema update for this version.
    try {
      if (schemaVersion == 1) {
	createSolrField(solr, "collection", "string");
	createSolrField(solr, "auid", "string");
	createSolrField(solr, "uri", "string");
	createSolrField(solr, "committed", "boolean");
	createSolrField(solr, "storageUrl", "string");
	createSolrField(solr, "contentLength", "plong");
	createSolrField(solr, "contentDigest", "string");
	createSolrField(solr, "version", "pint");
	createSolrField(solr, "collectionDate", "long");
      } else if (schemaVersion == 2) {
	updateSchemaFrom1To2();
      } else {
	throw new RuntimeException("Non-existent method to update the schema "
	    + "to version " + schemaVersion + ".");
      }
    } catch (SolrServerException sse) {
	String errorMessage = "Exception caught updating Solr schema to LOCKSS "
	    + "version " + schemaVersion;
	log.error(errorMessage, sse);
	throw new RuntimeException(errorMessage, sse);
    } catch (IOException ioe) {
	String errorMessage = "Exception caught updating Solr schema to LOCKSS "
	    + "version " + schemaVersion;
	log.error(errorMessage, ioe);
	throw new RuntimeException(errorMessage, ioe);
    }
  }

  /**
   * Updates the Solr schema from LOCKSS version 1 to 2.
   */
  private void updateSchemaFrom1To2() {
    log.debug2("Invoked");

    try {
      // Create the new field in the schema.
      createSolrField(solr, "sortUri", "string");

      // Loop through all the documents in the index.
      SolrQuery q = new SolrQuery().setQuery("*:*");

      for (Artifact artifact : IteratorUtils.asIterable(query(q))) {
	// Initialize a document with the artifact identifier.
	SolrInputDocument document = new SolrInputDocument();
	document.addField("id", artifact.getId());

	// Add the new field value.
	Map<String, Object> fieldModifier = new HashMap<>();
	fieldModifier.put("set", artifact.getSortUri());
	document.addField("sortUri", fieldModifier);
	log.trace("document = {}", document);

	try {
	  // Add the document with the new field.
	  this.solr.add(document);
	  this.solr.commit();
	} catch (SolrServerException e) {
	  throw new IOException(e);
	}
      }
    } catch (SolrServerException sse) {
	String errorMessage =
	    "Exception caught updating Solr schema to LOCKSS version 2";
	log.error(errorMessage, sse);
	throw new RuntimeException(errorMessage, sse);
    } catch (IOException ioe) {
	String errorMessage =
	    "Exception caught updating Solr schema to LOCKSS version 2";
	log.error(errorMessage, ioe);
	throw new RuntimeException(errorMessage, ioe);
    }

    log.debug2("Done");
  }

  /**
   * Updates the Solr schema LOCKSS version field in the index.
   *
   * @param schemaVersion An int with the LOCKSS version of the Solr schema.
   */
  public void updateSolrLockssSchemaVersion(int schemaVersion) {
    log.debug2("schemaVersion = {}", schemaVersion);

    Map<String, Object> newFieldAttributes = new LinkedHashMap<>();
    newFieldAttributes.put("name",
	lockssSolrSchemaVersionFieldNamePrefix + schemaVersion);
    newFieldAttributes.put("type", "int");
    newFieldAttributes.put("indexed", true);
    newFieldAttributes.put("stored", true);
    newFieldAttributes.put("multiValued", false);
    newFieldAttributes.put("required", false);

    try {
      // Loop through all the Solr schema fields.
      for (Map<String, Object> field : getSolrSchemaFields()) {
	String fieldName = (String)field.get("name");
	log.trace("fieldName = {}", fieldName);

        // Check whether the name of the field matches the name of the Solr
        // schema LOCKSS version field.
	if (fieldName.startsWith(lockssSolrSchemaVersionFieldNamePrefix)) {
	  // Delete the existing field.
	  SchemaRequest.DeleteField deleteFieldRequest =
	      new SchemaRequest.DeleteField(fieldName);
	  SchemaResponse.UpdateResponse deleteFieldResponse =
	      deleteFieldRequest.process(solr);
	  log.trace("deleteFieldResponse = {}", deleteFieldResponse);
	  break;
	}
      }

      // Add the field for the Solr schema LOCKSS version.
      log.debug("Adding field to Solr schema: {}", newFieldAttributes);
      SchemaRequest.AddField addFieldReq =
	  new SchemaRequest.AddField(newFieldAttributes);
      SchemaResponse.UpdateResponse addFieldResponse =addFieldReq.process(solr);
      log.trace("addFieldResponse = {}", addFieldResponse);
    } catch (SolrServerException sse) {
	String errorMessage = "Exception caught updating Solr schema LOCKSS "
	    + "version to " + schemaVersion;
	log.error(errorMessage, sse);
	throw new RuntimeException(errorMessage, sse);
    } catch (IOException ioe) {
	String errorMessage = "Exception caught updating Solr schema LOCKSS "
	    + "version to " + schemaVersion;
	log.error(errorMessage, ioe);
	throw new RuntimeException(errorMessage, ioe);
    }

    log.debug2("Done");
  }

  /**
   * Checks whether the Solr cluster is alive by calling {@code SolrClient#ping()}.
   *
   * @return
   */
  private boolean checkAlive() {
    try {
      solr.ping();
      return true;
    } catch (Exception e) {
      log.warn(String.format("Could not ping Solr: %s", e));
    }

    return false;
  }

  /**
   * Returns a boolean indicating whether this artifact index is ready.
   *
   * @return
   */
  @Override
    public boolean isReady() {
      init();
      return initialized && checkAlive();
    }

    /**
     * Creates a Solr field of the given name and type, that is indexed, stored, required but not multivalued.
     *
     * @param solr
     *          A {@code SolrClient} that points to a Solr core or collection to add the field to.
     * @param fieldName
     *          A {@code String} containing the name of the new field.
     * @param fieldType
     *          A {@code String} containing the name of the type of the new field.
     * @throws IOException
     * @throws SolrServerException
     */
    private void createSolrField(SolrClient solr, String fieldName, String fieldType) throws IOException, SolrServerException {
        createSolrField(solr, fieldName, fieldType, null);
    }

    /**
     * Creates a Solr field of the given name and type, that is indexed, stored, required but not multivalued.
     *
     * Additional field attributes can be provided, or default attributes can be overridden, by passing field attributes.
     *
     * @param solr
     *          A {@code SolrClient} that points to a Solr core or collection to add the field to.
     * @param fieldName
     *          A {@code String} containing the name of the new field.
     * @param fieldType
                A {@code String} containing the name of the type of the new field.
     * @param fieldAttributes
     *          A {@code Map<String, Object>} containing additional field attributes, and/or a map of fields to override.
     * @throws IOException
     * @throws SolrServerException
     */
    private void createSolrField(SolrClient solr, String fieldName, String fieldType, Map<String,Object> fieldAttributes) throws IOException, SolrServerException {
        // https://lucene.apache.org/solr/guide/7_2/defining-fields.html#DefiningFields-OptionalFieldTypeOverrideProperties
        Map<String, Object> newFieldAttributes =
	    getNewFieldAttributes(fieldName, fieldType, fieldAttributes);

        // Only create the field if it does not exist
        if (!hasSolrField(fieldName, fieldType)) {
            // Create and process new field request
            log.debug("Adding field to Solr schema: {}", newFieldAttributes);
            SchemaRequest.AddField addFieldReq = new SchemaRequest.AddField(newFieldAttributes);
            addFieldReq.process(solr);
        } else {
            log.warn("Field already exists in Solr schema: {}; skipping field addition", newFieldAttributes);
        }
    }

    /**
     * Adds an artifact to the artifactIndex.
     *
     * @param artifactData An ArtifactData with the artifact to be added to the artifactIndex,.
     * @return an Artifact with the artifact indexing data.
     */
    @Override
    public Artifact indexArtifact(ArtifactData artifactData) throws IOException {
        log.debug("Adding artifact to index: {}", artifactData);

        ArtifactIdentifier artifactId = artifactData.getIdentifier();

        // Create an instance of Artifact to represent the artifact
        Artifact artifact = new Artifact(
                artifactId,
                false,
                artifactData.getStorageUrl(),
                artifactData.getContentLength(),
                artifactData.getContentDigest()
        );

        // Save the artifact collection date.
        artifact.setCollectionDate(artifactData.getCollectionDate());

        // Add the Artifact to Solr as a bean
        try {
            this.solr.addBean(artifact);
            this.solr.commit();
        } catch (SolrServerException e) {
            throw new IOException(e);
        }

        // Return the Artifact added to the Solr collection
        log.debug("Added artifact to index: {}", artifactData);

        return artifact;
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
        SolrQuery q = new SolrQuery();
//        q.addFilterQuery(String.format("committed:%s", true));
//        q.addFilterQuery(String.format("{!term f=id}%s", artifactId));
        q.setQuery(String.format("id:%s", artifactId));

        // Artifact to eventually return
        Artifact indexData = null;

        try {
            // Query the Solr artifactIndex and get results as Artifact
            final QueryResponse response = solr.query(q);
            final List<Artifact> documents = response.getBeans(Artifact.class);

            // Run some checks against the results of the query
            if (!documents.isEmpty()) {
                if (documents.size() > 1) {
                    // This should never happen; id field should be unique
                    throw new RuntimeException(String.format("Multiple Solr documents found for id: %s!", artifactId));
                } else {
                    // Set indexData to the single result to return
                    indexData = documents.get(0);
                }
            }
        } catch (SolrServerException e) {
            throw new IOException(e);
        }

        // TODO: Should we throw an exception instead?
        if (indexData == null)
            log.warn(String.format("No Solr documents found with artifact ID: %s", artifactId));

        return indexData;
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
        // Perform an atomic update
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", artifactId);

        // Setup type of field modification, and replacement value
        Map<String, Object> fieldModifier = new HashMap<>();
        fieldModifier.put("set", true);
        document.addField("committed", fieldModifier);

        try {
            // Update the field
            this.solr.add(document);
            this.solr.commit();
        } catch (SolrServerException e) {
            throw new IOException(e);
        }

        // Return updated Artifact
        return getArtifact(artifactId);
    }

    /**
     * Commits to the artifactIndex an artifact with a given artifactIndex identifier UUID.
     *
     * @param artifactId An UUID with the artifact artifactIndex identifier.
     * @return an Artifact with the committed artifact indexing data.
     */
    @Override
    public Artifact commitArtifact(UUID artifactId) throws IOException {
        return commitArtifact(artifactId.toString());
    }

    /**
     * Removes from the artifactIndex an artifact with a given text artifactIndex identifier.
     *
     * @param artifactId A String with the artifact artifactIndex identifier.
     * @throws IOException
     */
    @Override
    public boolean deleteArtifact(String artifactId) throws IOException {
        try {
            solr.deleteById(artifactId);
            return true;
        } catch (SolrServerException e) {
            throw new IOException(e);
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
      // Perform an atomic update
      SolrInputDocument document = new SolrInputDocument();
      document.addField("id", artifactId);

      // Setup type of field modification, and replacement value
      Map<String, Object> fieldModifier = new HashMap<>();
      fieldModifier.put("set", storageUrl);
      document.addField("storageUrl", fieldModifier);

      try {
          // Update the field
          this.solr.add(document);
          this.solr.commit();
      } catch (SolrServerException e) {
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
    private boolean isEmptySolrIndex() throws IOException, SolrServerException {
      // Match all documents but set the number of documents to be returned to zero
      SolrQuery q = new SolrQuery();
      q.setQuery("*:*");
      q.setRows(0);

      // Return the number of documents our query matched
      QueryResponse result = solr.query(q);
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
            // Cannot perform facet field query on an empty Solr index; one or more artifacts must exist for a
            // collection to exist
            if (isEmptySolrIndex()) {
              return IterableUtils.emptyIterable();
            }

            // Perform a Solr facet query on the collection ID field
            SolrQuery q = new SolrQuery();
            q.setQuery("*:*");
            q.setRows(0);
            q.addFacetQuery("committed:true");
            q.addFacetField("collection");

            // Get the facet field from the result
            QueryResponse result = solr.query(q);
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
            return IteratorUtils.asIterable(ff.getValues().stream().map(x -> x.getName()).sorted().iterator());

        } catch (SolrServerException e) {
            throw new IOException(e);
        }
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
        // We use a Solr facet query but another option is Solr groups. I believe faceting is better in this case,
        // because we are not actually interested in the Solr documents - only aggregate information about them.
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.setFields("auid");
        q.setRows(0);
        q.addFacetField("auid");

        try {
            QueryResponse response = solr.query(q);
            return IteratorUtils.asIterable(
                    response.getFacetField("auid").getValues().stream().map(x -> x.getName()).sorted().iterator()
            );
        } catch (SolrServerException e) {
            throw new IOException(e);
        }
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
    public Iterable<Artifact> getArtifacts(String collection, String auid, boolean includeUncommitted) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");

        // Filter by committed status equal to true?
        if (!includeUncommitted)
            q.addFilterQuery(String.format("committed:%s", true));

        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addSort(SORTURI_ASC);
        q.addSort(VERSION_DESC);

        // Ensure the result is not empty for the collapse filter query
        if (isEmptyResult(q)) {
            log.debug(String.format(
                    "Solr returned null result set after filtering by (committed: true, collection: %s, auid: %s)",
                    collection,
                    auid
            ));

            return IteratorUtils.asIterable(IteratorUtils.emptyIterator());
        }

        // Perform collapse filter query and return result
        q.addFilterQuery("{!collapse field=uri max=version}");
        return IteratorUtils.asIterable(query(q));
    }

    /**
     * Returns the artifacts of all versions of all URLs, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param includeUncommitted
     *          A {@code boolean} indicating whether to return all the versions among both committed and uncommitted
     *          artifacts.
     * @return An {@code Iterator<Artifact>} containing the artifacts of all version of all URLs in an AU.
     */
    @Override
    public Iterable<Artifact> getArtifactsAllVersions(String collection, String auid, boolean includeUncommitted) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");

        if (!includeUncommitted) {
            q.addFilterQuery(String.format("committed:%s", true));
        }

        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addSort(SORTURI_ASC);
        q.addSort(VERSION_DESC);

        return IteratorUtils.asIterable(query(q));
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
            log.debug(String.format(
                    "Solr returned null result set after filtering by (committed: true, collection: %s, auid: %s, uri (prefix): %s)",
                    collection,
                    auid,
                    prefix
            ));

            return IteratorUtils.asIterable(IteratorUtils.emptyIterator());
        }

        // Perform collapse filter query and return result
        q.addFilterQuery("{!collapse field=uri max=version}");
        return IteratorUtils.asIterable(query(q));
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
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
     *         prefix from an AU.
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

        return IteratorUtils.asIterable(query(q));
    }

    /**
     * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param prefix
     *          A String with the URL prefix.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
     *         prefix.
     */
    @Override
    public Iterable<Artifact> getArtifactsWithPrefixAllVersionsAllAus(String collection, String prefix) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!prefix f=uri}%s", prefix));
        q.addSort(SORTURI_ASC);
        q.addSort(VERSION_DESC);
        q.addSort(AUID_ASC);

        return IteratorUtils.asIterable(query(q));
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
    public Iterable<Artifact> getArtifactsAllVersions(String collection, String auid, String url) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addFilterQuery(String.format("{!term f=uri}%s", url));
        q.addSort(SORTURI_ASC);
        q.addSort(VERSION_DESC);

        return IteratorUtils.asIterable(query(q));
    }

    /**
     * Returns the committed artifacts of all versions of a given URL, from a specified collection.
     *
     * @param collection
     *          A {@code String} with the collection identifier.
     * @param url
     *          A {@code String} with the URL to be matched.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL.
     */
    @Override
    public Iterable<Artifact> getArtifactsAllVersionsAllAus(String collection, String url) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=uri}%s", url));
        q.addSort(SORTURI_ASC);
        q.addSort(VERSION_DESC);
        q.addSort(AUID_ASC);

        return IteratorUtils.asIterable(query(q));
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
     * @param includeUncommitted
     *          A {@code boolean} indicating whether to return the latest version among both committed and uncommitted
     *          artifacts of a URL.
     * @return An {@code Artifact} representing the latest version of the URL in the AU.
     * @throws IOException
     */
    @Override
    public Artifact getArtifact(String collection, String auid, String url, boolean includeUncommitted) throws IOException {
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
            log.debug(String.format(
                    "Solr returned null result set after filtering by (collection: %s, auid: %s, uri: %s)",
                    collection,
                    auid,
                    url
            ));

            return null;
        }

        // Perform a collapse filter query (must have documents in result set to operate on)
        q.addFilterQuery("{!collapse field=uri max=version}");
        Iterator<Artifact> result = query(q);

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
    public Artifact getArtifactVersion(String collection, String auid, String url, Integer version) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addFilterQuery(String.format("{!term f=uri}%s", url));
        q.addFilterQuery(String.format("version:%s", version));

        Iterator<Artifact> result = query(q);
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
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));

        // Ensure the result is not empty for the collapse filter query
        if (isEmptyResult(q)) {
            log.debug(String.format(
                    "Solr returned null result set after filtering by (committed: true, collection: %s, auid: %s)",
                    collection,
                    auid
            ));

            return null;
        }

        q.addFilterQuery("{!collapse field=uri max=version}");
        q.setGetFieldStatistics(true);
        q.setGetFieldStatistics("contentLength");
        q.setRows(0);

        try {
            // Query Solr and get
            QueryResponse response = solr.query(q);
            FieldStatsInfo contentLengthStats = response.getFieldStatsInfo().get("contentLength");

            return ((Double)contentLengthStats.getSum()).longValue();
        } catch (SolrServerException e) {
            throw new IOException(e);
        }
    }

    /**
     * Executes a Solr query and returns an interator of Artifact.
     *
     * @param q
     *          An instance of {@code SolrQuery} containing the query to submit.
     * @return An {@code Iterator<Artifact} containing the matched Artifact from the query.
     * @throws IOException
     */
    private Iterator<Artifact> query(SolrQuery q) throws IOException {
        try {
          // Set paging parameters
          q.setRows(10);
          q.addSort(SolrQuery.SortClause.asc("id"));

          String cursorMark = CursorMarkParams.CURSOR_MARK_START;
          List<Artifact> artifacts = new ArrayList<>();

          while (true) {
            // Set the query's cursor mark and perform the query
            q.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
            QueryResponse response = solr.query(q);
            String nextCursorMark = response.getNextCursorMark();

            // TODO: Potential to exhaust memory for large enough query results; find a better way to do this
            artifacts.addAll(response.getBeans(Artifact.class));

            // Determine whether there are more pages to retrieve from Solr
            if (!cursorMark.equals(nextCursorMark)) {
              // Update the cursor mark for the next page
              cursorMark = nextCursorMark;
            } else {
              // We have reached the end - break out of the while loop
              break;
            }
          }

          return artifacts.iterator();
        } catch (SolrServerException e) {
          throw new IOException(e);
        }
    }

    private boolean isEmptyResult(SolrQuery q) throws IOException {
      // Override number of rows to return
      q.setRows(0);

      // Perform query and find the number of documents
      try {
        QueryResponse response = solr.query(q);
        return response.getResults().getNumFound() == 0;
      } catch (SolrServerException e) {
        throw new IOException(String.format("Caught SolrServerException attempting to execute Solr query: %s", q));
      }
    }
}
