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
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.lockss.laaws.rs.io.index.AbstractArtifactIndex;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.log.L4JLogger;

import java.io.IOException;
import java.util.*;

/**
 * An Apache Solr implementation of ArtifactIndex.
 */
public class SolrArtifactIndex extends AbstractArtifactIndex {
  private final static L4JLogger log = L4JLogger.getLogger();

  // The prefix of the name of the field that holds the LOCKSS version of the
  // Solr schema.
  private static final String lockssSolrSchemaVersionFieldName =
      "solrSchemaLockssVersion";

  private SolrClient solrClient;
  private boolean internalClient;

  private static final SolrQuery.SortClause SORTURI_ASC = new SolrQuery.SortClause("sortUri", SolrQuery.ORDER.asc);
  private static final SolrQuery.SortClause VERSION_DESC = new SolrQuery.SortClause("version", SolrQuery.ORDER.desc);
  private static final SolrQuery.SortClause AUID_ASC = new SolrQuery.SortClause("auid", SolrQuery.ORDER.asc);

  private static final String solrIntegerType = "pint";
  private static final String solrLongType = "plong";

  // The version of the schema to be targeted by this indexer.
  //
  // After this indexer has started successfully, this is the version of the
  // schema that will be in place, as long as the schema version prior to
  // starting the indexer was not higher already.
  private final int targetSchemaVersion = 2;

  // The fields defined in the Solr schema, indexed by their names.
  private Map<String, Map<String, Object>> solrSchemaFields = null;

  /**
   * Constructor. Creates and uses a HttpSolrClient from a Solr collection URL.
   *
   * @param solrCollectionUrl A {@code String} containing the URL to a Solr collection or core.
   */
  public SolrArtifactIndex(String solrCollectionUrl) {
    // Get a handle to the Solr collection
    setSolrClient(new HttpSolrClient.Builder(solrCollectionUrl).build(), true);
  }

  /**
   * Constructor that uses a given SolrClient.
   *
   * @param client A {@code SolrClient} to use to artifactIndex artifacts.
   */
  public SolrArtifactIndex(SolrClient client) {
    setSolrClient(client, false);
  }

  private void setSolrClient(SolrClient client, boolean internalClient) {
    this.solrClient = client;
    this.internalClient = internalClient;
  }

  /**
   * Modifies the schema of a collection pointed to by a SolrClient, to support artifact indexing.
   */
  @Override
  public synchronized void initIndex() {
    if (getState() == ArtifactIndexState.UNINITIALIZED) {
      try {
	// Update the schema, if necessary.
	updateIfNecessary(targetSchemaVersion);
      } catch (SorlResponseErrorException | SolrServerException | IOException e)
      {
	String errorMessage = "Exception caught initializing Solr index";
	log.error(errorMessage, e);
	throw new RuntimeException(errorMessage, e);
      }

      setState(ArtifactIndexState.INITIALIZED);
    }
  }

  /**
   * Updates the schema, if necessary.
   *
   * @param targetSchemaVersion An int with the LOCKSS version of the Solr
   *                            schema to be updated.
   * @throws SorlResponseErrorException if Solr reports problems.
   * @throws SolrServerException        if Solr reports problems.
   * @throws IOException                if Solr reports problems.
   */
  private void updateIfNecessary(int targetSchemaVersion)
      throws SorlResponseErrorException, SolrServerException, IOException {
    log.debug2("targetSchemaVersion = " + targetSchemaVersion);

    // Get the Solr schema fields.
    solrSchemaFields = getSolrSchemaFields();

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
      updateSolrLockssSchemaVersionField(lastUpdatedVersion);

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
   * Provides the definitions of the Solr schema fields.
   *
   * @return a Map<Map<String, Object>> with the definitions of the Solr schema
   *         fields.
   * @throws SorlResponseErrorException if Solr reports problems.
   * @throws SolrServerException        if Solr reports problems.
   * @throws IOException                if Solr reports problems.
   */
  private Map<String, Map<String, Object>> getSolrSchemaFields()
      throws SorlResponseErrorException, SolrServerException, IOException {
    log.debug2("Invoked");
    SchemaResponse.FieldsResponse fieldsResponse = null;

    try {
      // Request the Solr schema fields.
      fieldsResponse =
	  handleSolrResponse(new SchemaRequest.Fields().process(solrClient),
	  "Problem getting Solr schema fields");
    } catch (SolrServerException | IOException e) {
      String errorMessage = "Exception caught getting Solr schema fields";
      log.error(errorMessage, e);
      throw e;
    }

    Map<String, Map<String, Object>> result = new HashMap<>();

    // Loop through all the fields.
    for (Map<String, Object> field : fieldsResponse.getFields()) {
      // Get the field name.
      String fieldName = (String)field.get("name");
      log.trace("fieldName = {}", fieldName);

      // Add the field to  the result.
      result.put(fieldName, field);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Returns a Solr response unchanged, if it has a zero status; throws,
   * otherwise.
   *
   * @param solrResponse A SolrResponseBase with the Solr response tobe handled.
   * @param errorMessage A String with a custom error message to be included in
   *                     the thrown exception, if necessary.
   * @return a SolrResponseBase with the passed Solr response unchanged, if it
   *         has a zero status.
   * @throws SorlResponseErrorException if the passed Solr response has a
   *                                    non-zero status.
   */
  static <T extends SolrResponseBase> T handleSolrResponse(T solrResponse,
      String errorMessage) throws SorlResponseErrorException {
    log.debug2("solrResponse = {}", solrResponse);

    NamedList<Object> solrResponseResponse =
	(NamedList<Object>)solrResponse.getResponse();

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

    SorlResponseErrorException snzse =
	new SorlResponseErrorException(errorMessage, solrResponse);
    log.error(snzse);
    throw snzse;
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
	&& hasSolrField("contentLength", solrLongType)
	&& hasSolrField("contentDigest", "string")
	&& hasSolrField("version", solrIntegerType)
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

    // Get the Solr schema LOCKSS version field.
    Map<String, Object> field =
	solrSchemaFields.get(lockssSolrSchemaVersionFieldName);

    // Check whether it exists.
    if (field != null) {
      // Yes: Get the version.
      result = Integer.valueOf((String)field.get("default"));
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Provides an indication of whether a Solr schema field exists.
   *
   * @param fieldName A String with the name of the field.
   * @param fieldType A String with the type of the field.
   * @return a boolean with the indication.
   */
  private boolean hasSolrField(String fieldName, String fieldType) {
    Map<String, Object> field = solrSchemaFields.get(fieldName);

    return field != null
	&& field.equals(getNewFieldAttributes(fieldName, fieldType, null));
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
   * Updates the schema to the target version.
   *
   * @param existingVersion An int with the existing schema version.
   * @param finalVersion    An int with the version of the schema to which the
   *                        schema is to be updated.
   * @return an int with the highest update version recorded in the schema.
   * @throws SorlResponseErrorException if Solr reports problems.
   * @throws SolrServerException        if Solr reports problems.
   * @throws IOException                if Solr reports problems.
   */
  private int updateSchema(int existingVersion, int finalVersion)
      throws SorlResponseErrorException, SolrServerException, IOException {
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
   * @param targetVersion An int with the LOCKSS version of the schema to which
   *                      the Solr schema is to be updated.
   * @throws SorlResponseErrorException if Solr reports problems.
   * @throws SolrServerException        if Solr reports problems.
   * @throws IOException                if Solr reports problems.
   */
  private void updateSchemaToVersion(int targetVersion)
      throws SorlResponseErrorException, SolrServerException, IOException {
    log.debug2("targetVersion = {}", targetVersion);

    // Perform the appropriate Solr schema update for this version.
    try {
      if (targetVersion == 1) {
        updateSchemaFrom0To1();
      } else if (targetVersion == 2) {
	updateSchemaFrom1To2();
      } else {
	throw new IllegalArgumentException("Non-existent method to update the "
	    + "schema to version " + targetVersion + ".");
      }
    } catch (SolrServerException | IOException e) {
      String errorMessage = "Exception caught updating Solr schema to LOCKSS "
	  + "version " + targetVersion;
      log.error(errorMessage, e);
      throw e;
    }
  }

  private void updateSchemaFrom0To1() throws SolrServerException, IOException, SorlResponseErrorException {
    // New field type definition for "long" (using depreciated TrieLongField)
    // <fieldType name="long" class="solr.TrieLongField" docValues="true" precisionStep="0" positionIncrementGap="0"/>
    Map<String, Object> ftd = new HashMap<>();
    ftd.put("name", "long");
    ftd.put("class", "solr.TrieLongField");
    ftd.put("docValues", "true");
    ftd.put("precisionStep", 0);
    ftd.put("positionIncrementGap", 0);

    // Create "long" field type if it does not exist
    if (!hasSolrFieldType(ftd)) {
      createSolrFieldType(ftd);
    }

    // Create initial set of fields
    createSolrField(solrClient, "collection", "string");
    createSolrField(solrClient, "auid", "string");
    createSolrField(solrClient, "uri", "string");
    createSolrField(solrClient, "committed", "boolean");
    createSolrField(solrClient, "storageUrl", "string");
    createSolrField(solrClient, "contentLength", solrLongType);
    createSolrField(solrClient, "contentDigest", "string");
    createSolrField(solrClient, "version", solrIntegerType);
    createSolrField(solrClient, "collectionDate", "long");
  }

  /**
   * Updates the Solr schema from LOCKSS version 1 to 2.
   *
   * @throws SorlResponseErrorException if Solr reports problems.
   * @throws SolrServerException        if Solr reports problems.
   * @throws IOException                if Solr reports problems.
   */
  private void updateSchemaFrom1To2()
      throws SorlResponseErrorException, SolrServerException, IOException {
    log.debug2("Invoked");

    // Replace collectionDate field definition with one that uses field type "plong"
    SchemaRequest.ReplaceField req = new SchemaRequest.ReplaceField(
        getNewFieldAttributes("collectionDate", solrLongType, null)
    );
    req.process(solrClient);

    // Remove "long" field type definition
//    SchemaRequest.DeleteFieldType delReq = new SchemaRequest.DeleteFieldType("long");
//    delReq.process(solrClient);

    try {
      // Create the new field in the schema.
      createSolrField(solrClient, "sortUri", "string");

      // Loop through all the documents in the index.
      SolrQuery q = new SolrQuery().setQuery("*:*");

      for (Artifact artifact : IteratorUtils.asIterable(new SolrQueryArtifactIterator(solrClient, q))) {
        // Initialize a document with the artifact identifier.
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", artifact.getId());

        // Add the new field value.
        document.addField("sortUri", getFieldModifier("set", artifact.getSortUri()));
        document.addField("collectionDate", getFieldModifier("set", artifact.getCollectionDate()));
        log.trace("document = {}", document);

        // Add the document with the new field.
        handleSolrResponse(solrClient.add(document), "Problem adding document '" + document + "' to Solr");
      }

      // Commit all changes
      handleSolrResponse(solrClient.commit(), "Problem committing changes to Solr");

    } catch (SolrServerException | IOException e) {
      String errorMessage = "Exception caught updating Solr schema to LOCKSS version 2";
      log.error(errorMessage, e);
      throw e;
    }

    log.debug2("Done");
  }

  /**
   * Creates a {@code Map} containing a field modifier.
   *
   * See https://lucene.apache.org/solr/guide/6_6/updating-parts-of-documents.html#UpdatingPartsofDocuments-In-PlaceUpdates
   *
   * @param modifier A {@code String} specifying the modifier. Can be one of "set" or "inc".
   * @param value A {@code Object} new value of the field, or number to increment a numeric field.
   * @return A {@code Map<String, Object>} containing the field modifier.
   */
  private Map<String, Object> getFieldModifier(String modifier, Object value) {
    Map<String, Object> fieldModifier = new HashMap<>();
    fieldModifier.put(modifier, value);
    return fieldModifier;
  }

  /**
   * Updates the Solr schema LOCKSS version field in the index.
   *
   * @param schemaVersion An int with the LOCKSS version of the Solr schema.
   * @throws SorlResponseErrorException if Solr reports problems.
   * @throws SolrServerException        if Solr reports problems.
   * @throws IOException                if Solr reports problems.
   */
  public void updateSolrLockssSchemaVersionField(int schemaVersion)
      throws SorlResponseErrorException, SolrServerException, IOException {
    log.debug2("schemaVersion = {}", schemaVersion);

    Map<String, Object> newFieldAttributes = new LinkedHashMap<>();
    newFieldAttributes.put("name", lockssSolrSchemaVersionFieldName);
    newFieldAttributes.put("type", solrIntegerType);
    newFieldAttributes.put("indexed", true);
    newFieldAttributes.put("stored", true);
    newFieldAttributes.put("multiValued", false);
    newFieldAttributes.put("required", false);
    newFieldAttributes.put("default", String.valueOf(schemaVersion));

    try {
      // Get the Solr schema LOCKSS version field.
      Map<String, Object> field =
	  solrSchemaFields.get(lockssSolrSchemaVersionFieldName);
      log.trace("field = {}", field);

      // Check whether it exists.
      if (field != null) {
	// Yes: Replace the existing field.
	log.trace("Replacing field '{}' in Solr schema", field);
	handleSolrResponse(new SchemaRequest.ReplaceField(newFieldAttributes)
	    .process(solrClient),
	    "Problem replacing field '" + field + "' in the Solr schema");
      } else {
	// No: Add the field for the Solr schema LOCKSS version.
	log.trace("Adding field to Solr schema: {}", newFieldAttributes);
	handleSolrResponse(new SchemaRequest.AddField(newFieldAttributes)
	    .process(solrClient),
	    "Problem adding field '" + field + "' in the Solr schema");
      }
    } catch (SolrServerException | IOException e) {
      String errorMessage = "Exception caught updating Solr schema LOCKSS "
	  + "version field to " + schemaVersion;
      log.error(errorMessage, e);
      throw e;
    }

    log.debug2("Done");
  }

  @Override
  public void shutdownIndex() {
    try {
      if (internalClient) {
        solrClient.close();
      }

      setState(ArtifactIndexState.SHUTDOWN);
    } catch (IOException e) {
      log.error("Could not close Solr client connection: {}", e);
    }
  }

  /**
   * Checks whether the Solr cluster is alive by calling {@code SolrClient#ping()}.
   *
   * @return
   */
  private boolean checkAlive() {
    try {
      handleSolrResponse(solrClient.ping(), "Problem pinging Solr");
      return true;
    } catch (Exception e) {
      log.warn("Could not ping Solr: {}", e);
    }

    return false;
  }

  /**
   * Returns a boolean indicating whether this artifact index is ready.
   *
   * @return a boolean with the indication.
   */
  @Override
  public boolean isReady() {

    return getState() == ArtifactIndexState.READY || getState() == ArtifactIndexState.INITIALIZED && checkAlive();
  }

  /**
   * Creates a Solr field of the given name and type, that is indexed, stored, required but not multivalued.
   *
   * @param solr      A {@code SolrClient} that points to a Solr core or collection to add the field to.
   * @param fieldName A {@code String} containing the name of the new field.
   * @param fieldType A {@code String} containing the name of the type of the new field.
   * @throws SorlResponseErrorException if Solr reports problems.
   * @throws SolrServerException        if Solr reports problems.
   * @throws IOException                if Solr reports problems.
   */
  private void createSolrField(SolrClient solr, String fieldName, String fieldType)
      throws SorlResponseErrorException, IOException, SolrServerException {
    createSolrField(solr, fieldName, fieldType, null);
  }

  /**
   * Creates a Solr field of the given name and type, that is indexed, stored, required but not multivalued.
   * <p>
   * Additional field attributes can be provided, or default attributes can be overridden, by passing field attributes.
   *
   * @param solr            A {@code SolrClient} that points to a Solr core or collection to add the field to.
   * @param fieldName       A {@code String} containing the name of the new field.
   * @param fieldType       A {@code String} containing the name of the type of the new field.
   * @param fieldAttributes A {@code Map<String, Object>} containing additional field attributes, and/or a map of fields to override.
   * @throws SorlResponseErrorException if Solr reports problems.
   * @throws SolrServerException        if Solr reports problems.
   * @throws IOException                if Solr reports problems.
   */
  private void createSolrField(SolrClient solr, String fieldName, String fieldType, Map<String, Object> fieldAttributes)
      throws SorlResponseErrorException, IOException, SolrServerException {
    // https://lucene.apache.org/solr/guide/7_2/defining-fields.html#DefiningFields-OptionalFieldTypeOverrideProperties
    Map<String, Object> newFieldAttributes =
    getNewFieldAttributes(fieldName, fieldType, fieldAttributes);

    // Only create the field if it does not exist
    if (!hasSolrField(fieldName, fieldType)) {
      // Create and process new field request
      log.debug("Adding field to Solr schema: {}", newFieldAttributes);
      SchemaRequest.AddField addFieldReq = new SchemaRequest.AddField(newFieldAttributes);
      handleSolrResponse(addFieldReq.process(solr),
	  "Problem adding field to Solr schema");
    } else {
      log.debug("Field already exists in Solr schema: {}; skipping field addition", newFieldAttributes);
    }
  }

  /**
   * Creates a new Solr field type, provided the new field type's attributes. Does not support setting an analyzer.
   *
   * @param fieldTypeAttributes A {@code Map<String, Object>} containing attributes of the field type to create.
   * @return An {@code UpdateResponse} from Solr.
   * @throws IOException         if Solr reports problems.
   * @throws SolrServerException if Solr reports problems.
   */
  private SchemaResponse.UpdateResponse createSolrFieldType(Map<String, Object> fieldTypeAttributes)
      throws IOException, SolrServerException {
    // Create new field type definition
    FieldTypeDefinition ftd = new FieldTypeDefinition();
    ftd.setAttributes(fieldTypeAttributes);

    // Create and submit new add-field-type request
    SchemaRequest.AddFieldType req = new SchemaRequest.AddFieldType(ftd);
    return req.process(solrClient);
  }

  /**
   * Returns a boolean indicating whether a Solr field type matching the provided field type attributes exists in the
   * Solr schema.
   *
   * @param fieldTypeAttributes A {@code Map<String, Object>} containing attributes of the field type to search for.
   * @return A {@code boolean} indicating whether the field type exists in the Solr schema.
   * @throws IOException         if Solr reports problems.
   * @throws SolrServerException if Solr reports problems.
   */
  private boolean hasSolrFieldType(Map<String, Object> fieldTypeAttributes) throws IOException, SolrServerException {
    SchemaRequest.FieldTypes req = new SchemaRequest.FieldTypes();
    SchemaResponse.FieldTypesResponse res = req.process(solrClient);

    return res.getFieldTypes().stream().anyMatch(ft -> ft.equals(fieldTypeAttributes));
  }

  /**
   * Adds an artifact to the artifactIndex.
   *
   * @param artifactData An ArtifactData with the artifact to be added to the artifactIndex,.
   * @return an Artifact with the artifact indexing data.
   */
  @Override
  public Artifact indexArtifact(ArtifactData artifactData) throws IOException {
    log.debug2("Indexing artifact data: {}", artifactData);

    if (artifactData == null) {
      throw new IllegalArgumentException("Null artifact data");
    }

    ArtifactIdentifier artifactId = artifactData.getIdentifier();

    if (artifactId == null) {
      throw new IllegalArgumentException("ArtifactData has null identifier");
    }// Create an instance of Artifact to represent the artifact
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
      handleSolrResponse(solrClient.addBean(artifact), "Problem adding artifact '"
	    + artifact + "' to Solr");
      handleSolrResponse(solrClient.commit(), "Problem committing addition of "
	    + "artifact '" + artifact + "' to Solr");
    } catch (SorlResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }

    // Return the Artifact added to the Solr collection
    log.debug("Added artifact to index: {}", artifact);

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
    if (artifactId == null) {
      throw new IllegalArgumentException("Null or empty artifact ID");
    }

    SolrQuery q = new SolrQuery();
    q.setQuery(String.format("id:%s", artifactId));
//        q.addFilterQuery(String.format("{!term f=id}%s", artifactId));
//        q.addFilterQuery(String.format("committed:%s", true));

    // Artifact to eventually return
    Artifact indexedArtifact = null;

    try {
      // Submit the Solr query and get results as Artifact objects
      final QueryResponse response =
	  handleSolrResponse(solrClient.query(q), "Problem performing Solr query");
      final List<Artifact> artifacts = response.getBeans(Artifact.class);

      // Run some checks against the results
      if (!artifacts.isEmpty()) {
        if (artifacts.size() > 1) {
          // This should never happen; id field should be unique
          throw new RuntimeException(String.format("More than one artifact found with same artifact ID [artifactId: %s]", artifactId)
        );} else {
          // Set indexData to the single result to return
          indexedArtifact = artifacts.get(0);
        }
      }
    } catch (SorlResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }


    if (indexedArtifact == null) {
      log.debug2("No Solr documents found [artifactId: {}]", artifactId);
    }

    return indexedArtifact;
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
    }return this.getArtifact(artifactId.toString());
  }

  /**
   * Commits to the artifactIndex an artifact with a given text artifactIndex identifier.
   *
   * @param artifactId A String with the artifact artifactIndex identifier.
   * @return an Artifact with the committed artifact indexing data.
   */
  @Override
  public Artifact commitArtifact(String artifactId) throws IOException {
    if (artifactExists(artifactId)) {
    SolrInputDocument document = new SolrInputDocument();
    document.addField("id", artifactId);

    // Perform an atomic update (see https://lucene.apache.org/solr/guide/6_6/updating-parts-of-documents.html) by
      // setting the type of field modification and itsreplacement value
    Map<String, Object> fieldModifier = new HashMap<>();
    fieldModifier.put("set", true);
    document.addField("committed", fieldModifier);

    try {
      // Update the artifact.
      handleSolrResponse(solrClient.add(document), "Problem adding document '"
	  + document + "' to Solr");
      handleSolrResponse(solrClient.commit(), "Problem committing addition of "
	  + "document '" + document + "' to Solr");
    } catch (SorlResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }

    // Return updated Artifact
    return getArtifact(artifactId);
  }else {
      return null;
    }
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
    }return commitArtifact(artifactId.toString());
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
      // Yes: Artifact found - remove Solr document for this artifact
      try {
        handleSolrResponse(solrClient.deleteById(artifactId), "Problem deleting "
  	  + "artifact '" + artifactId + "' from Solr");
        handleSolrResponse(solrClient.commit(), "Problem committing deletion of "
  	  + "artifact '" + artifactId + "' from Solr");
        return true;
      } catch (SorlResponseErrorException | SolrServerException e) {
        log.error("Could not remove artifact from Solr index [artifactId: {}]: {}", artifactId);
        throw new IOException(
            String.format("Could not remove artifact from Solr index [artifactId: %s]", artifactId), e
        );
      }
    } else {
      // Artifact not found in index; nothing deleted
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
    }return deleteArtifact(artifactId.toString());
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
    if (artifactId == null) {
      throw new IllegalArgumentException("Cannot update storage URL for a null artifact ID");
    }

    if (StringUtils.isEmpty(storageUrl)) {
      throw new IllegalArgumentException("Invalid storage URL: Must not be null or empty");
    }

    if (artifactExists(artifactId)) {
      // Perform a partial update of an existing Solr document
    SolrInputDocument document = new SolrInputDocument();
    document.addField("id", artifactId);

    // Specify type of field modification, field name, and replacement value
    Map<String, Object> fieldModifier = new HashMap<>();
    fieldModifier.put("set", storageUrl);
    document.addField("storageUrl", fieldModifier);

    try {
      // Update the field
      handleSolrResponse(solrClient.add(document), "Problem adding document '"
	  + document + "' to Solr");
      handleSolrResponse(solrClient.commit(), "Problem committing addition of "
	  + "document '" + document + "' to Solr");
    } catch (SorlResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }}

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
      throws SorlResponseErrorException, IOException, SolrServerException {
    // Match all documents but set the number of documents to be returned to zero
    SolrQuery q = new SolrQuery();
    q.setQuery("*:*");
    q.setRows(0);

    // Return the number of documents our query matched
    QueryResponse result =
	handleSolrResponse(solrClient.query(q), "Problem performing Solr query");
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
      q.setQuery("committed:true");
      q.setRows(0);

      q.addFacetField("collection");
      q.setFacetLimit(-1); // Unlimited

      // Get the facet field from the result
      QueryResponse result =
  	  handleSolrResponse(solrClient.query(q), "Problem performing Solr query");
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
              .map(x -> x.getName())
              .sorted()
              .iterator()
      );

    } catch (SorlResponseErrorException | SolrServerException e) {
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
    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=collection}%s", collection));
    q.setFields("auid");
    q.setRows(0);
    q.addFacetField("auid");
    q.setFacetLimit(-1); // Unlimited

    try {
      QueryResponse response =
  	  handleSolrResponse(solrClient.query(q), "Problem performing Solr query");
      FacetField ff = response.getFacetField("auid");return IteratorUtils.asIterable(
          ff.getValues().stream()
              .filter(x -> x.getCount() > 0).map(x -> x.getName()).sorted().iterator()
      );
    } catch (SorlResponseErrorException | SolrServerException e) {
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
      log.debug2(String.format(
          "Solr returned null result set after filtering by (committed: true, collection: %s, auid: %s)",
          collection,
          auid
      ));

      return IteratorUtils.asIterable(IteratorUtils.emptyIterator());
    }

    // Perform collapse filter query and return result
    q.addFilterQuery("{!collapse field=uri max=version}");
    return IteratorUtils.asIterable(new SolrQueryArtifactIterator(solrClient, q));
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
    SolrQuery q = new SolrQuery();
    q.setQuery("*:*");

    if (!includeUncommitted) {
      q.addFilterQuery(String.format("committed:%s", true));
    }

    q.addFilterQuery(String.format("{!term f=collection}%s", collection));
    q.addFilterQuery(String.format("{!term f=auid}%s", auid));
    q.addSort(SORTURI_ASC);
    q.addSort(VERSION_DESC);

    return IteratorUtils.asIterable(new SolrQueryArtifactIterator(solrClient, q));
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
      log.debug2(String.format(
          "Solr returned null result set after filtering by (committed: true, collection: %s, auid: %s, uri (prefix): %s)",
          collection,
          auid,
          prefix
      ));

      return IteratorUtils.asIterable(IteratorUtils.emptyIterator());
    }

    // Perform collapse filter query and return result
    q.addFilterQuery("{!collapse field=uri max=version}");
    return IteratorUtils.asIterable(new SolrQueryArtifactIterator(solrClient, q));
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

    return IteratorUtils.asIterable(new SolrQueryArtifactIterator(solrClient, q));
  }

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified collection.
   *
   * @param collection A String with the collection identifier.
   * @param prefix     A String with the URL prefix.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithPrefixAllVersionsAllAus(String collection, String prefix) throws IOException {
    SolrQuery q = new SolrQuery();
    q.setQuery("*:*");
    q.addFilterQuery(String.format("committed:%s", true));
    q.addFilterQuery(String.format("{!term f=collection}%s", collection));
// Q: Perhaps it would be better to throw an IllegalArgumentException if prefix is null? Without this filter, we
    //    return all the committed artifacts in a collection. Is that useful?
    if (prefix != null) {
      q.addFilterQuery(String.format("{!prefix f=uri}%s", prefix));
    }
    q.addSort(SORTURI_ASC);
    q.addSort(VERSION_DESC);
    q.addSort(AUID_ASC);

    return IteratorUtils.asIterable(new SolrQueryArtifactIterator(solrClient, q));
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

    return IteratorUtils.asIterable(new SolrQueryArtifactIterator(solrClient, q));
  }

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified collection.
   *
   * @param collection A {@code String} with the collection identifier.
   * @param url        A {@code String} with the URL to be matched.
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

    return IteratorUtils.asIterable(new SolrQueryArtifactIterator(solrClient, q));
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
      log.debug2(String.format(
          "Solr returned null result set after filtering by (collection: %s, auid: %s, uri: %s)",
          collection,
          auid,
          url
      ));

      return null;
    }

    // Perform a collapse filter query (must have documents in result set to operate on)
    q.addFilterQuery("{!collapse field=uri max=version}");
    Iterator<Artifact> result = new SolrQueryArtifactIterator(solrClient, q);

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
   * @param collection A String with the collection identifier.
   * @param auid       A String with the Archival Unit identifier.
   * @param url        A String with the URL to be matched.
   * @param version    A String with the version.
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

    Iterator<Artifact> result = new SolrQueryArtifactIterator(solrClient, q);
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
      log.debug2(String.format(
          "Solr returned null result set after filtering by (committed: true, collection: %s, auid: %s)",
          collection,
          auid
      ));

      return 0L;
    }

    q.addFilterQuery("{!collapse field=uri max=version}");
    q.setGetFieldStatistics(true);
    q.setGetFieldStatistics("contentLength");
    q.setRows(0);

    try {
      // Query Solr and get
      QueryResponse response =
  	  handleSolrResponse(solrClient.query(q), "Problem performing Solr query");
      FieldStatsInfo contentLengthStats = response.getFieldStatsInfo().get("contentLength");

      return ((Double) contentLengthStats.getSum()).longValue();
    } catch (SorlResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }
  }

  private boolean isEmptyResult(SolrQuery q) throws IOException {
    // Override number of rows to return
    q.setRows(0);

    // Perform query and find the number of documents
    try {
      QueryResponse response =
	  handleSolrResponse(solrClient.query(q), "Problem performing Solr query");
      return response.getResults().getNumFound() == 0;
    } catch (SorlResponseErrorException | SolrServerException e) {
      throw new IOException(String.format("Caught SolrServerException attempting to execute Solr query: %s", q));
    }
  }
}
