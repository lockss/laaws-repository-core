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

package org.lockss.laaws.rs.io.index.solr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.ArtifactIndexData;

import java.io.IOException;
import java.util.*;

/**
 * An Apache Solr implementation of ArtifactIndex.
 */
public class SolrArtifactIndex implements ArtifactIndex {
    private static final Log log = LogFactory.getLog(ArtifactIndexData.class);
    private final SolrClient solr;

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
     *          A {@code SolrClient} to use to index artifacts.
     */
    public SolrArtifactIndex(SolrClient client) {
        // Modify the schema to support an artifact index
        createArtifactSchema(client);
        this.solr = client;
    }

    /**
     * Modifies the schema of a collection pointed to by a SolrClient, to support artifact indexing.
     *
     * @param solr
     *          An instance of {@code SolrClient} pointing to the Solr Core or Collection to be modified.
     */
    private static void createArtifactSchema(SolrClient solr) {
        try {
//            createSolrField(solr,"artfactId", "string");
            createSolrField(solr,"collection", "string");
            createSolrField(solr,"auid", "string");
            createSolrField(solr,"uri", "string");
            createSolrField(solr,"committed", "boolean");
            createSolrField(solr,"storageUrl", "string");

            // Version is a DatePointField type which requires the field attribute docValues to be set to true to enable
            // sorting when the field is a single value field. See the link below for more information:
            // https://lucene.apache.org/solr/guide/6_6/field-types-included-with-solr.html
            Map<String, Object> versionFieldAttributes = new LinkedHashMap<>();
            versionFieldAttributes.put("docValues", true);
//            createSolrField(solr,"version", "pdate", versionFieldAttributes);
            createSolrField(solr,"version", "string");

        } catch (IOException e) {
            throw new RuntimeException("IOException caught while attempting to create the fields in the Solr schema");
        } catch (SolrServerException e) {
            throw new RuntimeException("SolrServerException caught while attempting to create the fields in the Solr schema");
        }
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
    private static void createSolrField(SolrClient solr, String fieldName, String fieldType) throws IOException, SolrServerException {
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
    private static void createSolrField(SolrClient solr, String fieldName, String fieldType, Map<String,Object> fieldAttributes) throws IOException, SolrServerException {
        log.info(String.format(
                "Attempting to add field to schema: (name: %s, type: %s) to %s",
                fieldName,
                fieldType,
                solr
        ));

        // https://lucene.apache.org/solr/guide/7_2/defining-fields.html#DefiningFields-OptionalFieldTypeOverrideProperties
        Map<String, Object> newFieldAttributes = new LinkedHashMap<>();
        newFieldAttributes.put("name", fieldName);
        newFieldAttributes.put("type", fieldType);
        newFieldAttributes.put("indexed", true);
        newFieldAttributes.put("stored", true);
        newFieldAttributes.put("multiValued", false);
        newFieldAttributes.put("required", true);

        // Allow default attributes to be overridden if field attributes were provided
        if (fieldAttributes != null)
            newFieldAttributes.putAll(fieldAttributes);

        // Create and submit add field request
        SchemaRequest.AddField addFieldReq = new SchemaRequest.AddField(newFieldAttributes);
        addFieldReq.process(solr);
    }

    /**
     * Adds an artifact to the index.
     *
     * @param artifact An Artifact with the artifact to be added to the index,.
     * @return an ArtifactIndexData with the artifact indexing data.
     */
    @Override
    public ArtifactIndexData indexArtifact(Artifact artifact) throws IOException {
        ArtifactIdentifier artifactId = artifact.getIdentifier();

        // Create an instance of ArtifactIndexData to represent the artifact
        ArtifactIndexData indexData = new ArtifactIndexData(
                artifactId.getId(),
                artifactId.getCollection(),
                artifactId.getAuid(),
                artifactId.getUri(),
                // TODO: Support for artifact version
//                artifactId.getVersion(),
                "2018-10-10T13:00:00Z",
                false,
                artifact.getStorageUrl()
        );

        // Add the ArtifactIndexData to Solr as a bean
        try {
            this.solr.addBean(indexData);
            this.solr.commit();
        } catch (SolrServerException e) {
            throw new IOException(e);
        }

        // Return the ArtifactIndexData added to the Solr collection
        return indexData;
    }

    /**
     * Provides the index data of an artifact with a given text index
     * identifier.
     *
     * @param indexDataId A String with the artifact index identifier.
     * @return an ArtifactIndexData with the artifact indexing data.
     */
    @Override
    public ArtifactIndexData getArtifactIndexData(String indexDataId) throws IOException {
        SolrQuery q = new SolrQuery();
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=id}%s", indexDataId));

        // ArtifactIndexData to eventually return
        ArtifactIndexData indexData = null;

        try {
            // Query the Solr index and get results as ArtifactIndexData
            final QueryResponse response = solr.query(q);
            final List<ArtifactIndexData> documents = response.getBeans(ArtifactIndexData.class);

            // Run some checks against the results of the query
            if (!documents.isEmpty()) {
                if (documents.size() > 1) {
                    // This should never happen; id field should be unique
                    throw new RuntimeException(String.format("Multiple Solr documents found for id: %s!", indexDataId));
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
            log.warn(String.format("No Solr documents found with artifact ID: %s", indexDataId));

        return indexData;
    }

    /**
     * Provides the index data of an artifact with a given index identifier
     * UUID.
     *
     * @param indexDataId An UUID with the artifact index identifier.
     * @return an ArtifactIndexData with the artifact indexing data.
     */
    @Override
    public ArtifactIndexData getArtifactIndexData(UUID indexDataId) throws IOException {
        return this.getArtifactIndexData(indexDataId.toString());
    }

    /**
     * Commits to the index an artifact with a given text index identifier.
     *
     * @param indexDataId A String with the artifact index identifier.
     * @return an ArtifactIndexData with the committed artifact indexing data.
     */
    @Override
    public ArtifactIndexData commitArtifact(String indexDataId) throws IOException {
        // Perform an atomic update
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", indexDataId);

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

        // Return updated ArtifactIndexData
        return getArtifactIndexData(indexDataId);
    }

    /**
     * Commits to the index an artifact with a given index identifier UUID.
     *
     * @param indexDataId An UUID with the artifact index identifier.
     * @return an ArtifactIndexData with the committed artifact indexing data.
     */
    @Override
    public ArtifactIndexData commitArtifact(UUID indexDataId) throws IOException {
        return commitArtifact(indexDataId.toString());
    }

    /**
     * Removes from the index an artifact with a given text index identifier.
     *
     * @param indexDataId A String with the artifact index identifier.
     * @throws IOException
     */
    @Override
    public boolean deleteArtifact(String indexDataId) throws IOException {
        try {
            solr.deleteById(indexDataId);
            return true;
        } catch (SolrServerException e) {
            throw new IOException(e);
        }
    }

    /**
     * Removes from the index an artifact with a given index identifier UUID.
     *
     * @param indexDataId A String with the artifact index identifier.
     * @return <code>true</code> if the artifact was removed from in the index,
     * <code>false</code> otherwise.
     */
    @Override
    public boolean deleteArtifact(UUID indexDataId) throws IOException {
        return deleteArtifact(indexDataId.toString());
    }

    /**
     * Provides an indication of whether an artifact with a given text index
     * identifier exists in the index.
     *
     * @param artifactId A String with the artifact identifier.
     * @return <code>true</code> if the artifact exists in the index,
     * <code>false</code> otherwise.
     */
    @Override
    public boolean artifactExists(String artifactId) throws IOException {
        return getArtifactIndexData(artifactId) != null;
    }

    /**
     * Provides the collection identifiers of the committed artifacts in the
     * index.
     *
     * @return an {@code Iterator<String>} with the index committed artifacts
     * collection identifiers.
     */
    @Override
    public Iterator<String> getCollectionIds() throws IOException {
        SolrQuery q = new SolrQuery();
        q.addFacetQuery("committed:true");
        q.addFacetField("collection");
        q.setRows(0); // Do not return matched documents

        try {
            QueryResponse result = solr.query(q);
            FacetField ff = result.getFacetField("collection");

            log.info(String.format(
                    "FacetField: [getName: %s, getValues: %s, getValuesCount: %s]",
                    ff.getName(),
                    ff.getValues(),
                    ff.getValueCount()
            ));

            return ff.getValues().stream().map(x -> x.getName()).iterator();

        } catch (SolrServerException e) {
            throw new IOException(e);
        }
    }

    /**
     * Returns a list of Archival Unit IDs (AUIDs) in this LOCKSS repository collection.
     *
     * @param collection A {@code String} containing the LOCKSS repository collection ID.
     * @return A {@code Iterator<String>} iterating over the AUIDs in this LOCKSS repository collection.
     * @throws IOException
     */
    @Override
    public Iterator<String> getAuIds(String collection) throws IOException {
        // We use a Solr facet query but another option is Solr groups. I believe faceting is better in this case,
        // because we are not actually interested in the Solr documents - only aggregate information about them.
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.setFields("auid");
        q.setRows(0);
        q.addFacetField("auid");

        try {
            QueryResponse response = solr.query(q);
            return response.getFacetField("auid").getValues().stream().map(x -> x.getName()).iterator();
        } catch (SolrServerException e) {
            throw new IOException(e);
        }
    }

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit.
     *
     * @param collection A String with the collection identifier.
     * @param auid       A String with the Archival Unit identifier.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     * artifacts in the collection that belong to the Archival Unit.
     */
    @Override
    public Iterator<ArtifactIndexData> getArtifactsInAU(String collection, String auid) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));

        return query(q);
    }

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain a URL with a given prefix.
     *
     * @param collection A String with the collection identifier.
     * @param auid       A String with the Archival Unit identifier.
     * @param prefix     A String with the URL prefix.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     * artifacts in the collection that belong to the Archival Unit and
     * that contain a URL with the given prefix.
     */
    @Override
    public Iterator<ArtifactIndexData> getArtifactsInAUWithURL(String collection, String auid, String prefix) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addFilterQuery(String.format("{!prefix f=uri}%s", prefix));

        return query(q);
    }

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain an exact match of a URL.
     *
     * @param collection A String with the collection identifier.
     * @param auid       A String with the Archival Unit identifier.
     * @param url        A String with the URL to be matched.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     * artifacts in the collection that belong to the Archival Unit and
     * that contain an exact match of a URL.
     */
    @Override
    public Iterator<ArtifactIndexData> getArtifactsInAUWithURLMatch(String collection, String auid, String url) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addFilterQuery(String.format("{!term f=uri}%s", url));

        return query(q);
    }

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain a URL with a given prefix and that match a
     * given version.
     *
     * @param collection A String with the collection identifier.
     * @param auid       A String with the Archival Unit identifier.
     * @param prefix     A String with the URL prefix.
     * @param version    A String with the version.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     * artifacts in the collection that belong to the Archival Unit and
     * that contain a URL with the given prefix and that match the given
     * version.
     */
    @Override
    public Iterator<ArtifactIndexData> getArtifactsInAUWithURL(String collection, String auid, String prefix, String version) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addFilterQuery(String.format("{!prefix f=uri}%s", prefix));
        q.addFilterQuery(String.format("version:%s", version));

        return query(q);
    }

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain an exact match of a URL and that match a
     * given version.
     *
     * @param collection A String with the collection identifier.
     * @param auid       A String with the Archival Unit identifier.
     * @param url        A String with the URL to be matched.
     * @param version    A String with the version.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     * artifacts in the collection that belong to the Archival Unit and
     * that contain an exact match of a URL and that match the given
     * version.
     */
    @Override
    public Iterator<ArtifactIndexData> getArtifactsInAUWithURLMatch(String collection, String auid, String url, String version) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addFilterQuery(String.format("{!term f=uri}%s", url));
        q.addFilterQuery(String.format("version:%s", version));

        return query(q);
    }

    /**
     * Executes a Solr query and returns an interator of ArtifactIndexData.
     *
     * @param q
     *          An instance of {@code SolrQuery} containing the query to submit.
     * @return An {@code Iterator<ArtifactIndexData} containing the matched ArtifactIndexData from the query.
     * @throws IOException
     */
    private Iterator<ArtifactIndexData> query(SolrQuery q) throws IOException {
        try {
            QueryResponse response = solr.query(q);
            // TODO: Potential to exhaust memory for large enough query results; find a better way to do this
            List<ArtifactIndexData> documents = response.getBeans(ArtifactIndexData.class);
            return documents.iterator();
        } catch (SolrServerException e) {
            throw new IOException(e);
        }
    }
}
