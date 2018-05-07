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

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.Artifact;

import java.io.IOException;
import java.util.*;

/**
 * An Apache Solr implementation of ArtifactIndex.
 */
public class SolrArtifactIndex implements ArtifactIndex {
    private static final Log log = LogFactory.getLog(Artifact.class);
    private final SolrClient solr;

    private static final SolrQuery.SortClause URI_ASC = new SolrQuery.SortClause("uri", SolrQuery.ORDER.asc);
    private static final SolrQuery.SortClause VERSION_DESC = new SolrQuery.SortClause("version", SolrQuery.ORDER.desc);

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
        // Modify the schema to support an artifact artifactIndex
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
            createSolrField(solr, "contentLength", "plong");
            createSolrField(solr, "contentDigest", "string");

            // Version is a DatePointField type which requires the field attribute docValues to be set to true to enable
            // sorting when the field is a single value field. See the link below for more information:
            // https://lucene.apache.org/solr/guide/6_6/field-types-included-with-solr.html
            Map<String, Object> versionFieldAttributes = new LinkedHashMap<>();
            versionFieldAttributes.put("docValues", true);
//            createSolrField(solr,"version", "pdate", versionFieldAttributes);
            createSolrField(solr,"version", "pint");

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
     * Adds an artifact to the artifactIndex.
     *
     * @param artifactData An ArtifactData with the artifact to be added to the artifactIndex,.
     * @return an Artifact with the artifact indexing data.
     */
    @Override
    public Artifact indexArtifact(ArtifactData artifactData) throws IOException {
        ArtifactIdentifier artifactId = artifactData.getIdentifier();

        // Create an instance of Artifact to represent the artifact
        Artifact artifact = new Artifact(
                artifactId,
                false,
                artifactData.getStorageUrl(),
                artifactData.getContentLength(),
                artifactData.getContentDigest()
        );

        // Add the Artifact to Solr as a bean
        try {
            this.solr.addBean(artifact);
            this.solr.commit();
        } catch (SolrServerException e) {
            throw new IOException(e);
        }

        // Return the Artifact added to the Solr collection
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
     * Provides the collection identifiers of the committed artifacts in the artifactIndex.
     *
     * @return An {@code Iterator<String>} with the artifactIndex committed artifacts
     * collection identifiers.
     */
    @Override
    public Iterable<String> getCollectionIds() throws IOException {
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
    public Iterable<Artifact> getAllArtifacts(String collection, String auid) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addSort(URI_ASC);
        q.addSort(VERSION_DESC);

        // Ensure the result is not empty for the collapse filter query
        if (!query(q).hasNext()) {
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
     * Returns the committed artifacts of all versions of all URLs, from a specified Archival Unit and collection.
     *
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all version of all URLs in an AU.
     */
    @Override
    public Iterable<Artifact> getAllArtifactsAllVersions(String collection, String auid) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addSort(URI_ASC);
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
    public Iterable<Artifact> getAllArtifactsWithPrefix(String collection, String auid, String prefix) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addFilterQuery(String.format("{!prefix f=uri}%s", prefix));
        q.addSort(URI_ASC);
        q.addSort(VERSION_DESC);

        // Ensure the result is not empty for the collapse filter query
        if (!query(q).hasNext()) {
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
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matchign a
     *         prefix from an AU.
     */
    @Override
    public Iterable<Artifact> getAllArtifactsWithPrefixAllVersions(String collection, String auid, String prefix) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addFilterQuery(String.format("{!prefix f=uri}%s", prefix));
        q.addSort(URI_ASC);
        q.addSort(VERSION_DESC);

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
    public Iterable<Artifact> getArtifactAllVersions(String collection, String auid, String url) throws IOException {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(String.format("committed:%s", true));
        q.addFilterQuery(String.format("{!term f=collection}%s", collection));
        q.addFilterQuery(String.format("{!term f=auid}%s", auid));
        q.addFilterQuery(String.format("{!term f=uri}%s", url));
        q.addSort(URI_ASC);
        q.addSort(VERSION_DESC);

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

        Iterator<Artifact> result = query(q);

        // Ensure the result is not empty for the collapse filter query
        if (!result.hasNext()) {
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
        result = query(q);

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
        q.setGetFieldStatistics(true);
        q.setGetFieldStatistics("contentLength");
        q.setRows(0);

        try {
            // Query Solr and get
            QueryResponse response = solr.query(q);
            FieldStatsInfo contentLengthStats = response.getFieldStatsInfo().get("contentLength");

            return (Long)contentLengthStats.getSum();
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
            QueryResponse response = solr.query(q);
            // TODO: Potential to exhaust memory for large enough query results; find a better way to do this
            List<Artifact> documents = response.getBeans(Artifact.class);
            return documents.iterator();
        } catch (SolrServerException e) {
            throw new IOException(e);
        }
    }
}
