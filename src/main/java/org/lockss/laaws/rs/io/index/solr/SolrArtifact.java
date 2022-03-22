package org.lockss.laaws.rs.io.index.solr;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.beans.Field;
import org.lockss.laaws.rs.model.ArtifactData;

import javax.validation.Valid;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SolrArtifact {

  @Field("id")
  @JsonProperty("id")
  private String id;

  @Field("collection")
  @JsonProperty("collection")
  private String collection;

  @Field("auid")
  @JsonProperty("auid")
  private String auid;

  @Field("uri")
  @JsonProperty("uri")
  private String uri;

  @Field("version")
  @JsonProperty("version")
  private Integer version;

  @Field("committed")
  @JsonProperty("committed")
  private Boolean committed;

  @Field("storageUrl")
  @JsonProperty("storageUrl")
  private String storageUrl;

  @Field("contentLength")
  @JsonProperty("contentLength")
  private Long contentLength;

  @Field("contentDigest")
  @JsonProperty("contentDigest")
  private String contentDigest;

//  @Field("contentType")
  @JsonProperty("contentType")
  private String contentType;

  @Field("collectionDate")
  @JsonProperty("collectionDate")
  private Long collectionDate;

//  @Field("properties")
  @JsonProperty("properties")
  @Valid
  private Map<String, List<String>> properties = null;

  @Field("sortUri")
  @JsonProperty("sortUri")
  private String sortUri;

  public static SolrArtifact from(ArtifactData ad) {
    SolrArtifact sa = new SolrArtifact();

    sa.setId(ad.getId());
    sa.setCollection(ad.getCollection());
    sa.setAuid(ad.getAuid());
    sa.setUri(ad.getUri());
    sa.setVersion(ad.getVersion());

    sa.setCommitted(ad.getCommitted());
    sa.setStorageUrl(ad.getStorageUrl());

    sa.setContentLength(ad.getContentLength());
    sa.setContentDigest(ad.getContentDigest());
    sa.setContentType(ad.getContentType());

    sa.setCollectionDate(ad.getCollectionDate());

    sa.setProperties(ad.getProperties());

    sa.setSortUri(ad.getUri());

    return sa;
  }

  SolrArtifact id(String id) {
    this.id = id;
    return this;
  }

  /**
   * Get id
   * @return id
   */

  @Schema(name = "id", required = false)
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  SolrArtifact collection(String collection) {
    this.collection = collection;
    return this;
  }

  /**
   * Get collection
   * @return collection
   */

  @Schema(name = "collection", required = false)
  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  SolrArtifact auid(String auid) {
    this.auid = auid;
    return this;
  }

  /**
   * Get auid
   * @return auid
   */

  @Schema(name = "auid", required = false)
  public String getAuid() {
    return auid;
  }

  public void setAuid(String auid) {
    this.auid = auid;
  }

  SolrArtifact uri(String uri) {
    this.uri = uri;
    return this;
  }

  /**
   * Get uri
   * @return uri
   */

  @Schema(name = "uri", required = false)
  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  SolrArtifact version(Integer version) {
    this.version = version;
    return this;
  }

  /**
   * Get version
   * @return version
   */

  @Schema(name = "version", required = false)
  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  SolrArtifact committed(Boolean committed) {
    this.committed = committed;
    return this;
  }

  /**
   * Get committed
   * @return committed
   */

  @Schema(name = "committed", required = false)
  public Boolean getCommitted() {
    return committed;
  }

  public void setCommitted(Boolean committed) {
    this.committed = committed;
  }

  SolrArtifact storageUrl(String storageUrl) {
    this.storageUrl = storageUrl;
    return this;
  }

  /**
   * Get storageUrl
   * @return storageUrl
   */

  @Schema(name = "storageUrl", required = false)
  public String getStorageUrl() {
    return storageUrl;
  }

  public void setStorageUrl(String storageUrl) {
    this.storageUrl = storageUrl;
  }

  SolrArtifact contentLength(Long contentLength) {
    this.contentLength = contentLength;
    return this;
  }

  /**
   * Get contentLength
   * @return contentLength
   */

  @Schema(name = "contentLength", required = false)
  public Long getContentLength() {
    return contentLength;
  }

  public void setContentLength(Long contentLength) {
    this.contentLength = contentLength;
  }

  SolrArtifact contentDigest(String contentDigest) {
    this.contentDigest = contentDigest;
    return this;
  }

  /**
   * Get contentDigest
   * @return contentDigest
   */

  @Schema(name = "contentDigest", required = false)
  public String getContentDigest() {
    return contentDigest;
  }

  public void setContentDigest(String contentDigest) {
    this.contentDigest = contentDigest;
  }

  SolrArtifact contentType(String contentType) {
    this.contentType = contentType;
    return this;
  }

  /**
   * Get contentType
   * @return contentType
   */

  @Schema(name = "contentType", required = false)
  public String getContentType() {
    return contentType;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  SolrArtifact collectionDate(Long collectionDate) {
    this.collectionDate = collectionDate;
    return this;
  }

  /**
   * Get collectionDate
   * @return collectionDate
   */

  @Schema(name = "collectionDate", required = false)
  public Long getCollectionDate() {
    return collectionDate;
  }

  public void setCollectionDate(Long collectionDate) {
    this.collectionDate = collectionDate;
  }

  SolrArtifact properties(Map<String, List<String>> properties) {
    this.properties = properties;
    return this;
  }

  SolrArtifact putPropertiesItem(String key, List<String> propertiesItem) {
    if (this.properties == null) {
      this.properties = new HashMap<>();
    }
    this.properties.put(key, propertiesItem);
    return this;
  }

  /**
   * Get properties
   * @return properties
   */
  @Valid
  @Schema(name = "properties", required = false)
  public Map<String, List<String>> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, List<String>> properties) {
    this.properties = properties;
  }

  public SolrArtifact sortUri(String sortUri) {
    this.sortUri = sortUri;
    return this;
  }

  public String getSortUri() {
    String uri = getUri();
    if ((sortUri == null) && (uri != null)) {
      this.setSortUri(uri.replaceAll("/", "\u0000"));
    }
    return sortUri;
  }

  public void setSortUri(String sortUri) {
    if (StringUtils.isEmpty(sortUri)) {
      throw new IllegalArgumentException("Cannot set null or empty SortURI");
    }
    this.sortUri = sortUri;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SolrArtifact artifact = (SolrArtifact) o;
    return Objects.equals(this.id, artifact.id) &&
        Objects.equals(this.collection, artifact.collection) &&
        Objects.equals(this.auid, artifact.auid) &&
        Objects.equals(this.uri, artifact.uri) &&
        Objects.equals(this.version, artifact.version) &&
        Objects.equals(this.committed, artifact.committed) &&
        Objects.equals(this.storageUrl, artifact.storageUrl) &&
        Objects.equals(this.contentLength, artifact.contentLength) &&
        Objects.equals(this.contentDigest, artifact.contentDigest) &&
        Objects.equals(this.contentType, artifact.contentType) &&
        Objects.equals(this.collectionDate, artifact.collectionDate) &&
        Objects.equals(this.properties, artifact.properties) &&
        Objects.equals(this.sortUri, artifact.sortUri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, collection, auid, uri, version, committed, storageUrl, contentLength, contentDigest, contentType, collectionDate, properties, sortUri);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Artifact {\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    collection: ").append(toIndentedString(collection)).append("\n");
    sb.append("    auid: ").append(toIndentedString(auid)).append("\n");
    sb.append("    uri: ").append(toIndentedString(uri)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    committed: ").append(toIndentedString(committed)).append("\n");
    sb.append("    storageUrl: ").append(toIndentedString(storageUrl)).append("\n");
    sb.append("    contentLength: ").append(toIndentedString(contentLength)).append("\n");
    sb.append("    contentDigest: ").append(toIndentedString(contentDigest)).append("\n");
    sb.append("    contentType: ").append(toIndentedString(contentType)).append("\n");
    sb.append("    collectionDate: ").append(toIndentedString(collectionDate)).append("\n");
    sb.append("    properties: ").append(toIndentedString(properties)).append("\n");
    sb.append("    sortUri: ").append(toIndentedString(sortUri)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
