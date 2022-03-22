package org.lockss.laaws.rs.model;

import java.io.Serializable;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;

import io.swagger.v3.oas.annotations.media.Schema;


import javax.annotation.Generated;

/**
 * Artifact
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen")
public class Artifact implements Serializable {

  @JsonProperty("id")
  private String id;

  @JsonProperty("collection")
  private String collection;

  @JsonProperty("auid")
  private String auid;

  @JsonProperty("uri")
  private String uri;

  @JsonProperty("version")
  private Integer version;

  @JsonProperty("committed")
  private Boolean committed;

  @JsonProperty("storageUrl")
  private String storageUrl;

  @JsonProperty("contentLength")
  private Long contentLength;

  @JsonProperty("contentDigest")
  private String contentDigest;

  @JsonProperty("contentType")
  private String contentType;

  @JsonProperty("collectionDate")
  private Long collectionDate;

  @JsonProperty("properties")
  @Valid
  private Map<String, List<String>> properties = null;

  public Artifact id(String id) {
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

  public Artifact collection(String collection) {
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

  public Artifact auid(String auid) {
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

  public Artifact uri(String uri) {
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

  public Artifact version(Integer version) {
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

  public Artifact committed(Boolean committed) {
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

  public Artifact storageUrl(String storageUrl) {
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

  public Artifact contentLength(Long contentLength) {
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

  public Artifact contentDigest(String contentDigest) {
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

  public Artifact contentType(String contentType) {
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

  public Artifact collectionDate(Long collectionDate) {
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

  public Artifact properties(Map<String, List<String>> properties) {
    this.properties = properties;
    return this;
  }

  public Artifact putPropertiesItem(String key, List<String> propertiesItem) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Artifact artifact = (Artifact) o;
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
        Objects.equals(this.properties, artifact.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, collection, auid, uri, version, committed, storageUrl, contentLength, contentDigest, contentType, collectionDate, properties);
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

