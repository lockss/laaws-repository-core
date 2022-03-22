package org.lockss.laaws.rs.model;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lockss.laaws.rs.model.Artifact;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * ArtifactData
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen")
public class ArtifactData   {

  @JsonProperty("storedDate")
  private Long storedDate;

  @JsonProperty("data")
  private org.springframework.core.io.Resource data;

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

  public ArtifactData storedDate(Long storedDate) {
    this.storedDate = storedDate;
    return this;
  }

  /**
   * Get storedDate
   * @return storedDate
  */
  
  @Schema(name = "storedDate", required = false)
  public Long getStoredDate() {
    return storedDate;
  }

  public void setStoredDate(Long storedDate) {
    this.storedDate = storedDate;
  }

  public ArtifactData data(org.springframework.core.io.Resource data) {
    this.data = data;
    return this;
  }

  /**
   * Get data
   * @return data
  */
  @Valid 
  @Schema(name = "data", required = false)
  public org.springframework.core.io.Resource getData() {
    return data;
  }

  public void setData(org.springframework.core.io.Resource data) {
    this.data = data;
  }

  public ArtifactData id(String id) {
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

  public ArtifactData collection(String collection) {
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

  public ArtifactData auid(String auid) {
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

  public ArtifactData uri(String uri) {
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

  public ArtifactData version(Integer version) {
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

  public ArtifactData committed(Boolean committed) {
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

  public ArtifactData storageUrl(String storageUrl) {
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

  public ArtifactData contentLength(Long contentLength) {
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

  public ArtifactData contentDigest(String contentDigest) {
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

  public ArtifactData contentType(String contentType) {
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

  public ArtifactData collectionDate(Long collectionDate) {
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

  public ArtifactData properties(Map<String, List<String>> properties) {
    this.properties = properties;
    return this;
  }

  public ArtifactData putPropertiesItem(String key, List<String> propertiesItem) {
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
    ArtifactData artifactData = (ArtifactData) o;
    return Objects.equals(this.storedDate, artifactData.storedDate) &&
        Objects.equals(this.data, artifactData.data) &&
        Objects.equals(this.id, artifactData.id) &&
        Objects.equals(this.collection, artifactData.collection) &&
        Objects.equals(this.auid, artifactData.auid) &&
        Objects.equals(this.uri, artifactData.uri) &&
        Objects.equals(this.version, artifactData.version) &&
        Objects.equals(this.committed, artifactData.committed) &&
        Objects.equals(this.storageUrl, artifactData.storageUrl) &&
        Objects.equals(this.contentLength, artifactData.contentLength) &&
        Objects.equals(this.contentDigest, artifactData.contentDigest) &&
        Objects.equals(this.contentType, artifactData.contentType) &&
        Objects.equals(this.collectionDate, artifactData.collectionDate) &&
        Objects.equals(this.properties, artifactData.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storedDate, data, id, collection, auid, uri, version, committed, storageUrl, contentLength, contentDigest, contentType, collectionDate, properties);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ArtifactData {\n");
    sb.append("    storedDate: ").append(toIndentedString(storedDate)).append("\n");
    sb.append("    data: ").append(toIndentedString(data)).append("\n");
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

