package org.lockss.laaws.rs.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.validation.annotation.Validated;
import javax.validation.Valid;
import javax.validation.constraints.*;

/**
 * ArtifactProperties
 */
@Validated


public class ArtifactProperties   {
  @JsonProperty("namespace")
  private String namespace = "lockss";

  @JsonProperty("uuid")
  private String uuid = null;

  @JsonProperty("auid")
  private String auid = null;

  @JsonProperty("uri")
  private String uri = null;

  @JsonProperty("version")
  private Integer version = null;

  @JsonProperty("contentLength")
  private Long contentLength = null;

  @JsonProperty("contentDigest")
  private String contentDigest = null;

  @JsonProperty("collectionDate")
  private Long collectionDate = null;

  @JsonProperty("state")
  private String state = null;

  public ArtifactProperties namespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  /**
   * Get namespace
   * @return namespace
  **/
  @ApiModelProperty(value = "")


  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public ArtifactProperties uuid(String uuid) {
    this.uuid = uuid;
    return this;
  }

  /**
   * Get uuid
   * @return uuid
  **/
  @ApiModelProperty(value = "")


  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public ArtifactProperties auid(String auid) {
    this.auid = auid;
    return this;
  }

  /**
   * Get auid
   * @return auid
  **/
  @ApiModelProperty(value = "")


  public String getAuid() {
    return auid;
  }

  public void setAuid(String auid) {
    this.auid = auid;
  }

  public ArtifactProperties uri(String uri) {
    this.uri = uri;
    return this;
  }

  /**
   * Get uri
   * @return uri
  **/
  @ApiModelProperty(value = "")


  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  public ArtifactProperties version(Integer version) {
    this.version = version;
    return this;
  }

  /**
   * Get version
   * @return version
  **/
  @ApiModelProperty(value = "")


  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public ArtifactProperties contentLength(Long contentLength) {
    this.contentLength = contentLength;
    return this;
  }

  /**
   * Get contentLength
   * @return contentLength
  **/
  @ApiModelProperty(value = "")


  public Long getContentLength() {
    return contentLength;
  }

  public void setContentLength(Long contentLength) {
    this.contentLength = contentLength;
  }

  public ArtifactProperties contentDigest(String contentDigest) {
    this.contentDigest = contentDigest;
    return this;
  }

  /**
   * Get contentDigest
   * @return contentDigest
  **/
  @ApiModelProperty(value = "")


  public String getContentDigest() {
    return contentDigest;
  }

  public void setContentDigest(String contentDigest) {
    this.contentDigest = contentDigest;
  }

  public ArtifactProperties collectionDate(Long collectionDate) {
    this.collectionDate = collectionDate;
    return this;
  }

  /**
   * Get collectionDate
   * @return collectionDate
  **/
  @ApiModelProperty(value = "")


  public Long getCollectionDate() {
    return collectionDate;
  }

  public void setCollectionDate(Long collectionDate) {
    this.collectionDate = collectionDate;
  }

  public ArtifactProperties state(String state) {
    this.state = state;
    return this;
  }

  /**
   * Get state
   * @return state
  **/
  @ApiModelProperty(value = "")


  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ArtifactProperties artifactProperties = (ArtifactProperties) o;
    return Objects.equals(this.namespace, artifactProperties.namespace) &&
        Objects.equals(this.uuid, artifactProperties.uuid) &&
        Objects.equals(this.auid, artifactProperties.auid) &&
        Objects.equals(this.uri, artifactProperties.uri) &&
        Objects.equals(this.version, artifactProperties.version) &&
        Objects.equals(this.contentLength, artifactProperties.contentLength) &&
        Objects.equals(this.contentDigest, artifactProperties.contentDigest) &&
        Objects.equals(this.collectionDate, artifactProperties.collectionDate) &&
        Objects.equals(this.state, artifactProperties.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, uuid, auid, uri, version, contentLength, contentDigest, collectionDate, state);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ArtifactProperties {\n");
    
    sb.append("    namespace: ").append(toIndentedString(namespace)).append("\n");
    sb.append("    uuid: ").append(toIndentedString(uuid)).append("\n");
    sb.append("    auid: ").append(toIndentedString(auid)).append("\n");
    sb.append("    uri: ").append(toIndentedString(uri)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    contentLength: ").append(toIndentedString(contentLength)).append("\n");
    sb.append("    contentDigest: ").append(toIndentedString(contentDigest)).append("\n");
    sb.append("    collectionDate: ").append(toIndentedString(collectionDate)).append("\n");
    sb.append("    state: ").append(toIndentedString(state)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

