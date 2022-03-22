package org.lockss.laaws.rs.model;

import java.io.Serializable;
import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ComparisonChain;
import org.lockss.util.PreOrderComparator;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * ArtifactIdentifier
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen")
public class ArtifactIdentifier implements Serializable, Comparable<org.lockss.laaws.rs.model.ArtifactIdentifier> {

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

  public ArtifactIdentifier id(String id) {
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

  public ArtifactIdentifier collection(String collection) {
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

  public ArtifactIdentifier auid(String auid) {
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

  public ArtifactIdentifier uri(String uri) {
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

  public ArtifactIdentifier version(Integer version) {
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

//  @Override
//  public boolean equals(Object o) {
//    if (this == o) {
//      return true;
//    }
//    if (o == null || getClass() != o.getClass()) {
//      return false;
//    }
//    ArtifactIdentifier artifactIdentifier = (ArtifactIdentifier) o;
//    return Objects.equals(this.id, artifactIdentifier.id) &&
//        Objects.equals(this.collection, artifactIdentifier.collection) &&
//        Objects.equals(this.auid, artifactIdentifier.auid) &&
//        Objects.equals(this.uri, artifactIdentifier.uri) &&
//        Objects.equals(this.version, artifactIdentifier.version);
//  }
//
//  @Override
//  public int hashCode() {
//    return Objects.hash(id, collection, auid, uri, version);
//  }

  /**
   * Implements Comparable - The canonical order here from most significant to least significant is the assigned
   * collection, archival unit (AU), URI, and version. The artifactId is a unique internal handle and has no
   * useful ordering in this context, and so is not included in the comparison calculation.
   *
   * @param other The other instance of ArtifactIdentifier to compare against.
   * @return An integer indicating whether order relative to other.
   */
  @Override
  public int compareTo(ArtifactIdentifier other) {
    return ComparisonChain.start()
        .compare(this.getCollection(), other.getCollection())
        .compare(this.getAuid(), other.getAuid())
        .compare(this.getUri(), other.getUri(),
            PreOrderComparator.INSTANCE)
        .compare(this.getVersion(), other.getVersion())
//                .compare(this.getId(), this.getId())
        .result();
  }

  @Override
  public boolean equals(Object o) {
    ArtifactIdentifier other = (ArtifactIdentifier)o;
    return other != null && this.compareTo(other) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(collection, auid, uri, version);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ArtifactIdentifier {\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    collection: ").append(toIndentedString(collection)).append("\n");
    sb.append("    auid: ").append(toIndentedString(auid)).append("\n");
    sb.append("    uri: ").append(toIndentedString(uri)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
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

