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
 * AuSize
 */
@Validated


public class AuSize   {
  @JsonProperty("totalLatestVersions")
  private Long totalLatestVersions = null;

  @JsonProperty("totalAllVersions")
  private Long totalAllVersions = null;

  @JsonProperty("totalWarcSize")
  private Long totalWarcSize = null;

  public AuSize totalLatestVersions(Long totalLatestVersions) {
    this.totalLatestVersions = totalLatestVersions;
    return this;
  }

  /**
   * Get totalLatestVersions
   * @return totalLatestVersions
  **/
  @ApiModelProperty(value = "")


  public Long getTotalLatestVersions() {
    return totalLatestVersions;
  }

  public void setTotalLatestVersions(Long totalLatestVersions) {
    this.totalLatestVersions = totalLatestVersions;
  }

  public AuSize totalAllVersions(Long totalAllVersions) {
    this.totalAllVersions = totalAllVersions;
    return this;
  }

  /**
   * Get totalAllVersions
   * @return totalAllVersions
  **/
  @ApiModelProperty(value = "")


  public Long getTotalAllVersions() {
    return totalAllVersions;
  }

  public void setTotalAllVersions(Long totalAllVersions) {
    this.totalAllVersions = totalAllVersions;
  }

  public AuSize totalWarcSize(Long totalWarcSize) {
    this.totalWarcSize = totalWarcSize;
    return this;
  }

  /**
   * Get totalWarcSize
   * @return totalWarcSize
  **/
  @ApiModelProperty(value = "")


  public Long getTotalWarcSize() {
    return totalWarcSize;
  }

  public void setTotalWarcSize(Long totalWarcSize) {
    this.totalWarcSize = totalWarcSize;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuSize auSize = (AuSize) o;
    return Objects.equals(this.totalLatestVersions, auSize.totalLatestVersions) &&
        Objects.equals(this.totalAllVersions, auSize.totalAllVersions) &&
        Objects.equals(this.totalWarcSize, auSize.totalWarcSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(totalLatestVersions, totalAllVersions, totalWarcSize);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AuSize {\n");
    
    sb.append("    totalLatestVersions: ").append(toIndentedString(totalLatestVersions)).append("\n");
    sb.append("    totalAllVersions: ").append(toIndentedString(totalAllVersions)).append("\n");
    sb.append("    totalWarcSize: ").append(toIndentedString(totalWarcSize)).append("\n");
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

