/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
package org.lockss.laaws.rs.model;

import java.util.Objects;

/**
 * The information related to pagination of content.
 */
public class PageInfo {
  private Integer totalCount = null;
  private Integer resultsPerPage = null;
  private String continuationToken = null;
  private String curLink = null;
  private String nextLink = null;

  /**
   * Provides the total number of results.
   * 
   * @return an Integer with the total number of results.
   */
  public Integer getTotalCount() {
    return totalCount;
  }

  /**
   * Saves the total number of results.
   * 
   * @param totalCount An Integer with the total number of results.
   */
  public void setTotalCount(Integer totalCount) {
    this.totalCount = totalCount;
  }

  /**
   * Provides the number of results per page.
   * 
   * @return an Integer with the number of results per page.
   */
  public Integer getResultsPerPage() {
    return resultsPerPage;
  }

  /**
   * Saves the number of results per page.
   * 
   * @param resultsPerPage An Integer with the number of results per page.
   */
  public void setResultsPerPage(Integer resultsPerPage) {
    this.resultsPerPage = resultsPerPage;
  }

  /**
   * Provides the continuation token.
   * 
   * @return a String with the continuation token.
   */
  public String getContinuationToken() {
    return continuationToken;
  }

  /**
   * Saves the continuation token.
   * 
   * @param continuationToken A String with the continuation token.
   */
  public void setContinuationToken(String continuationToken) {
    this.continuationToken = continuationToken;
  }

  /**
   * Provides link to the current page.
   * 
   * @return a String with the link to the current page.
   */
  public String getCurLink() {
    return curLink;
  }

  /**
   * Saves the link to the current page.
   * 
   * @param curLink A String with the link to the current page.
   */
  public void setCurLink(String curLink) {
    this.curLink = curLink;
  }

  /**
   * Provides link to the next page.
   * 
   * @return a String with the link to the next page.
   */
  public String getNextLink() {
    return nextLink;
  }

  /**
   * Saves the link to the next page.
   * 
   * @param nextLink A String with the link to the next page.
   */
  public void setNextLink(String nextLink) {
    this.nextLink = nextLink;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PageInfo pageInfo = (PageInfo) o;
    return Objects.equals(this.totalCount, pageInfo.totalCount) &&
	Objects.equals(this.resultsPerPage, pageInfo.resultsPerPage) &&
        Objects.equals(this.continuationToken, pageInfo.continuationToken) &&
        Objects.equals(this.curLink, pageInfo.curLink) &&
        Objects.equals(this.nextLink, pageInfo.nextLink);
  }

  @Override
  public int hashCode() {
    return Objects.hash(totalCount, resultsPerPage, continuationToken, curLink,
	nextLink);
  }

  @Override
  public String toString() {
    return "[PageInfo totalCount=" + totalCount + ", resultsPerPage="
	+ resultsPerPage + ", continuationToken=" + continuationToken
	+ ", curLink=" + curLink + ", nextLink=" + nextLink + "]";
  }
}
