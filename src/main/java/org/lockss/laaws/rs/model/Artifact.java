/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.beans.Field;
import org.lockss.log.L4JLogger;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * LOCKSS repository Artifact
 *
 * Represents an atomic unit of data in a LOCKSS repository.
 */
public class Artifact implements Serializable {
    private static final long serialVersionUID = 1961138745993115018L;
    private final static L4JLogger log = L4JLogger.getLogger();

    // These need to match those in the Artifact model defined in the Swagger/OpenAPI spec
    public final static String ARTIFACT_NAMESPACE_KEY = "namespace";
    public final static String ARTIFACT_UUID_KEY = "uuid";
    public final static String ARTIFACT_AUID_KEY = "auid";
    public final static String ARTIFACT_URI_KEY = "uri";
    public final static String ARTIFACT_VERSION_KEY = "version";
    public final static String ARTIFACT_COMMITTED_STATUS_KEY = "committed";
    public final static String ARTIFACT_LENGTH_KEY = "contentLength";
    public final static String ARTIFACT_DIGEST_KEY = "contentDigest";
    public final static String ARTIFACT_COLLECTION_DATE_KEY = "collectionDate";

    // We have chosen to map the artifact UUID to the Solr document's "id" field
    // for the sake of convention, even though Solr appears to support assigning
    // another field as the unique identifier.
    @Field("id")
    private String uuid;

    @Field(ARTIFACT_NAMESPACE_KEY)
    private String namespace = "lockss";

    @Field(ARTIFACT_AUID_KEY)
    private String auid;

    @Field(ARTIFACT_URI_KEY)
    private String uri;

    @Field("sortUri")
    private String sortUri;

    @Field(ARTIFACT_VERSION_KEY)
    private Integer version;

    @Field(ARTIFACT_COMMITTED_STATUS_KEY)
    private Boolean committed;

    @Field("storageUrl")
    private String storageUrl;

    @Field(ARTIFACT_LENGTH_KEY)
    private long contentLength;

    @Field(ARTIFACT_DIGEST_KEY)
    private String contentDigest;

    @Field(ARTIFACT_COLLECTION_DATE_KEY)
    private long collectionDate;

    /**
     * Constructor. Needed by SolrJ for getBeans() support. *
     *
     * TODO: Reconcile difference with constructor below, which checks parameters for illegal arguments.
     */
    public Artifact() {
        // Intentionally left blank
    }

    public Artifact(ArtifactIdentifier aid, Boolean committed, String storageUrl, long contentLength, String contentDigest) {
        this(
                aid.getUuid(), aid.getNamespace(), aid.getAuid(), aid.getUri(), aid.getVersion(),
                committed,
                storageUrl,
                contentLength,
                contentDigest
        );
    }

    public Artifact(String uuid, String namespace, String auid, String uri, Integer version, Boolean committed,
                    String storageUrl, long contentLength, String contentDigest) {
        if (StringUtils.isEmpty(uuid)) {
          throw new IllegalArgumentException(
              "Cannot create Artifact with null or empty UUID");
        }
        this.uuid = uuid;

        if (StringUtils.isEmpty(namespace)) {
          throw new IllegalArgumentException(
              "Cannot create Artifact with null or empty namespace");
        }
        this.namespace = namespace;

        if (StringUtils.isEmpty(auid)) {
          throw new IllegalArgumentException(
              "Cannot create Artifact with null or empty auid");
        }
        this.auid = auid;

        if (StringUtils.isEmpty(uri)) {
          throw new IllegalArgumentException(
              "Cannot create Artifact with null or empty URI");
        }
        this.setUri(uri);

        if (version == null) {
          throw new IllegalArgumentException(
              "Cannot create Artifact with null version");
        }
        this.version = version;

        if (committed == null) {
          throw new IllegalArgumentException(
              "Cannot create Artifact with null commit status");
        }
        this.committed = committed;

        if (StringUtils.isEmpty(storageUrl)) {
          throw new IllegalArgumentException("Cannot create "
              + "Artifact with null or empty storageUrl");
        }
        this.storageUrl = storageUrl;

        this.contentLength = contentLength;
        this.contentDigest = contentDigest;
    }

    public ArtifactIdentifier getIdentifier() {
        return new ArtifactIdentifier(uuid, namespace, auid, uri, version);
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        if (StringUtils.isEmpty(namespace)) {
          throw new IllegalArgumentException(
              "Cannot set null or empty namespace");
        }
        this.namespace = namespace;
    }

    public String getAuid() {
        return auid;
    }

    public void setAuid(String auid) {
        if (StringUtils.isEmpty(auid)) {
          throw new IllegalArgumentException("Cannot set null or empty auid");
        }
        this.auid = auid;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        if (StringUtils.isEmpty(uri)) {
          throw new IllegalArgumentException("Cannot set null or empty URI");
        }
      this.uri = uri;
      this.setSortUri(uri.replaceAll("/", "\u0000"));
    }

    public String getSortUri() {
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

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
//        if (StringUtils.isEmpty(version)) {
//          throw new IllegalArgumentException(
//              "Cannot set null or empty version");
//        }
        if (version == null) {
            throw new IllegalArgumentException("Cannot set null version");
        }

        this.version = version;
    }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getUuid() {
        return uuid;
    }

    public Boolean getCommitted() {
        return committed;
    }

    public boolean isCommitted() {
      return getCommitted() == true;
    }

    public void setCommitted(Boolean committed) {
        if (committed == null) {
          throw new IllegalArgumentException("Cannot set null commit status");
        }
        this.committed = committed;
    }

    public String getStorageUrl() {
        return storageUrl;
    }

    public void setStorageUrl(String storageUrl) {
        if (StringUtils.isEmpty(storageUrl)) {
          throw new IllegalArgumentException(
              "Cannot set null or empty storageUrl");
        }
        this.storageUrl = storageUrl;
    }

    public long getContentLength() {
        return contentLength;
    }

    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }

    public String getContentDigest() {
        return contentDigest;
    }

    public void setContentDigest(String contentDigest) {
        this.contentDigest = contentDigest;
    }

  /**
   * Provides the artifact collection date.
   * 
   * @return a long with the artifact collection date in milliseconds since the
   *         epoch.
   */
  public long getCollectionDate() {
    return collectionDate;
  }

  /**
   * Saves the artifact collection date.
   * 
   * @param collectionDate
   *          A long with the artifact collection date in milliseconds since the
   *          epoch.
   */
  public void setCollectionDate(long collectionDate) {
    this.collectionDate = collectionDate;
  }

    @Override
    public String toString() {
        return "Artifact{" +
                "uuid='" + uuid + '\'' +
                ", namespace='" + namespace + '\'' +
                ", auid='" + auid + '\'' +
                ", uri='" + uri + '\'' +
//                 ", sortUri='" + sortUri + '\'' +
                ", version='" + version + '\'' +
                ", committed=" + committed +
                ", storageUrl='" + storageUrl + '\'' +
                ", contentLength='" + contentLength + '\'' +
                ", contentDigest='" + contentDigest + '\'' +
                ", collectionDate='" + collectionDate + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
      // Cast to Artifact is safe because equalsExceptStorageUrl has
      // already checked instanceof
      return equalsExceptStorageUrl(o)
        && storageUrl.equalsIgnoreCase(((Artifact)o).getStorageUrl());
    }

    public boolean equalsExceptStorageUrl(Object o) {
        if (!(o instanceof Artifact)) {
          return false;
        }
        Artifact other = (Artifact)o;

        return other != null
            && ((this.getIdentifier() == null && other.getIdentifier() == null)
        	|| (this.getIdentifier() != null && this.getIdentifier().equals(other.getIdentifier())))
            && committed.equals(other.getCommitted())
            && getContentLength() == other.getContentLength()
            && ((contentDigest == null && other.getContentDigest() == null)
        	|| (contentDigest != null && contentDigest.equals(other.getContentDigest())))
            && getCollectionDate() == other.getCollectionDate();
    }


    /** Return a String that uniquely identifies the Artifact with the
     * specified values.  version -1 means latest version */
    public static String makeKey(String namespace, String auid,
				 String uri, int version) {
	StringBuilder sb = new StringBuilder(200);
	sb.append(namespace);
	sb.append(":");
	sb.append(auid);
	sb.append(":");
	sb.append(uri);
	sb.append(":");
	sb.append(version);
	return sb.toString();
    }

    /** Return a String that uniquely identifies "the latest committed
     * version of the Artifact with the specified values" */
    public static String makeLatestKey(String namespace, String auid,
				       String uri) {
	return makeKey(namespace, auid, uri, -1);
    }

    /** Return a String that uniquely identifies this Artifact */
    public String makeKey() {
	return Artifact.makeKey(getNamespace(), getAuid(),
				getUri(), getVersion());
    }

    /** Return a String that uniquely identifies "the latest committed
     * version of the Artifact" */
    public String makeLatestKey() {
	return Artifact.makeLatestKey(getNamespace(), getAuid(), getUri());
    }

    // matches ":<ver>" at the end
    static Pattern LATEST_VER_PATTERN = Pattern.compile(":[^:]+$");

    /** Return a String that uniquely identifies "the latest committed
     * version of the Artifact with the specified key" */
    public static String makeLatestKey(String key) {
	Matcher m1 = LATEST_VER_PATTERN.matcher(key);
	if (m1.find()) {
	    return m1.replaceAll(":-1");
	}
	return null;
    }

  public Artifact copyOf() {

    Artifact ret = new Artifact(
        this.getUuid(),
        this.getNamespace(),
        this.getAuid(),
        this.getUri(),
        this.getVersion(),
        this.getCommitted(),
        this.getStorageUrl(),
        this.getContentLength(),
        this.getContentDigest()
    );

    ret.setCollectionDate(this.getCollectionDate());

    return ret;
  }
}
