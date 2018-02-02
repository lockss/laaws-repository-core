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

package org.lockss.laaws.rs.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ArtifactIndexData {
    private static final Log log = LogFactory.getLog(ArtifactIndexData.class);

    private String id;
    private String collection;
    private String auid;
    private String uri;
    private String version;
    private Boolean committed;

    private long warcRecordOffset;
    private String warcFilePath;
    private String warcRecordId;

    public ArtifactIndexData(String id, String collection, String auid, String uri, String version, Boolean committed,
                             String warcRecordId, String warcFilePath, long warcRecordOffset) {
        this.id = id;
        this.collection = collection;
        this.auid = auid;
        this.uri = uri;
        this.version = version;
        this.committed = committed;
        this.warcRecordId = warcRecordId;
        this.warcFilePath = warcFilePath;
        this.warcRecordOffset = warcRecordOffset;
    }

    public ArtifactIdentifier getIdentifier() {
        return new ArtifactIdentifier(id, collection, auid, uri, version);
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getAuid() {
        return auid;
    }

    public void setAuid(String auid) {
        this.auid = auid;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getId() {
        return id;
    }

    public Boolean getCommitted() {
        return committed;
    }

    public void setCommitted(Boolean committed) {
        this.committed = committed;
    }

    public long getWarcRecordOffset() {
        return warcRecordOffset;
    }

    public void setWarcRecordOffset(long warcRecordOffset) {
        this.warcRecordOffset = warcRecordOffset;
    }

    public String getWarcFilePath() {
        return warcFilePath;
    }

    public void setWarcFilePath(String warcFilePath) {
        this.warcFilePath = warcFilePath;
    }

    public String getWarcRecordId() {
        return warcRecordId;
    }

    public void setWarcRecordId(String warcRecordId) {
        this.warcRecordId = warcRecordId;
    }

    @Override
    public String toString() {
        return "ArtifactIndexData{" +
                "id='" + id + '\'' +
                ", collection='" + collection + '\'' +
                ", auid='" + auid + '\'' +
                ", uri='" + uri + '\'' +
                ", version='" + version + '\'' +
                ", committed=" + committed +
                ", warcRecordOffset=" + warcRecordOffset +
                ", warcFilePath='" + warcFilePath + '\'' +
                ", warcRecordId='" + warcRecordId + '\'' +
                '}';
    }
}
