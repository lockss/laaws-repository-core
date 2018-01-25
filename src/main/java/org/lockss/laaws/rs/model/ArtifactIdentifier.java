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

import com.google.common.collect.ComparisonChain;

import java.io.Serializable;
import java.util.UUID;

public class ArtifactIdentifier implements Serializable, Comparable<ArtifactIdentifier> {
    private String id;
    private String collection;
    private String auid;
    private String uri;
    private String version;

    public ArtifactIdentifier(String collection, String auid, String uri, String version) {
        this(UUID.randomUUID().toString(), collection, auid, uri, version);
    }

    public ArtifactIdentifier(String id, String collection, String auid, String uri, String version) {
        this.id = id;
        this.collection = collection;
        this.auid = auid;
        this.uri = uri;
        this.version = version;
    }

    public String getCollection() {
        return collection;
    }

    public String getAuid() {
        return auid;
    }

    public String getUri() {
        return uri;
    }

    public String getVersion() {
        return version;
    }

    public String getId() {
        return id;
    }

    @Override
    public int compareTo(ArtifactIdentifier other) {
        return ComparisonChain.start()
                .compare(this.getCollection(), other.getCollection())
                .compare(this.getAuid(), other.getAuid())
                .compare(this.getUri(), other.getUri())
                .compare(this.getVersion(), other.getVersion())
                .compare(this.getId(), this.getId())
                .result();
    }
}
