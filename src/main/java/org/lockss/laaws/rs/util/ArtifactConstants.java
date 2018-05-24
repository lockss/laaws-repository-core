/*
 * Copyright (c) 2017-2018, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.util;

/**
 * Class to hold constants related to artifacts.
 */
public class ArtifactConstants {
    public static final String ARTIFACTID_ID_KEY = "X-Lockss-ArtifactId";
    public static final String ARTIFACTID_COLLECTION_KEY = "X-Lockss-Collection";
    public static final String ARTIFACTID_AUID_KEY = "X-Lockss-AuId";
    public static final String ARTIFACTID_URI_KEY = "X-Lockss-Uri";
    public static final String ARTIFACTID_VERSION_KEY = "X-Lockss-Version";
    public static final String ARTIFACTID_ORIGIN_KEY = "X-Lockss-Origin";
    public static final String ARTIFACTID_CONTENT_LENGTH_KEY = "WARC-Payload-Length";
    public static final String ARTIFACTID_DIGEST_KEY = "WARC-Payload-Digest";

    public static final String ARTIFACT_STATE_COMMITTED = "X-Lockss-Committed";
    public static final String ARTIFACT_STATE_DELETED = "X-Lockss-Deleted";
}
