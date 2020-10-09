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
    // Artifact identity
    public static final String ARTIFACT_ID_KEY = "X-LockssRepo-Artifact-Id";
    public static final String ARTIFACT_COLLECTION_KEY = "X-LockssRepo-Artifact-Collection";
    public static final String ARTIFACT_AUID_KEY = "X-LockssRepo-Artifact-AuId";
    public static final String ARTIFACT_URI_KEY = "X-LockssRepo-Artifact-Uri";
    public static final String ARTIFACT_VERSION_KEY = "X-LockssRepo-Artifact-Version";

    // Repository state
    public static final String ARTIFACT_STATE_COMMITTED = "X-LockssRepo-Artifact-Committed";
    public static final String ARTIFACT_STATE_DELETED = "X-LockssRepo-Artifact-Deleted";

    // Repository
    public static final String ARTIFACT_LENGTH_KEY = "X-LockssRepo-Artifact-Length";
    public static final String ARTIFACT_DIGEST_KEY = "X-LockssRepo-Artifact-Digest";

    // Miscellaneous
    public static final String ARTIFACT_ORIGIN_KEY = "X-LockssRepo-Artifact-Origin";
    public static final String ARTIFACT_CREATED_DATE = "X-LockssRepo-Artifact-Created";
}
