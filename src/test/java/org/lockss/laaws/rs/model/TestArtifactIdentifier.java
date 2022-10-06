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

import org.junit.jupiter.api.*;
import org.lockss.util.test.LockssTestCase5;

public class TestArtifactIdentifier extends LockssTestCase5 {
    private final static String ARTIFACT_NAMESPACE = "ns1";
    private final static String ARTIFACT_AUID = "auid123";
    private final static String ARTIFACT_URI = "uri123";
    private final static Integer ARTIFACT_VERSION = 1;

    private ArtifactIdentifier identifier;

    @BeforeEach
    public void setUp() throws Exception {
        // Instantiate an ArtifactInstance to test getters against
        this.identifier = new ArtifactIdentifier(
            ARTIFACT_NAMESPACE,
                ARTIFACT_AUID,
                ARTIFACT_URI,
                ARTIFACT_VERSION
        );
    }

    @Test
    public void getNamespace() {
        assertEquals(ARTIFACT_NAMESPACE, identifier.getNamespace());
    }

    @Test
    public void getAuid() {
        assertEquals(ARTIFACT_AUID, identifier.getAuid());
    }

    @Test
    public void getUri() {
        assertEquals(ARTIFACT_URI, identifier.getUri());
    }

    @Test
    public void getVersion() {
        assertEquals(ARTIFACT_VERSION, identifier.getVersion());
    }

    @Test
    public void compareTo_equal() {
        ArtifactIdentifier id1 = new ArtifactIdentifier(
                "id1",
                "c",
                "a",
                "u",
                1
        );

        ArtifactIdentifier id2 = new ArtifactIdentifier(
                "id1",
                "c",
                "a",
                "u",
                1
        );

        assertTrue(id1.compareTo(id2) == 0);
    }

    @Test
    public void compareTo_greaterThan() {
        ArtifactIdentifier id1 = new ArtifactIdentifier(
                "c",
                "a",
                "u2",
                1
        );

        ArtifactIdentifier id2 = new ArtifactIdentifier(
                "c",
                "a",
                "u1",
                1
        );

        assertTrue(id1.compareTo(id2) >= 1);
    }

    @Test
    public void compareTo_lessThan() {
        ArtifactIdentifier id1 = new ArtifactIdentifier(
                "b",
                "a",
                "u",
                1
        );

        ArtifactIdentifier id2 = new ArtifactIdentifier(
                "c",
                "a",
                "u",
                1
        );

        assertTrue(id1.compareTo(id2) <= -1);
    }

  
  ArtifactIdentifier artId(String namespace, String auid, String uri, int version) {
    return new ArtifactIdentifier(namespace, auid, uri, version);
  }

  @Test
  public void sortOrder() {
    assertTrue(artId("c", "a", "foo", 1).compareTo(artId("c", "a", "foo/", 1)) <= -1);
    assertTrue(artId("c", "a", "foo/", 1).compareTo(artId("c", "a", "foo.", 1)) <= -1);
    assertTrue(artId("c", "a", "foo/", 1).compareTo(artId("c", "a", "foo/a", 1)) <= -1);
    assertTrue(artId("c", "a", "bar", 1).compareTo(artId("c", "a", "foo", 1)) <= -1);
  };
}
