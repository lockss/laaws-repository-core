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

package org.lockss.laaws.rs.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.lockss.test.LockssTestCase4;

/**
 * Test class for {@code org.lockss.laaws.rs.model.ArtifactIndexData}
 */
public class TestArtifactIndexData extends LockssTestCase4 {
    private static final Log log = LogFactory.getLog(TestArtifactIndexData.class);

    @Test
    public void testConstructor() {
      String expectedMessage =
	  "Cannot create ArtifactIndexData with null or empty id";

      try {
	new ArtifactIndexData(null, null, null, null, null, null, null, null,
	    0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	new ArtifactIndexData("", null, null, null, null, null, null, null,
	    0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      expectedMessage =
	  "Cannot create ArtifactIndexData with null or empty collection";

      try {
	new ArtifactIndexData("aidid", null, null, null, null, null, null, null,
	    0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	new ArtifactIndexData("aidid", "", null, null, null, null, null, null,
	    0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      expectedMessage =
	  "Cannot create ArtifactIndexData with null or empty auid";

      try {
	new ArtifactIndexData("aidid", "coll", null, null, null, null, null,
	    null, 0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	new ArtifactIndexData("aidid", "coll", "", null, null, null, null,
	    null, 0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      expectedMessage =
	  "Cannot create ArtifactIndexData with null or empty URI";

      try {
	new ArtifactIndexData("aidid", "coll", "auid", null, null, null, null,
	    null, 0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	new ArtifactIndexData("aidid", "coll", "auid", "", null, null, null,
	    null, 0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      expectedMessage =
	  "Cannot create ArtifactIndexData with null or empty version";

      try {
	new ArtifactIndexData("aidid", "coll", "auid", "uri", null, null, null,
	    null, 0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	new ArtifactIndexData("aidid", "coll", "auid", "uri", "", null, null,
	    null, 0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      expectedMessage =
	  "Cannot create ArtifactIndexData with null commit status";

      try {
	new ArtifactIndexData("aidid", "coll", "auid", "uri", "ver", null,
	    null, null, 0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      expectedMessage =
	  "Cannot create ArtifactIndexData with null or empty warcRecordId";

      try {
	new ArtifactIndexData("aidid", "coll", "auid", "uri", "ver",
	    Boolean.FALSE, null, null, 0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	new ArtifactIndexData("aidid", "coll", "auid", "uri", "ver",
	    Boolean.FALSE, "", null, 0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      expectedMessage =
	  "Cannot create ArtifactIndexData with null or empty warcFilePath";

      try {
	new ArtifactIndexData("aidid", "coll", "auid", "uri", "ver",
	    Boolean.TRUE, "wri", null, 0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	new ArtifactIndexData("aidid", "coll", "auid", "uri", "ver",
	    Boolean.TRUE, "wri", "", 0);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      ArtifactIndexData aidata = new ArtifactIndexData("aidid", "coll", "auid",
	  "uri", "ver", Boolean.TRUE, "wri", "wfp", 1);
      assertEquals("aidid", aidata.getId());
      assertEquals("coll", aidata.getCollection());
      assertEquals("auid", aidata.getAuid());
      assertEquals("uri", aidata.getUri());
      assertEquals("ver", aidata.getVersion());
      assertTrue(aidata.getCommitted());
      assertEquals("wri", aidata.getWarcRecordId());
      assertEquals("wfp", aidata.getWarcFilePath());
      assertEquals(1, aidata.getWarcRecordOffset());
    }

    @Test
    public void testSetters() {
      ArtifactIndexData aidata = new ArtifactIndexData("aidid", "coll", "auid",
	  "uri", "ver", Boolean.TRUE, "wri", "wfp", 1);

      String expectedMessage = "Cannot set null or empty collection";

      try {
	aidata.setCollection(null);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	aidata.setCollection("");
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      aidata.setCollection("newColl");
      assertEquals("newColl", aidata.getCollection());

      expectedMessage = "Cannot set null or empty auid";

      try {
	aidata.setAuid(null);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	aidata.setAuid("");
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      aidata.setAuid("newAuid");
      assertEquals("newAuid", aidata.getAuid());

      expectedMessage = "Cannot set null or empty URI";

      try {
	aidata.setUri(null);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	aidata.setUri("");
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      aidata.setUri("newUri");
      assertEquals("newUri", aidata.getUri());

      expectedMessage = "Cannot set null or empty version";

      try {
	aidata.setVersion(null);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	aidata.setVersion("");
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      aidata.setVersion("newVer");
      assertEquals("newVer", aidata.getVersion());

      expectedMessage = "Cannot set null commit status";

      try {
	aidata.setCommitted(null);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      aidata.setCommitted(Boolean.FALSE);
      assertEquals(Boolean.FALSE, aidata.getCommitted());

      expectedMessage = "Cannot set null or empty warcRecordId";

      try {
	aidata.setWarcRecordId(null);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	aidata.setWarcRecordId("");
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      aidata.setWarcRecordId("newWri");
      assertEquals("newWri", aidata.getWarcRecordId());

      expectedMessage = "Cannot set null or empty warcFilePath";

      try {
	aidata.setWarcFilePath(null);
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      try {
	aidata.setWarcFilePath("");
        fail("Should have thrown IllegalArgumentException(" + expectedMessage
  	  + ")");
      } catch (IllegalArgumentException iae) {
        assertEquals(expectedMessage, iae.getMessage());
      }

      aidata.setWarcFilePath("newWfp");
      assertEquals("newWfp", aidata.getWarcFilePath());

      aidata.setWarcRecordOffset(123);
      assertEquals(123, aidata.getWarcRecordOffset());
    }
}