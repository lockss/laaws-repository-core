/*

Copyright (c) 2000-2018, Board of Trustees of Leland Stanford Jr. University,
All rights reserved.

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

package org.lockss.laaws.rs.test;

import org.junit.*;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.lockss.util.test.LockssTestCase5;

public abstract class AbstractArtifactDataStoreTest<ID extends ArtifactIdentifier, AD extends ArtifactData, MD extends RepositoryArtifactMetadata>
    extends LockssTestCase5 {

  protected ArtifactDataStore<ID, AD, MD> store;
  
  protected abstract ArtifactDataStore<ID, AD, MD> makeArtifactDataStore() throws Exception;
  
  @Before
  public void setUpArtifactDataStore() throws Exception {
    this.store = makeArtifactDataStore();
  }
 
  @After
  public void tearDownArtifactDataStore() throws Exception {
    this.store = null;
  }

  @Test
  public void testAddArtifactData() throws Exception {
    assertThrows(NullPointerException.class, () -> store.addArtifactData(null));
  }
  
  @Test
  public void testCommitArtifactData() throws Exception {
    assertThrows(NullPointerException.class, () -> store.commitArtifactData(null));
  }
  
  @Test
  public void testGetArtifactData() throws Exception {
    assertThrows(NullPointerException.class, () -> store.getArtifactData(null));
  }
  
  @Test
  public void testUpdateArtifactData() throws Exception {
    assertThrows(NullPointerException.class, () -> store.updateArtifactMetadata(null, null));
  }
  
  @Test
  public void testDeleteArtifactData() throws Exception {
    assertThrows(NullPointerException.class, () -> store.deleteArtifactData(null));
  }
  
}