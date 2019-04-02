/*

Copyright (c) 2000-2019, Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.laaws.rs.io.storage.warc;

import org.junit.jupiter.api.Test;
import org.lockss.util.test.LockssTestCase5;

/**
 * Test class for
 * <code>org.lockss.laaws.rs.io.storage.warc.TempWarcInUseTracker</code>.
 */
public class TestTempWarcInUseTracker extends LockssTestCase5 {

  @Test
  public void test() {
    // Nothing is in use to begin with.
    assertFalse(TempWarcInUseTracker.INSTANCE.isInUse(null));
    assertFalse(TempWarcInUseTracker.INSTANCE.isInUse("one"));

    // Not possible to end a use that did not start.
    try {
      TempWarcInUseTracker.INSTANCE.markUseEnd("one");
      fail("Should have thrown IllegalStateException");
    } catch (IllegalStateException ise) {
      // Expected.
    }

    // Start one use in one file.
    TempWarcInUseTracker.INSTANCE.markUseStart("one");
    assertTrue(TempWarcInUseTracker.INSTANCE.isInUse("one"));

    assertFalse(TempWarcInUseTracker.INSTANCE.isInUse("two"));

    // Start another use in another file.
    TempWarcInUseTracker.INSTANCE.markUseStart("two");
    assertTrue(TempWarcInUseTracker.INSTANCE.isInUse("two"));

    // End the first use.
    TempWarcInUseTracker.INSTANCE.markUseEnd("one");
    assertFalse(TempWarcInUseTracker.INSTANCE.isInUse("one"));
    assertTrue(TempWarcInUseTracker.INSTANCE.isInUse("two"));

    // Not possible to end a use that did not start.
    try {
      TempWarcInUseTracker.INSTANCE.markUseEnd("one");
      fail("Should have thrown IllegalStateException");
    } catch (IllegalStateException ise) {
      // Expected.
    }

    // Start a second use of the same file.
    TempWarcInUseTracker.INSTANCE.markUseStart("two");
    assertTrue(TempWarcInUseTracker.INSTANCE.isInUse("two"));

    // End one use of the same file.
    TempWarcInUseTracker.INSTANCE.markUseEnd("two");
    assertTrue(TempWarcInUseTracker.INSTANCE.isInUse("two"));

    // End the other use of the same file.
    TempWarcInUseTracker.INSTANCE.markUseEnd("two");
    assertFalse(TempWarcInUseTracker.INSTANCE.isInUse("two"));

    // Still not possible to end a use that did not start.
    try {
      TempWarcInUseTracker.INSTANCE.markUseEnd("one");
      fail("Should have thrown IllegalStateException");
    } catch (IllegalStateException ise) {
      // Expected.
    }

    assertFalse(TempWarcInUseTracker.INSTANCE.isInUse("one"));

    // Use the first file again.
    TempWarcInUseTracker.INSTANCE.markUseStart("one");
    assertTrue(TempWarcInUseTracker.INSTANCE.isInUse("one"));
    TempWarcInUseTracker.INSTANCE.markUseEnd("one");
    assertFalse(TempWarcInUseTracker.INSTANCE.isInUse("one"));
  }
}
