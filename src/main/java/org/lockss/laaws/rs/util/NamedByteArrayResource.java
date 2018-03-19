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

package org.lockss.laaws.rs.util;

import org.springframework.core.io.ByteArrayResource;

/**
 * Extends ByteArrayResource to implement getFilename().
 *
 * This is necessary so that FormHttpMessageConverter#getFilename returns a String instead of null (which is the
 * default return of Resource#getFilename). Then HttpHeaders#setContentDispositionFormData correctly includes
 * "; filename=" in the Content-Disposition header during FormHttpMessageConverted#writePart.
 */
public class NamedByteArrayResource extends ByteArrayResource {
    private String name;

    /**
     * Constructor.
     * @param name
     *          A {@code String} containing the name of this resource.
     * @param byteArray
     *          The {@code byte[]} of this resource.
     */
    public NamedByteArrayResource(String name, byte[] byteArray) {
        super(byteArray);
        this.name = name;
    }

    /**
     * Returns the filename of this resource.
     *
     * @return A {@code String} containing the filename of this resource.
     */
    @Override
    public String getFilename() {
        return this.name;
    }

    /**
     * Returns a description of this resource.
     *
     * @return A {@code String} containing a description of this resource.
     */
    @Override
    public String getDescription() {
        return "Named byte array resource [" + this.getFilename() + ", bytes: " + getByteArray().length + "]";
    }
}
