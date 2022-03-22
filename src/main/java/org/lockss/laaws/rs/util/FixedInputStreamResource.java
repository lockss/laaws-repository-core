package org.lockss.laaws.rs.util;

import org.springframework.core.io.InputStreamResource;

import java.io.IOException;
import java.io.InputStream;

public class FixedInputStreamResource extends InputStreamResource {
  protected boolean read = false;

  /**
   * Create a new InputStreamResource.
   *
   * @param inputStream the InputStream to use
   */
  public FixedInputStreamResource(InputStream inputStream) {
    super(inputStream);
  }

  /**
   * Create a new InputStreamResource.
   *
   * @param inputStream the InputStream to use
   * @param description where the InputStream comes from
   */
  public FixedInputStreamResource(InputStream inputStream, String description) {
    super(inputStream, description);
  }

  /**
   * This implementation throws IllegalStateException if attempting to
   * read the underlying stream multiple times.
   */
  @Override
  public InputStream getInputStream() throws IOException, IllegalStateException {
    if (this.read) {
      throw new IllegalStateException("InputStream has already been read - " +
          "do not use InputStreamResource if a stream needs to be read multiple times");
    }
    this.read = true;
    return super.getInputStream();
  }

  /**
   * This implementation always returns {@code true} for a resource
   * that {@link #exists() exists} (revised as of 5.1).
   */
  @Override
  public boolean isReadable() {
    return !read;
  }
}

