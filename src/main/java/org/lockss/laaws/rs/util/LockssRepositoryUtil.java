package org.lockss.laaws.rs.util;

import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.log.L4JLogger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods for {@link LockssRepository} implementations.
 */
public class LockssRepositoryUtil {
  private final static L4JLogger log = L4JLogger.getLogger();

  private static final Pattern NAMESPACE_PATTERN = Pattern.compile("^[a-zA-Z0-9]([a-zA-Z0-9._-]+)?$");

  /**
   * Validates a namespace.
   *
   * @param namespace A {@link String} containing the namespace to validate.
   * @return A {@code boolean} indicating whether the namespace passed validation.
   */
  public static boolean validateNamespace(String namespace) {
    if (namespace == null) {
      return false;
    }

    Matcher m = NAMESPACE_PATTERN.matcher(namespace);
    return m.matches();
  }
}
