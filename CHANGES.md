# `laaws-repository-core` Release Notes

## Changes Since 2.14.0 

### Features

*   Added the ability to supply and use Solr credentials in SolrJ calls made by the 
    RestLockssRepository client.

### Fixes

*   Improved error handling and forwarding in data store, repository, and client layers.
*   Major performance and scaling improvements in WARC data store operations that
    previously relied on the artifact state journal.
*   Refactored and simplified artifact lifecycle within WARC data store implementations.
*   Fixed several race conditions e.g., through the introduction of SemaphoreMap
    and artifact version locking support in artifact index implementations.
*   Numerous other small bug fixes and improvements.

## Changes Since 2.0.14.0

*   Switched to a 3-part version numbering scheme.
*   Added support for uncompressed and GZIP compressed WARCs

## 2.0.14.0

### Features

*   ...

### Fixes

*   ...

## 2.0.13.0

### Features

*   Move reference Solr version from 6.6.5 to 7.2.1.
*   Infrastructural work to support upgrading Solr config sets.
*   `Artifact` and `ArtifactData` caching improves performance.
*   Paginating iterators improve performance.

### Fixes

*   Fix incorrect URL enumeration order in `SolrArtifactIndex`.
*   Clean up large deferred temporary files after use.
*   Remove file size limit.
*   Increase URL length limit.
*   Allow fetching uncommitted artifacts.
*   Optionally include uncommitted artifacts in iterators.
*   Bug fixes and improved unit tests.
