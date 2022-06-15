# `laaws-repository-core` Release Notes

## Changes Since 2.16.0

* Support for "bulk storage mode" for migration from class LOCKSS daemons.
* Improvements to `SolrArtifactIndex` (and clients of it) to be more judicious about when and 
    where Solr commits are performed and number of queries performed.
* Numerous internal improvements and bug fixes. E.g., removed the use of `DeferredTempFileOputStream` 
    in the serialization of artifacts to WARC records in `WarcArtifactDataStore`.
* Support for filesystems larger than 8 EiB.

## Changes Since 2.15.0

### Features

* Upgraded Hadoop client libraries to version 3.3.1
* Upgraded Solr client libraries and configuration to version 8.9.0. Merged LOCKSS-specific 
  configuration changes into Solr 8.9.0 configuration.
* `BaseLockssRepository` implementations now have a repository state directory. Its 
    subsystems (e.g., the data store) may organize their state into subdirectories
    of the repository state directory. It is currently used for artifact reindex 
    signaling and state.
* Reindex trigger and logic was implemented in `BaseLockssRepository`. The reindex
    is triggered by the presence of its state file (`.../state/index/reindex`). After
    a successful reindex, the reindex state file is renamed out of the way. The 
    reindex state file is a CSV log of WARCs that were reindexed. It may be useful
    for debugging or auditing the reindex.
* `WarcArtifactDataStore` and its subclasses were refactored so that they channel 
    their access to subsystems (e.g., index) through their BaseLockssRepository context.
* Introduced `LockssRepositorySubsystem` interface for subsystems (currently index
    and data store) of `BaseLockssRepository`.
* Numerous performances improvements to `SolrArtifactIndex` through control over
  when Solr performs soft and hard commits. Introduced `SolrCommitJournal` to record Solr
  updates and replay them if necessary.

### Fixes

* Re-enabled the resumption and processing of artifacts from temporary WARCs upon 
  `WarcArtifactDataStore` startup.
* Adjusted (and removed unnecessary) synchronization through the Repository service to
  improve performance and avoid a deadlock.

## Changes Since 2.0.14.0

### Features

*   Switched to a 3-part version numbering scheme.
*   Added support for uncompressed and GZIP compressed WARCs
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
