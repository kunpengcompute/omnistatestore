# Release Notes

View OmniStateStore version information and feature updates.

## Version Mapping

### Product Version

<table>
  <tbody>
    <tr>
      <td style="text-align: left;">Product Name</td>
      <td style="text-align: left;">Kunpeng BoostKit</td>
    </tr>
    <tr>
      <td style="text-align: left;">Product Version</td>
      <td style="text-align: left;">26.1.0</td>
    </tr>
    <tr>
      <td style="text-align: left;">Software Name and Version</td>
      <td style="text-align: left;">OmniStateStore 1.3.0</td>
    </tr>
  </tbody>
</table>

### Software Versions

<table>
  <thead>
    <tr>
      <th style="text-align: left;">Item</th>
      <th style="text-align: left;">Version</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">OS</td>
      <td style="text-align: left;">openEuler 22.03 LTS SP3</td>
    </tr>
    <tr>
      <td style="text-align: left;">GCC</td>
      <td style="text-align: left;">10.3.1</td>
    </tr>
    <tr>
      <td style="text-align: left;">JDK</td>
      <td style="text-align: left;">BiSheng JDK 1.8.0_432</td>
    </tr>
    <tr>
      <td style="text-align: left;">Flink</td>
      <td style="text-align: left;">1.16.3</td>
    </tr>
    <tr>
      <td style="text-align: left;">FRocksDB</td>
      <td style="text-align: left;">6.20.3</td>
    </tr>
  </tbody>
</table>

### Hardware Version

<table>
  <tbody>
    <tr>
      <td style="text-align: left;">Processor</td>
      <td style="text-align: left;">Kunpeng 920</td>
    </tr>
    <tr>
      <td style="text-align: left;">Memory Size</td>
      <td style="text-align: left;">32 GB or above</td>
    </tr>
  </tbody>
</table>

### Virus Scan Results

The software packages, release documents, and product documents have been scanned by multiple antivirus software, and no virus is found.
<table>
  <tbody>
    <tr>
      <td style="text-align: left;">Engine Name</td>
      <td style="text-align: left;">QiAnXin</td>
    </tr>
    <tr>
      <td style="text-align: left;">Engine Version</td>
      <td style="text-align: left;">8.0.5.5260</td>
    </tr>
    <tr>
      <td style="text-align: left;">Virus Lib Version</td>
      <td style="text-align: left;">2026-03-10 08:00:00.0</td>
    </tr>
    <tr>
      <td style="text-align: left;">Scan Time</td>
      <td style="text-align: left;">2026-03-11 22:44:53</td>
    </tr>
    <tr>
      <td style="text-align: left;">Scan Result</td>
      <td style="text-align: left;">OK</td>
    </tr>
  </tbody>
</table>
<table>
  <tbody>
    <tr>
      <td style="text-align: left;">Engine Name</td>
      <td style="text-align: left;">Bitdefender</td>
    </tr>
    <tr>
      <td style="text-align: left;">Engine Version</td>
      <td style="text-align: left;">7.5.1.200224</td>
    </tr>
    <tr>
      <td style="text-align: left;">Virus Lib Version</td>
      <td style="text-align: left;">7.99958</td>
    </tr>
    <tr>
      <td style="text-align: left;">Scan Time</td>
      <td style="text-align: left;">2026-03-11 22:45:17</td>
    </tr>
    <tr>
      <td style="text-align: left;">Scan Result</td>
      <td style="text-align: left;">OK</td>
    </tr>
  </tbody>
</table>
<table>
  <tbody>
    <tr>
      <td style="text-align: left;">Engine Name</td>
      <td style="text-align: left;">Kaspersky</td>
    </tr>
    <tr>
      <td style="text-align: left;">Engine Version</td>
      <td style="text-align: left;">12.0.0.6672</td>
    </tr>
    <tr>
      <td style="text-align: left;">Virus Lib Version</td>
      <td style="text-align: left;">2026-03 10:04:00</td>
    </tr>
    <tr>
      <td style="text-align: left;">Scan Time</td>
      <td style="text-align: left;">2026-03 22:44:59</td>
    </tr>
    <tr>
      <td style="text-align: left;">Scan Result</td>
      <td style="text-align: left;">OK</td>
    </tr>
  </tbody>
</table>

## Version Updates

## V1.3.0

### Change History

This version addresses the high CPU overhead of state compression and decompression in big data scenarios, reducing the cost of state compaction and improving the end-to-end throughput of Flink applications. Version 1.3.0 evolves from version 1.2.0 and has the following feature updates:

### New Features

- **LZ4-based software compression optimization**: Switches the compression format of RocksDB L0/L1 levels to LZ4 and, combined with optimized LZ4-based software algorithms, improves state compression/decompression performance and enhances end-to-end application throughput.

### Modified Features

None

### Removed Features

None

### Resolved Issues

- **State loss in hashMemTable during savepoint operations**: When creating an iterator over the in-memory memTable for a savepoint, set `total_order_seek` to `true` in the read options to prevent state loss during iterator initialization.

- **State loss in the dual-stream join data caching algorithm during checkpointing**: Ensures buffered data is processed before the join operator triggers a checkpoint, preventing data loss after state recovery.

### Known Issues

None

## V1.2.0

### Change Description

The current version aims to address poor I/O performance in big data scenarios by enhancing Flink's efficiency in using RocksDB and overall I/O operations. The architecture of version 1.2.0 has been revised and is independent of versions 1.1.0 and 1.0.0. The new features include:

### New Features

- **Flink semantic state caching algorithm**: States with the same key are preferentially aggregated in memory, reducing the frequency of RocksDB accesses.

- **Flink intelligent multi-stream awareness algorithm**: For states that require only point reads and writes, the MemTable data structure is replaced with a HashLinkedList to improve the efficiency of point operations.

- **Replace RMW with Merge**: Reduces the state update overhead for the Join operator.

- **Dual-stream Join data cache algorithm**: Minimizes the number of range queries on the state in the StreamJoinOperator.

- **Dynamic filter**: Eliminates redundant state query operations.

### Modified Features

None

### Removed Features

Delete some features such as key-value separation and persistent storage of priority queues.

### Resolved Issues

None

### Known Issues

None

## V1.1.0

### Change Description

In the current version, a new state storage technology is introduced to improve the I/O performance of Flink in big data scenarios.

### New Features

- Interconnection with the Flink metric framework to implement some common metrics
- Persistent storage of priority queues
- Key-value separated storage

### Modified Features

None

### Removed Features

None

### Resolved Issues

None

### Known Issues

None

## V1.0.0

### Change Description

In the current version, a new state storage technology is introduced to improve the I/O performance of Flink in big data scenarios.

### New Features

None

### Changed Features

None

### Removed Features

None

### Resolved Issues

None

### Known Issues

None

## 1.3 Related Documentation

### Documentation

<table>
  <thead>
    <tr>
      <th style="text-align: left;">Document</th>
      <th style="text-align: left;">Description</th>
      <th style="text-align: left;">Delivery Method</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">1.3.0 Release Notes</td>
      <td style="text-align: left;">Provides OmniStateStore version update and release information.</td>
      <td style="text-align: left;">Open-source repository</td>
    </tr>
    <tr>
      <td style="text-align: left;">Quick Start</td>
      <td style="text-align: left;">Provides quick start tutorials to help users quickly understand and use OmniStateStore.</td>
      <td style="text-align: left;">Open-source repository</td>
    </tr>
    <tr>
      <td style="text-align: left;">Installation Guide</td>
      <td style="text-align: left;">Provides guidance on how to install and deploy OmniStateStore.</td>
      <td style="text-align: left;">Open-source repository</td>
    </tr>
    <tr>
      <td style="text-align: left;">User Guide</td>
      <td style="text-align: left;">Provides guidance on how to use OmniStateStore.</td>
      <td style="text-align: left;">Open-source repository</td>
    </tr>
    <tr>
      <td style="text-align: left;">FAQs</td>
      <td style="text-align: left;">Records the issues that may occur during the installation, deployment, and use and their solutions.</td>
      <td style="text-align: left;">Open-source repository</td>
    </tr>
    <tr>
      <td style="text-align: left;">Best Practices</td>
      <td style="text-align: left;">Provides practical examples in typical OmniStateStore application scenarios to help users optimize performance and enhance the user experience.</td>
      <td style="text-align: left;">Open-source repository</td>
    </tr>
    <tr>
      <td style="text-align: left;">Design Guide</td>
      <td style="text-align: left;">Describes the system architecture and acceleration mechanisms of OmniStateStore, helping developers understand its design principles.</td>
      <td style="text-align: left;">Open-source repository</td>
    </tr>
  </tbody>
</table>

**Obtaining Documentation**

Visit the [open-source repository](https://atomgit.com/openeuler/OmniStateStore/tree/falcon) to view or download required documents.
