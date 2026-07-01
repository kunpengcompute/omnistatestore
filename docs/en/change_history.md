# Change History<a name="change_history"></a>

<table>
<thead>
  <tr>
    <th style="width: 30%">Date</th>
    <th style="width: 70%">Description</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>2026-06-30</td>
    <td>This issue is the fourth official release.<br>Released OmniStateStore 1.3.0: <br>By adjusting the compression format of different SST levels in RocksDB and optimizing with soft computing compression algorithms, we can reduce the compression/decompression overhead during state compaction and enhance the end-to-end throughput of the application.</td>
  </tr>
  <tr>
    <td>2026-03-30</td>
    <td>This issue is the third official release.<br>Released OmniStateStore 1.2.0: <br>This version integrates Flink and RocksDB as plugins to improve the performance of Flink stateful test cases. It introduces lightweight modifications to Flink. By leveraging state caching and filtering techniques, it reduces Flink's access to RocksDB and improves I/O performance for stateful workloads. The architecture of version 1.2.0 has been revised and is independent of versions 1.1.0 and 1.0.0.</td>
  </tr>
  <tr>
    <td>2025-12-30</td>
    <td>This issue is the second official release.<br>Released OmniStateStore 1.1.0: <br>It connects to the Flink metric framework to implement common metrics, as well as persistent storage of priority queues and KV separated storage.</td>
  </tr>
  <tr>
    <td>2025-06-30</td>
    <td>This issue is the first official release.<br>Released OmniStateStore 1.0.0: <br>It introduces a new state storage technology to improve the I/O performance of Flink in big data scenarios.</td>
  </tr>
  </tbody>
</table>
