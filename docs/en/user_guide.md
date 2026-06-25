# User Guide

Learn how to use the OmniStateStore feature effectively with this document. Ensure that OmniStateStore has been installed following instructions in the [Installation Guide](installation_guide.md).

## Using OmniStateStore

1. Set the related configuration items in the **$FLINK_HOME/conf/flink-conf.yaml** file based on the service usage and operating environment. Note that the modification must be performed on the JobManager and all TaskManagers.

&emsp;&emsp;&emsp;&emsp;The configuration item format is [Configuration item name] + [Colon] + [Space] + [Configuration item value]. For details about how to set the parameters, see [Configuration Items](#Configuration Items). The following is a configuration example:
<div style="margin-left: 50px;">

```text
## Enable the RocksDB state backend.
state.backend: rocksdb
state.backend.rocksdb.localdir: /data/rocksdb

## Set the OmniStateStore parameters.
state.backend.rocksdb.options-factory: com.huawei.falcon.state.RocksDBOptOptionsFactory
state.backend.rocksdb.falcon.use-partition-filter: true
state.backend.rocksdb.falcon.use-range-filter: true
state.backend.rocksdb.falcon.prefix-extractor.length: 13
state.backend.rocksdb.falcon.use-hash-memtable: true
state.backend.rocksdb.falcon.use-opt-join: true
state.backend.rocksdb.falcon.use-state-cache: true
state.backend.rocksdb.falcon.state-cache-sizeLimit: 20000
state.backend.rocksdb.falcon.state-cache-bypass-hitRatio: 0.2
state.backend.rocksdb.falcon.use-merge: true
```
</div>

2. Start the Flink task, verify that the configuration items in the logs are set correctly, and check the logs to confirm that OmniStateStore is enabled. For details, see [Observing the Enabling Status of OmniStateStore](#Observing the Enabling Status of OmniStateStore).

## Configuration Items

**Table 1** OmniStateStore configuration items
<table>
  <thead>
    <tr>
      <th style="text-align: left;">Configuration Item</th>
      <th style="text-align: left;">Example Value</th>
      <th style="text-align: left;">Description</th>
      <th style="text-align: left;">Remarks</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">state.backend</td>
      <td style="text-align: left;">rocksdb</td>
      <td style="text-align: left;">State backend type, either in-memory or RocksDB.</td>
      <td style="text-align: left;">OmniStateStore can be enabled only when the state backend is set to RocksDB.</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.localdir</td>
      <td style="text-align: left;">/data/rocksdb</td>
      <td style="text-align: left;">Drive path for the state backend.</td>
      <td style="text-align: left;">You are advised to set it to the NVMe drive path and ensure that the drive has sufficient storage space.</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.options-factory</td>
      <td style="text-align: left;">com.huawei.falcon.state.RocksDBOptOptionsFactory</td>
      <td style="text-align: left;">Indicates whether to enable dynamic filter. The subfeatures of this technology can be configured separately. The default value is "null".</td>
      <td style="text-align: left;">-</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.use-partition-filter</td>
      <td style="text-align: left;">true</td>
      <td style="text-align: left;">Subfeature 1 of dynamic filter, used to optimize point read/write operations on state. The default value is "false".</td>
      <td style="text-align: left;">-</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.use-hash-memtable</td>
      <td style="text-align: left;">true</td>
      <td style="text-align: left;">Subfeature 2 of dynamic filter, used to optimize ValueState read/write operations. The default value is "false".</td>
      <td style="text-align: left;">-</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.use-range-filter</td>
      <td style="text-align: left;">true</td>
      <td style="text-align: left;">Subfeature 3 of dynamic filter, used to optimize range queries on MapState. The default value is "false".</td>
      <td style="text-align: left;">-</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.prefix-extractor.length</td>
      <td style="text-align: left;">13</td>
      <td style="text-align: left;">Parameter for subfeature 3 of dynamic filter. Its value specifies the storage length of the state prefix filter. A larger value indicates a smaller number of filters that can be stored but higher accuracy of state filtering. The default value is 13. </td>
      <td style="text-align: left;">The recommended maximum value is 21. Avoid increasing it beyond this limit.</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.use-opt-join</td>
      <td style="text-align: left;">true</td>
      <td style="text-align: left;">Indicates whether to optimize StreamingJoinOperator data caching, used to reduce the frequency of MapState range queries in the operator. The default value is "false".</td>
      <td style="text-align: left;">-</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.use-merge</td>
      <td style="text-align: left;">true</td>
      <td style="text-align: left;">Indicates whether to enable merge read/write optimization for StreamingJoinOperator, used to reduce the MapState read/write overhead of the operator. The default value is "false".</td>
      <td style="text-align: left;">-</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.use-state-cache</td>
      <td style="text-align: left;">true</td>
      <td style="text-align: left;">Indicates whether to optimize ValueState caching, used to reduce the RocksDBValueState read and write overhead. The default value is "false".</td>
      <td style="text-align: left;">-</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.state-cache-sizeLimit</td>
      <td style="text-align: left;">20000</td>
      <td style="text-align: left;">Number of ValueState entries that can be cached by a TaskManager. The state cache uses the heap memory of Flink. You need to evaluate the TaskManager memory usage based on the service type and data characteristics. The default value is 12,000. If the KV size is 200 bytes, each TaskManager will consume an additional 2.2 MB of memory.</td>
      <td style="text-align: left;">The recommended maximum value is 20,000. If memory resources are limited, the value can be reduced accordingly.</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.state-cache-bypass-hitRatio</td>
      <td style="text-align: left;">0.2</td>
      <td style="text-align: left;">Threshold for bypassing ValueState caching. If the cache hit ratio falls below this value, state caching is disabled, and read/write operations revert to the native Flink mechanism. The default value is -1, indicating that state caching is never disabled.</td>
      <td style="text-align: left;">The recommended maximum value is 0.5 to ensure that state caching optimization is enabled in most scenarios. The recommended maximum value is 0.05 to prevent state caching when the cache hit ratio is low, avoiding additional performance overhead./td>
    </tr>
  </tbody>
</table>

## Observing the Enabling Status of OmniStateStore

After starting a Flink task, check the Flink logs to verify whether the OmniStateStore feature is enabled. The following table shows how to verify whether each OmniStateStore subfeature is enabled in the corresponding application scenario.

**Table 2** Observing the enabling status of OmniStateStore
<table>
  <thead>
    <tr>
      <th style="text-align: left;">Feature</th>
      <th style="text-align: left;">Application Scenario</th>
      <th style="text-align: left;">How to Observe</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">Dynamic filter–Flink intelligent multi-stream awareness algorithm</td>
      <td style="text-align: left;">For ValueState, the MemTable structure is changed from a SkipList to a HashLinkList to improve point read/write performance.</td>
      <td style="text-align: left;">If "[FALCON] {StateName} is valueState, use HashLinkList as memTable structure" is displayed, the subfeature is enabled.</td>
    </tr>
    <tr>
      <td style="text-align: left;">Dynamic filter–Prefix filtering</td>
      <td style="text-align: left;">For MapState, prefix filtering is used to eliminate redundant drive lookups, improving state range query performance.</td>
      <td style="text-align: left;">If "[FALCON] {StateName} is map, use range filter" is displayed, the subfeature is enabled.</td>
    </tr>
    <tr>
      <td style="text-align: left;">Flink semantic state caching–Join operator data caching</td>
      <td style="text-align: left;">For StreamingJoinOperator, data caching is used to reduce the number of mapState range queries.</td>
      <td style="text-align: left;">If "[FALCON] enable miniBatch process for StreaminJoinOperator" is displayed, the subfeature is enabled.</td>
    </tr>
    <tr>
      <td style="text-align: left;">Flink semantic state caching–ValueState caching</td>
      <td style="text-align: left;">For ValueState, state caching is used to reduce the overhead of point query and point write.</td>
      <td style="text-align: left;">If "[FALCON] <{StateName}, VALUE> enable falcon cache" is displayed, the subfeature is enabled.</td>
    </tr>
    <tr>
      <td style="text-align: left;">Merge read/write optimization</td>
      <td style="text-align: left;">For StreamingJoinOperator, the Merge interface of RocksDB is used to replace the RMW operation.</td>
      <td style="text-align: left;">If "[FALCON] merge operation is used for left-records" is displayed, the subfeature is enabled.</td>
    </tr>
  </tbody>
</table>

## Maintaining the Feature

To upgrade OmniStateStore, install the new version following instructions in the 《[Installation Guide](installation_guide.md/#12-Installing-OmniStateStore)》. You do not need to uninstall the existing version.

To uninstall OmniStateStore, perform operations following instructions in the 《[Installation Guide](installation_guide.md/#13-Uninstalling-OmniStateStore)》 and delete related configuration items from the **$FLINK_HOME/conf/flink-conf.yaml** file.