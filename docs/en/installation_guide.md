# Installation Guide

## Environment Requirements

Before installing and using OmniStateStore, ensure that the hardware and software environments meet the requirements for installation, deployment, and normal operation.

**Hardware Requirements**

The OmniStateStore software runs in Docker containers. [Table 1 Hardware requirements](#hardware-requirements) describes the hardware requirements.

**Table 1 Hardware requirements**<a id="hardware-requirements"></a>

<table style="undefined;table-layout: fixed; width: 450px"><colgroup>
<col style="width: 173px">
<col style="width: 277px">
</colgroup>
<thead>
  <tr>
    <th>Item</th>
    <th>Requirement</th>
  </tr></thead>
<tbody>
  <tr>
    <td>Processor</td>
    <td> Kunpeng 920</td>
  </tr>
  <tr>
    <td>Memory size</td>
    <td>256 GB or above</td>
  </tr>
  <tr>
    <td>Memory frequency</td>
    <td>4800 MT/s</td>
  </tr>
  <tr>
    <td>NIC</td>
    <td>NA</td>
  </tr>
  <tr>
    <td>Drive (NVMe SSD)</td>
    <td>At least one 3.6 TB or 7.68 TB SSD</td>
  </tr>
</tbody>
</table>

**Software Requirements**

Before installing the OmniStateStore software, check that you have installed all the dependency software. Install these dependencies based on their security standards. [Table 2 Software requirements](#software-requirements) describes the OS and software requirements of each node in the cluster.

**Table 2 Software requirements**<a id="software-requirements"></a>

<table style="undefined;table-layout: fixed; width: 611px"><colgroup>
<col style="width: 165px">
<col style="width: 265px">
<col style="width: 181px">
</colgroup>
<thead>
  <tr>
    <th>Software Name</th>
    <th>Software Version</th>
    <th>How to Obtain</th>
  </tr></thead>
<tbody>
  <tr>
    <td>OS</td>
    <td>openEuler 22.03 LTS SP3</td>
    <td><a href="https://easysoftware.openeuler.openatom.cn/en/field?os=openEuler-22.03-LTS-SP3">Link</a></td>
  </tr>
  <tr>
    <td>Java</td>
    <td>JDK 1.8.0_432</td>
    <td><a href="https://www.oracle.com/apac/java/technologies/downloads/#java8">Link</a></td>
  </tr>
  <tr>
    <td>Flink</td>
    <td><li>1.16.1</li><li>1.16.3</li><li>1.17.1</li><li>1.20.0</li></td>
    <td><a href="https://archive.apache.org/dist/flink/">Link</a></td>
  </tr>
</tbody>
</table>

**Obtaining the software package**

**Table 3 OmniStateStore software list**<a id="omnistatestore-software-list"></a>

<table style="undefined;table-layout: fixed; width: 758px"><colgroup>
<col style="width: 151px">
<col style="width: 220px">
<col style="width: 91px">
<col style="width: 154px">
<col style="width: 142px">
</colgroup>
<thead>
  <tr>
    <th>Name</th>
    <th>Package Name</th>
    <th>Release Type</th>
    <th>Description</th>
    <th>How to Obtain</th>
  </tr></thead>
<tbody>
  <tr>
    <td>OmniStateStore package</td>
    <td>BoostKit-omniruntime-omnistatestore-1.1.0.zip</td>
    <td>Open source</td>
    <td>OmniStateStore software installation package</td>
    <td><a href="https://atomgit.com/openeuler/OmniStateStore/releases">Link</a></td>
  </tr>
</tbody>
</table>

**Verifying Software Package Integrity**

To prevent a software package from being maliciously tampered with during transfer or storage, download also the corresponding digital signature file for integrity verification while obtaining the software package.

1. Obtain the software package from [Table 3 OmniStateStore software list](#omnistatestore-software-list).
2. Obtain the *OpenPGP Signature Verification Guide*.
    - Carrier users: Visit [http://support.huawei.com/carrier/digitalSignatureAction](http://support.huawei.com/carrier/digitalSignatureAction).
    - Enterprise customers: Visit [https://support.huawei.com/enterprise/en/tool/pgp-verify-TL1000000054](https://support.huawei.com/enterprise/en/tool/pgp-verify-TL1000000054).

3. Verify the software package integrity by following instructions in *OpenPGP Signature Verification Guide*.

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >
    >- If the verification fails, do not use the software package, and contact Huawei technical support engineers.
    >- Before using a software package for an installation or upgrade, verify the digital signature to ensure that the software package has not been tampered with.

Before installing and using OmniStateStore, ensure that the hardware and software environments meet the requirements for installation, deployment, and normal operation.

## Installing OmniStateStore<a id="installing-omnistatestore"></a>

1. Obtain the software package `BoostKit-omniruntime-omnistatestore-1.1.0.zip` from [Table 3 OmniStateStore software list](#omnistatestore-software-list).
2. Log in to the installation node and upload the `BoostKit-omniruntime-omnistatestore-1.1.0.zip` software package to the `${FLINK_HOME}/lib/` subdirectory.
3. Extract the software package.

    ```cmd
    unzip BoostKit-omniruntime-omnistatestore-1.1.0.zip
    tar -zxvf BoostKit-omniruntime-omnistatestore-1.1.0.tar.gz
    ```

4. Copy the JAR package of Flink to the `${FLINK_HOME}/lib/` directory.

    The following uses version 1.16.3 as an example.

    ```cmd
    cp BoostKit-omnistatestore_1.1.0/java/jars/flink-boost-statebackend-1.1.0-SNAPSHOT-for-flink-1.16.3.jar ./
    ```

5. To release drive space, run the following command to delete the software package:

    ```cmd
    rm -f BoostKit-omniruntime-omnistatestore-1.1.0.tar.gz
    rm -f BoostKit-omniruntime-omnistatestore-1.1.0.zip
    rm -rf BoostKit-omnistatestore_1.1.0/
    ```

## Starting OmniStateStore

This section describes how to start the OmniStateStore service to enable the acceleration function of Flink state storage.

1. Set the configuration items in the `flink-conf.yaml` file located in the Flink `conf` directory based on the service requirements and deployment environment.

    Configuration item format: _$\{Configuration item name\} + \$\{Colon\} + \$\{Space\} + $\{Configuration item value\}_ [Configuration Item Description](#configuration-item-description) describes the OmniStateStore configuration items. The following describes the configuration items in different scenarios.

    - Add or modify the following configuration items in `${FLINK_HOME}/conf/flink-conf.yaml` to enable OmniStateStore. Update the configuration files of the Job Manager and all Task Managers.

        **Table 1 Configuration items**<a id="configuration-items"></a>

        <table style="undefined;table-layout: fixed; width: 745px"><colgroup>
        <col style="width: 154px">
        <col style="width: 225px">
        <col style="width: 93px">
        <col style="width: 273px">
        </colgroup>
        <thead>
        <tr>
            <th>Configuration Item</th>
            <th>Description</th>
            <th>Example</th>
            <th>Remarks</th>
        </tr></thead>
        <tbody>
        <tr>
            <td>state.backend</td>
            <td>Open-source Flink parameter, which is used to configure the state backend.</td>
            <td>com.huawei.ock.bss.OckDBStateBackendFactory</td>
            <td>This configuration item determines the state backend type. Verify that the value is case-sensitive and correctly spelled.</td>
        </tr>
        <tr>
            <td>state.backend.ockdb.localdir</td>
            <td>Local OmniStateStore state data path.</td>
            <td>/usr/local/flink/ockdb</td>
            <td>Check that the path exists and the Flink run user has the read and write permissions on the path.</td>
        </tr>
        <tr>
            <td>state.backend.ockdb.jni.logfile</td>
            <td>OmniStateStore log path.</td>
            <td>/usr/local/flink/log/kv.log</td>
            <td>You are advised to set this parameter to the Flink log directory.</td>
        </tr>
        </tbody>
        </table>

        A configuration example is as follows:

        ```cmd
        state.backend: com.huawei.ock.bss.OckDBStateBackendFactory
        state.backend.ockdb.localdir: /usr/local/flink/ockdb
        state.backend.ockdb.jni.logfile: /usr/local/flink/log/kv.log
        ```

    - Enable persistent storage of priority queues.

        A configuration example is as follows:

        ```cmd
        state.backend.ockdb.timer-service.factory: OCKDB
        ```

    - Enable KV separated storage.

        A configuration example is as follows:

        ```cmd
        state.backend.ockdb.kv-separate.switch: true
        state.backend.ockdb.kv-separate.threshold: 200
        ```

2. Create the necessary directories.

    In the example, `state.backend.ockdb.localdir` is set to `/usr/local/flink/ockdb` and `state.backend.ockdb.checkpoint.backup` is set to `/usr/local/flink/checkpoint/backup`. Replace the example directories with the actual directories used in your installation.

    ```cmd
    mkdir -p /usr/local/flink/ockdb
    mkdir -p /usr/local/flink/checkpoint/backup
    ```

3. Start a Flink task and view the configuration items in the log to check whether the configuration is successful.
4. Run the `${FLINK_HOME}/examples/streaming/WordCount.jar` demo application.

    If "OmniStateStore service start success" is displayed in the Task Manager logs, OmniStateStore is started successfully.

## Maintaining the Feature

Follow the operating instructions when upgrading or uninstalling OmniStateStore.

**Upgrading the Software**

Replace the existing JAR package with the JAR package of the new version on the installation node. You do not need to uninstall the existing version. For details, see [Installing OmniStateStore](#installing-omnistatestore).

**Uninstalling the Software**

>![](public_sys-resources/icon-notice.gif) **NOTICE**
>Perform the following steps only when you need to uninstall OmniStateStore.

1. Delete the configured `state.backend.ockdb.localdir` path from the installation node.
2. Delete the `flink-boost-statebackend-1.1.0-SNAPSHOT-for-flink-$\{flink._x.x.x_\}.jar` file from the `${FLINK_HOME}/lib/` directory.

    ```cmd
    rm -f flink-boost-statebackend-1.1.0-SNAPSHOT-for-flink-x.x.x.jar
    ```

3. Set the `state.backend` configuration item in the `flink-conf.yaml` configuration file to another state backend.

## References

### Configuration Item Description<a id="configuration-item-description"></a>

The parameter configuration rules for the Log, StateStore, and Metric modules of OmniStateStore cover log management, state storage, and performance monitoring, providing guidance for deploying and tuning OmniStateStore on Flink.

For details about the configuration items of the Log, StateStore, and Metric modules, see [Table 1 Configuration items of the Log module](#configuration-items-of-the-log-module), [Table 2 Configuration items of the StateStore module](#configuration-items-of-the-statestore-module), and [Table 3 Configuration items of the Metric module](#configuration-items-of-the-metric-module).

**Table 1 Configuration items of the Log module**<a id="configuration-items-of-the-log-module"></a>

<table style="undefined;table-layout: fixed; width: 1046px"><colgroup>
<col style="width: 155px">
<col style="width: 181px">
<col style="width: 93px">
<col style="width: 274px">
<col style="width: 343px">
</colgroup>
<thead>
  <tr>
    <th>Configuration Item</th>
    <th>Description</th>
    <th>Default Value</th>
    <th>Valid Value/Range</th>
    <th>Remarks</th>
  </tr></thead>
<tbody>
  <tr>
    <td>state.backend.ockdb.jni.logfile</td>
    <td>Log file path and name.</td>
    <td>/usr/local/flink/log/kv.log</td>
    <td>Files under the path on which the Flink run user has the read and write permissions (The path must exist.) </td>
    <td>Check that the path exists and the Flink run user has the read and write permissions on the path.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.jni.loglevel</td>
    <td>Log level. <li>1: DEBUG</li><li>2: INFO</li><li>3: WARN</li><li>4: ERROR</li></td>
    <td>2</td>
    <td>[1, 4]</td>
    <td>Not specified.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.jni.lognum</td>
    <td>Maximum number of log files.</td>
    <td>20</td>
    <td>[10, 50]</td>
    <td>Not specified.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.jni.logsize</td>
    <td>Size of a single log file. Unit: MB.</td>
    <td>20</td>
    <td>[10, 50]</td>
    <td>Not specified.</td>
  </tr>
</tbody>
</table>

**Table 2 Configuration items of the StateStore module**<a id="configuration-items-of-the-statestore-module"></a>

<table style="undefined;table-layout: fixed; width: 1339px"><colgroup>
<col style="width: 198px">
<col style="width: 232px">
<col style="width: 120px">
<col style="width: 350px">
<col style="width: 439px">
</colgroup>
<thead>
  <tr>
    <th>Configuration Item</th>
    <th>Description</th>
    <th>Default Value</th>
    <th>Valid Value/Range</th>
    <th>Remarks</th>
  </tr></thead>
<tbody>
  <tr>
    <td>state.backend</td>
    <td>Open-source Flink parameter, which is used to configure the state backend.</td>
    <td>None</td>
    <td>com.huawei.ock.bss.OckDBStateBackendFactory</td>
    <td>Ensure that the case-sensitive characters are correctly spelled.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.localdir</td>
    <td>Local OmniStateStore data path.</td>
    <td>None</td>
    <td>An existing path for which the Flink run user has the read and write permissions.</td>
    <td>Check that the path exists and the Flink run user has the read and write permissions on the path. Check that the path and the <code>taskmanager.state.local.root-dirs</code> path are in the same file system.</td>
  </tr>
  <tr>
    <td>taskmanager.state.local.root-dirs</td>
    <td>Open-source Flink parameter, which is used to set the local checkpoint temporary directory.</td>
    <td>None</td>
    <td>An existing path for which the Flink run user has the read and write permissions.</td>
    <td>Recommended to set. If you choose not to set this configuration item, the path specified by <code>io.tmp.dirs</code> is used by default. Check that the path and the <code>state.backend.ockdb.localdir</code> path are in the same file system.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.savepoint.sort.local.dir</td>
    <td>Path for storing temporary sorting files generated during savepoint creation. This parameter is required for using savepoints.</td>
    <td>/usr/local/flink/savepoint/tmp</td>
    <td>An existing path for which the Flink run user has the read and write permissions.</td>
    <td>Check that the path exists and the Flink run user has the read and write permissions on the path.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.jni.slice.watermark.ratio</td>
    <td>The cache layer triggers data eviction by setting the high and low watermark ratio thresholds. Cold data is migrated to the LSM file storage layer based on the preset policy to dynamically balance storage resources.</td>
    <td>0.8</td>
    <td>(0, 1)</td>
    <td>Generally, you do not need to set it separately.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.file.memory.fraction</td>
    <td>Ratio of the memory cache space used for reading and writing data at the LSM layer to the maximum memory of the entire database instance.</td>
    <td>0.2</td>
    <td>[0.1, 0.5]</td>
    <td>Generally, you do not need to set it separately.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.jni.lsmstore.compaction.switch</td>
    <td>Indicates whether to sort and merge data in the LSM file storage layer. The leveled compaction mechanism of the LSM file storage layer controls the sorting and compaction of data files to optimize storage performance and space utilization.</td>
    <td>1</td>
    <td>0: disable; 1: enable</td>
    <td>You are advised to enable it.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.ttl.filter.switch</td>
    <td>Compresses time to live (TTL) expired data in the background.</td>
    <td>false</td>
    <td>false: disable; true: enable</td>
    <td>You are advised to enable this function for TTL State.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.lsmstore.compression.policy</td>
    <td>Compression policy of each level in the LSMStore. It is used with the default value of state.backend.ockdb.lsmstore.compression.level.policy. <code>level0</code>: compression disabled; <code>level1</code>: compression disabled; <code>level2</code>: LZ4 compression enabled; other levels: full compression</td>
    <td>lz4</td>
    <td><code>none</code>: compression disabled; <code>lz4</code>: LZ4 compression enabled</td>
    <td>If the checkpoint file to be uploaded is too large, you are advised to enable this function.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.lsmstore.compression.level.policy</td>
    <td>Configures the LSM file compression policy for different levels. The default value is <code>none,none,lz4</code>, which indicates that compression is disabled at level 0 and level 1 and LZ4 compression is enabled at level 2.</td>
    <td>none,none,lz4</td>
    <td><code>none</code>: compression disabled; <code>lz4</code>: LZ4 compression enabled</td>
    <td>When checkpoints become a bottleneck, you can advance the compression policy to a lower level. The default level range is [0, 5]. <code>level0</code> indicates foreground write compression. <code>None</code> is recommended. Other levels indicate background compression.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.lazy.download.switch</td>
    <td>Indicates whether to enable lazy loading during recovery from a checkpoint.</td>
    <td>false</td>
    <td><code>false</code>: disable; <code>true</code>: enable</td>
    <td>When the checkpoint size is large, enable this option to shorten the time required for restoring a task to the running state.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.bloom.filter.switch</td>
    <td>Enables or disables the Bloom filter for status keys.</td>
    <td>true</td>
    <td><code>false</code>: disable; <code>true</code>: enable</td>
    <td>You are advised to enable this function in scenarios where a large number of invalid key accesses. When this function is enabled, the memory usage increases by dozens of megabytes.</td>
  </tr>
  <tr>
    <td>state.backend.bloom.filter.expected.key.count</td>
    <td>Order of magnitude of keys to be filtered by the Bloom filter in a single state.</td>
    <td>8000000</td>
    <td>[1000000, 10000000]</td>
    <td>Generally, you do not need to set it separately. A larger value indicates that the Bloom filter occupies more memory.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.cache.filter.and.index.switch</td>
    <td>Enables or disables the use of the least recently used (LRU) cache for filter and index blocks at the LSM layer.</td>
    <td>true</td>
    <td><code>false</code>: disable; <code>true</code>: enable</td>
    <td>Generally, you do not need to set it separately. If there are a large number of files and different files are frequently read, you are advised to enable this function.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.cache.filter.and.index.ratio</td>
    <td>Ratio of the memory occupied by the filter and index blocks to the total memory. This memory is not subject to LRU-based eviction.</td>
    <td>0</td>
    <td>(0, 1)</td>
    <td>Generally, you do not need to set it separately. You are advised to enable this function when the filter and index blocks are frequently released in the cache due to heavy pressure.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.checkpoint.backup</td>
    <td>Directory for storing local checkpoint backup slice files when local restoration is enabled.</td>
    <td>None</td>
    <td>Files in the path on which the Flink run user has the read and write permissions (The path must exist.)</td>
    <td>Set this parameter when local restoration is enabled. Check that the path exists and the Flink run user has the read and write permissions on the path.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.timer-service.factory</td>
    <td>Location where the Flink timer is stored.</td>
    <td>OCKDB</td>
    <td><code>OCKDB</code>: persistently stored in the state backend; <code>HEAP</code>: stored in the JVM heap memory</td>
    <td>When the number of timers is small, the heap-based timer may have better performance.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.kv-separate.switch</td>
    <td>Enables or disables KV separation.</td>
    <td>false</td>
    <td>false: disable; true: enable</td>
    <td>Enable KV separation if this value is large.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.kv-separate.threshold</td>
    <td>Threshold for enabling KV separation. Enable KV separation when this value is exceeded.</td>
    <td>200</td>
    <td>(8, 4294967295)</td>
    <td>Values greater than this threshold are stored separately after KV separation.</td>
  </tr>
</tbody></table>

**Table 3 Configuration items of the Metric module**<a id="configuration-items-of-the-metric-module"></a>

<table style="undefined;table-layout: fixed; width: 1329px"><colgroup>
<col style="width: 196px">
<col style="width: 231px">
<col style="width: 119px">
<col style="width: 347px">
<col style="width: 436px">
</colgroup>
<thead>
  <tr>
    <th>Configuration Item</th>
    <th>Description</th>
    <th>Default Value</th>
    <th>Valid Value/Range</th>
    <th>Remarks</th>
  </tr></thead>
<tbody>
  <tr>
    <td>state.backend.ockdb.metric.enable</td>
    <td>Enables or disables the overall metric function. OmniStateStore collects metric information after this function is enabled.</td>
    <td>false</td>
    <td><code>false</code>: disabled<br><code>true</code>: enabled</td>
    <td>The metric function of each module takes effect only after this option is enabled.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.metric.memory</td>
    <td>Enables or disables the metric function of the MemoryManager module.</td>
    <td>false</td>
    <td><code>false</code>: disabled<br><code>true</code>: enabled</td>
    <td>Not specified.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.metric.fresh.table</td>
    <td>Enables or disables the metric function of the FreshTable module.</td>
    <td>false</td>
    <td><code>false</code>: disabled<br><code>true</code>: enabled</td>
    <td>Not specified.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.metric.slice.table</td>
    <td>Enables or disables the metric function of the SliceTable module.</td>
    <td>false</td>
    <td><code>false</code>: disabled<br><code>true</code>: enabled</td>
    <td>Not specified.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.metric.lsm.store</td>
    <td>Enables or disables the metric function of the LSM Store module.</td>
    <td>false</td>
    <td><code>false</code>: disabled<br><code>true</code>: enabled</td>
    <td>Not specified.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.metric.lsm.cache</td>
    <td>Enables or disables the metric function of the LSM Cache module.</td>
    <td>false</td>
    <td><code>false</code>: disabled<br><code>true</code>: enabled</td>
    <td>Not specified.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.metric.snapshot</td>
    <td>Enables or disables the metric function of the Snapshot module.</td>
    <td>false</td>
    <td><code>false</code>: disabled<br><code>true</code>: enabled</td>
    <td>Not specified.</td>
  </tr>
  <tr>
    <td>state.backend.ockdb.metric.restore</td>
    <td>Enables or disables the metric function of the Restore module.</td>
    <td>false</td>
    <td><code>false</code>: disabled<br><code>true</code>: enabled</td>
    <td>Not specified.</td>
  </tr>
</tbody></table>

The parameter configuration rules for the Log, StateStore, and Metric modules of OmniStateStore cover log management, state storage, and performance monitoring, providing guidance for deploying and tuning OmniStateStore on Flink.

### Metrics

OmniStateStore can connect to the Flink Metric framework to provide metrics for monitoring its internal status, such as memory usage and cache hit ratio, during task execution. These metrics serve as a reference for performance tuning and operational analysis in Flink scenarios.

You can add and view these metrics on the **Metric** page during task execution on the Flink WebUI to learn and analyze the running performance of OmniStateStore in real time.

>![](public_sys-resources/icon-notice.gif) **NOTICE**
>
>- Collecting metric data introduces additional performance overhead, which may affect task execution performance. It is recommended to enable the metric feature only during task testing or for performance-insensitive tasks.
>- The unit of all data volume metrics is byte, and the unit of all time metrics is second.

**MemoryManager Module**

**Table 1 Metric reference**<a id="Metric reference"></a>

<table style="undefined;table-layout: fixed; width: 559px"><colgroup>
<col style="width: 286px">
<col style="width: 273px">
</colgroup>
<thead>
  <tr>
    <th>Metric</th>
    <th>Description</th>
  </tr></thead>
<tbody>
  <tr>
    <td>ockdb_memory_used_fresh</td>
    <td>Memory usage of the FreshTable type</td>
  </tr>
  <tr>
    <td>ockdb_memory_used_slice</td>
    <td>Memory usage of the SliceTable type</td>
  </tr>
  <tr>
    <td>ockdb_memory_used_file</td>
    <td>Memory usage of the LSMStore type</td>
  </tr>
  <tr>
    <td>ockdb_memory_used_snapshot</td>
    <td>Memory usage of the Snapshot type</td>
  </tr>
  <tr>
    <td>ockdb_memory_used_borrow_heap</td>
    <td>Memory usage of the BorrowHeap type</td>
  </tr>
  <tr>
    <td>ockdb_memory_used_db</td>
    <td>Total managed memory of a single TaskSlot</td>
  </tr>
  <tr>
    <td>ockdb_memory_max_fresh</td>
    <td>Total allocated memory of the FreshTable type</td>
  </tr>
  <tr>
    <td>ockdb_memory_max_slice</td>
    <td>Total allocated memory of the SliceTable type</td>
  </tr>
  <tr>
    <td>ockdb_memory_max_file</td>
    <td>Total allocated memory of the LSMStore type</td>
  </tr>
  <tr>
    <td>ockdb_memory_max_snapshot</td>
    <td>Total allocated memory of the Snapshot type</td>
  </tr>
  <tr>
    <td>ockdb_memory_max_borrow_heap</td>
    <td>Total allocated memory of the BorrowHeap type</td>
  </tr>
  <tr>
    <td>ockdb_memory_max_db</td>
    <td>Total allocated memory of a single TaskSlot</td>
  </tr>
</tbody></table>

**FreshTable Module**

**Table 2 Metric reference**<a id="metric-reference-1"></a>

<table style="undefined;table-layout: fixed; width: 578px"><colgroup>
<col style="width: 296px">
<col style="width: 282px">
</colgroup>
<thead>
  <tr>
    <th>Metric</th>
    <th>Description</th>
  </tr></thead>
<tbody>
  <tr>
    <td>ockdb_fresh_hit_count</td>
    <td>FreshTable hit count</td>
  </tr>
  <tr>
    <td>ockdb_fresh_miss_count</td>
    <td>FreshTable miss count</td>
  </tr>
  <tr>
    <td>ockdb_fresh_record_count</td>
    <td>FreshTable access count</td>
  </tr>
  <tr>
    <td>ockdb_fresh_flushing_record_count</td>
    <td>Number of KV records being evicted from FreshTable</td>
  </tr>
  <tr>
    <td>ockdb_fresh_flushing_segment_count</td>
    <td>Number of segments being evicted from FreshTable</td>
  </tr>
  <tr>
    <td>ockdb_fresh_flushed_record_count</td>
    <td>Number of KV records evicted from FreshTable</td>
  </tr>
  <tr>
    <td>ockdb_fresh_flushed_segment_count</td>
    <td>Number of segments evicted from FreshTable</td>
  </tr>
  <tr>
    <td>ockdb_fresh_segment_create_fail_count</td>
    <td>Number of failures due to insufficient memory when creating FreshTable segments</td>
  </tr>
  <tr>
    <td>ockdb_fresh_flush_count</td>
    <td>Total number of times FreshTable data is evicted to SliceTable</td>
  </tr>
  <tr>
    <td>ockdb_fresh_binary_key_size</td>
    <td>Total size of all keys in FreshTable</td>
  </tr>
  <tr>
    <td>ockdb_fresh_binary_value_size</td>
    <td>Total size of all values in FreshTable</td>
  </tr>
  <tr>
    <td>ockdb_fresh_binary_map_node_size</td>
    <td>Total size of all MapNodes in FreshTable</td>
  </tr>
  <tr>
    <td>ockdb_fresh_wasted_size</td>
    <td>Total size of free segment space when segments in the FreshTable are evicted to the SliceTable</td>
  </tr>
</tbody></table>

**SliceTable Module**

**Table 3 Metric reference**<a id="metric-reference-2"></a>

<table style="undefined;table-layout: fixed; width: 578px"><colgroup>
<col style="width: 296px">
<col style="width: 282px">
</colgroup>
<thead>
  <tr>
    <th>Metric</th>
    <th>Description</th>
  </tr></thead>
<tbody>
  <tr>
    <td>ockdb_slice_hit_count</td>
    <td>SliceTable hit count</td>
  </tr>
  <tr>
    <td>ockdb_slice_miss_count</td>
    <td>SliceTable miss count</td>
  </tr>
  <tr>
    <td>ockdb_slice_read_count</td>
    <td>SliceTable access count</td>
  </tr>
  <tr>
    <td>ockdb_slice_read_avg_size</td>
    <td>Average traversal length of Slice chains in SliceTable per request</td>
  </tr>
  <tr>
    <td>ockdb_slice_evict_waiting_count</td>
    <td>Number of slices to be evicted</td>
  </tr>
  <tr>
    <td>ockdb_slice_compaction_count</td>
    <td>Number of completed compaction tasks in SliceTable</td>
  </tr>
  <tr>
    <td>ockdb_slice_compaction_slice_count</td>
    <td>Total number of slices compacted in SliceTable</td>
  </tr>
  <tr>
    <td>ockdb_slice_compaction_avg_slice_count</td>
    <td>Average number of slices processed per compaction task in SliceTable</td>
  </tr>
  <tr>
    <td>ockdb_slice_chain_avg_size</td>
    <td>Average slice chain length</td>
  </tr>
  <tr>
    <td>ockdb_slice_avg_size</td>
    <td>Average size of a single slice</td>
  </tr>
</tbody>
</table>

**FileCache Module**

**Table 4 Metric reference**<a id="metric-reference-3"></a>

<table style="undefined;table-layout: fixed; width: 789px"><colgroup>
<col style="width: 404px">
<col style="width: 385px">
</colgroup>
<thead>
  <tr>
    <th>Metric</th>
    <th>Description</th>
  </tr></thead>
<tbody>
  <tr>
    <td>ockdb_index_block_hit_count</td>
    <td>Number of IndexBlock cache hits in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_index_block_hit_size</td>
    <td>Data volume of IndexBlock cache hits in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_index_block_miss_count</td>
    <td>Number of IndexBlock cache misses in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_index_block_miss_size</td>
    <td>Data volume of IndexBlock cache misses in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_index_block_cache_count</td>
    <td>Number of IndexBlock entries cached in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_index_block_cache_size</td>
    <td>Total size of IndexBlock cached in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_data_block_hit_count</td>
    <td>Number of DataBlock cache hits in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_data_block_hit_size</td>
    <td>Data volume of DataBlock cache hits in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_data_block_miss_count</td>
    <td>Number of DataBlock cache misses in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_data_block_miss_size</td>
    <td>Data volume of DataBlock cache misses in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_data_block_cache_count</td>
    <td>Number of DataBlock entries cached in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_data_block_cache_size</td>
    <td>Total size of DataBlock cached in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_filter_hit_count</td>
    <td>Number of FilterBlock cache hits in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_filter_hit_size</td>
    <td>Data volume of FilterBlock cache hits in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_filter_miss_count</td>
    <td>Number of FilterBlock cache misses in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_filter_miss_size</td>
    <td>Data volume of FilterBlock cache misses in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_filter_cache_count</td>
    <td>Number of FilterBlock entries cached in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_filter_cache_size</td>
    <td>Total size of FilterBlock cached in BlockCache</td>
  </tr>
  <tr>
    <td>ockdb_filter_success_count</td>
    <td>Number of times FilterBlock indicates that a key does not exist</td>
  </tr>
  <tr>
    <td>ockdb_filter_exist_success_count</td>
    <td>Number of times FilterBlock indicates that a key exists and the key is actually present</td>
  </tr>
  <tr>
    <td>ockdb_filter_exist_fail_count</td>
    <td>Number of times FilterBlock indicates that a key exists and the key is not actually present</td>
  </tr>
</tbody></table>

**FileStore Module**

**Table 5 Metric reference**<a id="metric-reference-4"></a>

<table style="undefined;table-layout: fixed; width: 904px"><colgroup>
<col style="width: 463px">
<col style="width: 441px">
</colgroup>
<thead>
  <tr>
    <th>Metric</th>
    <th>Description</th>
  </tr></thead>
<tbody>
  <tr>
    <td>ockdb_lsm_flush_count</td>
    <td>Total number of files flushed to drives by the LSMStore module</td>
  </tr>
  <tr>
    <td>ockdb_lsm_flush_size</td>
    <td>Total data size of files flushed to drives by the LSMStore module</td>
  </tr>
  <tr>
    <td>ockdb_lsm_compaction_count</td>
    <td>Total number of compaction tasks completed by the LSMStore module</td>
  </tr>
  <tr>
    <td>ockdb_lsm_hit_count</td>
    <td>LSMStore hit count</td>
  </tr>
  <tr>
    <td>ockdb_lsm_miss_count</td>
    <td>LSMStore miss count</td>
  </tr>
  <tr>
    <td>ockdb_level0_hit_count</td>
    <td>Number of Level 0 file access hits in LSMStore</td>
  </tr>
  <tr>
    <td>ockdb_level0_miss_count</td>
    <td>Number of Level 0 file access misses in LSMStore</td>
  </tr>
  <tr>
    <td>ockdb_level1_hit_count</td>
    <td>Number of Level 1 file access hits in LSMStore</td>
  </tr>
  <tr>
    <td>ockdb_level1_miss_count</td>
    <td>Number of Level 1 file access misses in LSMStore</td>
  </tr>
  <tr>
    <td>ockdb_level2_hit_count</td>
    <td>Number of Level 2 file access hits in LSMStore</td>
  </tr>
  <tr>
    <td>ockdb_level2_miss_count</td>
    <td>Number of Level 2 file access misses in LSMStore</td>
  </tr>
  <tr>
    <td>ockdb_above_level2_hit_count</td>
    <td>Number of LSMStore Level 3 and above file hits</td>
  </tr>
  <tr>
    <td>ockdb_above_level2_miss_count</td>
    <td>Number of LSMStore Level 3 and above file misses</td>
  </tr>
  <tr>
    <td>ockdb_level0_file_size</td>
    <td>Total data volume of LSMStore Level 0 files</td>
  </tr>
  <tr>
    <td>ockdb_level1_file_size</td>
    <td>Total data volume of LSMStore Level 1 files</td>
  </tr>
  <tr>
    <td>ockdb_level2_file_size</td>
    <td>Total data volume of LSMStore Level 2 files</td>
  </tr>
  <tr>
    <td>ockdb_level3_file_size</td>
    <td>Total data volume of LSMStore Level 3 files</td>
  </tr>
  <tr>
    <td>ockdb_above_level3_file_size</td>
    <td>Total data volume of LSMStore Level 4 and above files</td>
  </tr>
  <tr>
    <td>ockdb_lsm_file_size</td>
    <td>Total data volume of LSMStore files at all levels</td>
  </tr>
  <tr>
    <td>ockdb_lsm_compaction_read_size</td>
    <td>Total size of files read during LSMStore compaction tasks</td>
  </tr>
  <tr>
    <td>ockdb_lsm_compaction_write_size</td>
    <td>Total size of files written during LSMStore compaction tasks</td>
  </tr>
  <tr>
    <td>ockdb_level0_compaction_rate</td>
    <td>Compaction rate of LSMStore Level 0 files</td>
  </tr>
  <tr>
    <td>ockdb_level1_compaction_rate</td>
    <td>Compaction rate of LSMStore Level 1 files</td>
  </tr>
  <tr>
    <td>ockdb_level2_compaction_rate</td>
    <td>Compaction rate of LSMStore Level 2 files</td>
  </tr>
  <tr>
    <td>ockdb_level3_compaction_rate</td>
    <td>Compaction rate of LSMStore Level 3 files</td>
  </tr>
  <tr>
    <td>ockdb_lsm_compaction_rate</td>
    <td>Compaction rate of LSMStore files at all levels</td>
  </tr>
  <tr>
    <td>ockdb_lsm_file_count</td>
    <td>Total number of LSMStore files at all levels</td>
  </tr>
</tbody></table>

**Snapshot Module**

**Table 6 Metric reference**<a id="metric-reference-5"></a>

<table style="undefined;table-layout: fixed; width: 1016px"><colgroup>
<col style="width: 520px">
<col style="width: 496px">
</colgroup>
<thead>
  <tr>
    <th>Metric</th>
    <th>Description</th>
  </tr></thead>
<tbody>
  <tr>
    <td>ockdb_snapshot_total_time</td>
    <td>Total execution time of the most recent snapshot task</td>
  </tr>
  <tr>
    <td>ockdb_snapshot_upload_time</td>
    <td>Time spent uploading data during the most recent snapshot task</td>
  </tr>
  <tr>
    <td>ockdb_snapshot_file_count</td>
    <td>Number of files created during the most recent snapshot task</td>
  </tr>
  <tr>
    <td>ockdb_snapshot_file_size</td>
    <td>Size of files created during the most recent snapshot task</td>
  </tr>
  <tr>
    <td>ockdb_snapshot_incremental_size</td>
    <td>Size of incremental files created during the most recent snapshot task</td>
  </tr>
  <tr>
    <td>ockdb_snapshot_slice_file_count</td>
    <td>Number of SliceTable snapshot files created during the most recent snapshot task</td>
  </tr>
  <tr>
    <td>ockdb_snapshot_slice_incremental_file_size</td>
    <td>Size of incremental SliceTable files created during the most recent snapshot task</td>
  </tr>
  <tr>
    <td>ockdb_snapshot_slice_file_size</td>
    <td>Size of SliceTable snapshot files created during the most recent snapshot task</td>
  </tr>
  <tr>
    <td>ockdb_snapshot_sst_file_count</td>
    <td>Number of LSMStore snapshot files created during the most recent snapshot task</td>
  </tr>
  <tr>
    <td>ockdb_snapshot_sst_incremental_file_size</td>
    <td>Size of incremental LSMStore files created during the most recent snapshot task</td>
  </tr>
  <tr>
    <td>ockdb_snapshot_sst_file_size</td>
    <td>Size of LSMStore snapshot files created during the most recent snapshot task</td>
  </tr>
</tbody></table>

**Restore Module**

**Table 7 Metric reference**<a id="metric-reference-6"></a>

<table style="undefined;table-layout: fixed; width: 689px"><colgroup>
<col style="width: 368px">
<col style="width: 321px">
</colgroup>
<thead>
  <tr>
    <th>Metric</th>
    <th>Description</th>
  </tr></thead>
<tbody>
  <tr>
    <td>ockdb_restore_total_time</td>
    <td>Total time spent on the most recent snapshot restore task</td>
  </tr>
  <tr>
    <td>ockdb_restore_download_time</td>
    <td>Time spent downloading data during the most recent snapshot restore task</td>
  </tr>
  <tr>
    <td>ockdb_restore_lazy_download_time</td>
    <td>Time spent on lazy loading during the most recent snapshot restore task</td>
  </tr>
</tbody>
</table>

OmniStateStore can connect to the Flink Metric framework to provide metrics for monitoring its internal status, such as memory usage and cache hit ratio, during task execution. These metrics serve as a reference for performance tuning and operational analysis in Flink scenarios.

### Function Specifications

When used as a Flink state backend, OmniStateStore and RocksDB both support core functions such as basic state read/write, checkpoints, and savepoints, providing a reference for evaluating the feasibility of replacing RocksDB with OmniStateStore.

For details about the comparison between the RocksDB state backend used in the open-source Flink and OmniStateStore, see [Table 1 State backend function comparison](#state-backend-function-comparison).

**Table 1 State backend function comparison**<a id="state-backend-function-comparison"></a>

<table style="undefined;table-layout: fixed; width: 808px"><colgroup>
<col style="width: 184px">
<col style="width: 215px">
<col style="width: 192px">
<col style="width: 217px">
</colgroup>
<thead>
  <tr>
    <th>Category</th>
    <th>Function</th>
    <th>RocksDB StateBackend</th>
    <th>OmniStateStore StateBackend</th>
  </tr></thead>
<tbody>
  <tr>
    <td>Basic state read/write APIs</td>
    <td>Operator State</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Basic state read/write APIs</td>
    <td>Broadcast State</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Basic state read/write APIs</td>
    <td>Value State</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Basic state read/write APIs</td>
    <td>List State</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Basic state read/write APIs</td>
    <td>Map State</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Basic state read/write APIs</td>
    <td>Reducing State</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Basic state read/write APIs</td>
    <td>Aggregating State</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Basic state read/write APIs</td>
    <td>State validity period (TTL)</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Basic state read/write APIs</td>
    <td>Timer</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Checkpoint</td>
    <td>Full checkpoint</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Checkpoint</td>
    <td>Incremental checkpoint</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Checkpoint</td>
    <td>Aligned checkpoint</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Checkpoint</td>
    <td>Unaligned checkpoint</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Checkpoint</td>
    <td>Standard checkpoint restore</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Checkpoint</td>
    <td>Checkpoint restore during parallelism scaling</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Savepoint</td>
    <td>Savepoint during non-stop job execution</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Savepoint</td>
    <td>Savepoint during job termination</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Savepoint</td>
    <td>Savepoint in standard format</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Savepoint</td>
    <td>Savepoint in native format</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Savepoint</td>
    <td>Savepoint deletion</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Savepoint</td>
    <td>Standard savepoint restore</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Savepoint</td>
    <td>Savepoint restore during parallelism scaling</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>Savepoint</td>
    <td>Savepoint state schema evolution support</td>
    <td>Supported</td>
    <td>Supported</td>
  </tr>
</tbody></table>

When used as a Flink state backend, OmniStateStore and RocksDB both support core functions such as basic state read/write, checkpoints, and savepoints, providing a reference for evaluating the feasibility of replacing RocksDB with OmniStateStore.
