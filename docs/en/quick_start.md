# Quick Start

You can refer to this document to quickly enable the feature and verify the acceleration capabilities of OmniStateStore.

## Usage Description

The acceleration capabilities of OmniStateStore include dynamic filter, Flink semantic state caching, and merge read/write optimizations. These are further composed of the following subfeatures:

- **Flink semantic state caching algorithm**: States with the same key are preferentially aggregated in memory, reducing the frequency of RocksDB accesses.
- **Flink intelligent multi-stream awareness algorithm**: For states that require only point reads and writes, the memTable data structure is replaced with a HashLinkList to improve the efficiency of point operations.
- **Replace RMW with Merge**: Reduces the state update overhead for the Join operator.
- **Dual-stream Join data cache algorithm**: Minimizes the number of range queries on the state in the StreamJoinOperator.
- **Dynamic filter**: Eliminates redundant state query operations.

The OmniStateStore feature has the following constraints:
- **Version compatibility**: This project applies to the Flink 1.6.3 + RocksDB 6.20.3 architecture. Data consistency and feature acceleration are ensured only in the specified version.
- **Intrusive modification**: The OmniStateStore feature includes lightweight modifications to Flink. If you have modified the Flink source code, resolve any conflicts before enabling this feature.
- **Acceleration effect**: The performance improvement from OmniStateStore is directly proportional to the proportion of RocksDB test cases and depends on the test case status. In scenarios where the proportion of RocksDB is low, performance does not deteriorate. Similarly, when ValueState and MapState are not used, performance remains unaffected.

## Environment Setup

Prepare the OmniStateStore **compilation environment** as described in the following table.

**Table 1** OmniStateStore compilation environment
<table>
  <thead>
    <tr>
      <th style="text-align: left;">Item</th>
      <th style="text-align: left;">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">Server</td>
      <td style="text-align: left;">Kunpeng 920 server</td>
    </tr>
    <tr>
      <td style="text-align: left;">OS</td>
      <td style="text-align: left;">openEuler 22.03 LTS SP3</td>
    </tr>
    <tr>
      <td style="text-align: left;">JDK</td>
      <td style="text-align: left;">openJDK 1.8.0_432</td>
    </tr>
    <tr>
      <td style="text-align: left;">Maven</td>
      <td style="text-align: left;">Apache Maven 3.6.3</td>
    </tr>
    <tr>
      <td style="text-align: left;">GCC</td>
      <td style="text-align: left;">10.3.1</td>
    </tr>
  </tbody>
</table>

Set up the OmniStateStore **runtime environment** as described in the following table to quickly verify its acceleration effect on stateful use cases.

**Table 2** OmniStateStore runtime environment
<table>
  <thead>
    <tr>
      <th style="text-align: left;">Item</th>
      <th style="text-align: left;">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">Flink</td>
      <td style="text-align: left;">1.16.3</td>
    </tr>
    <tr>
      <td style="text-align: left;">RocksDB</td>
      <td style="text-align: left;">FRocksDB 6.20.3</td>
    </tr>
    <tr>
      <td style="text-align: left;">Nexmark</td>
      <td style="text-align: left;">0.3</td>
    </tr>
    <tr>
      <td style="text-align: left;">Docker</td>
      <td style="text-align: left;">18.09.0</td>
    </tr>
  </tbody>
</table>

## Procedure

1. Download the [OmniStateStore source code](https://gitcode.com/openeuler/OmniStateStore.git) and select the **falcon** branch.

2. Compile the OmniStateStore source code.
Use [sh build.sh](../../scripts/build.sh) to compile OmniStateStore and generate **BoostKit-omniruntime-omniStateStore-1.3.0.zip** in the root directory of the project. For details about how to install OmniStateStore, see the [Installation Guide](./installation_guide.md).

3. Deploy the environment.
Use Docker to deploy the containerized runtime environment of Flink, including one JobManager container and two TaskManager containers. The container flavor is 8C32GB. The JobManager is allocated 8 GB of memory. Each TaskManager is allocated two task slots and 8 GB of memory.

4. Configure the environment variables.
Deploy Flink and Nexmark of the specified versions in the containers and configure the **JAVA_HOME** and **FLINK_HOME** environment variables. In addition, set **LD_LIBRARY_PATH** as follows:
```shell
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/lib:$JAVA_HOME/jre/lib/aarch64:$JAVA_HOME/jre/lib/aarch64/server:/usr/local/lib
```

5. Install OmniStateStore.
Extract **BoostKit-omniruntime-omniStateStore-1.3.0.zip**, copy **librocksdb.so.6** to the **/usr/local/lib** directory, and copy **flink-alg-falcon.jar** to the **$FLINK_HOME/lib** directory.
Once the copying operation is complete, the installation is finished. No additional configuration is required.

6. Set the OmniStateStore parameters.
Set the OmniStateStore parameters in **$FLINK_HOME/conf/flink-conf.yaml** to enable the acceleration feature and specify the RocksDB state backend. For details, see [flink-conf.yaml](../../conf/flink-conf.yaml).

7. Verify the acceleration effect.
Use the native Flink to run the Nexmark q4 test case and record the single-core throughput of the task. Enable the OmniStateStore feature to run the same test case and check whether the single-core throughput of the task significantly improves compared to native Flink.
You can search for the following keywords in Flink run logs to check whether OmniStateStore has been enabled:

```text
[FALCON] enable miniBatch process for StreaminJoinOperator.
[FALCON] accState is valueState, use HashLinkList as memTable structure.
[FALCON] left-records is map, use range filter.
[FALCON] <accState, VALUE> enable falcon cache.
[FALCON] merge operation is used for left-records.
```
If related log information is matched, OmniStateStore has taken effect and the task performance has improved.
