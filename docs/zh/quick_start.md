# 快速入门
<font size=3> 提供OmniStateStore的快速入门指南，用户可以参考本文档快速使能特性并验证OmniStateStore的加速能力。</font>



## 使用说明
<font size=3>
OmniStateStore的加速特性包含动态Filter技术、Flink语义状态缓存和Merge读写优化，具体地，可以拆分为以下子特性：<br>

- 通过**Flink语义状态缓存算法**，同Key状态优先在内存中完成聚合，减少状态对RocksDB的访问频次；<br>
- 通过**Flink智能多流感知算法**，对于仅需要点读、点写的状态，将memTable数据结构替换为HashLinkList, 提升状态点读和点写效率；<br>
- 通过**使用merge替换状态RMW**，减少Join算子的状态更新开销；<br>
- 通过**双流Join数据缓存算法**，减少StreamJoinOperator的状态范围查询次数；<br>
- 通过**动态Filter技术**，过滤冗余状态查询操作；<br>

OmniStateStore特性的使能过程中，存在以下约束：<br>
- **版本兼容性**：本项目适用于Flink 1.16.3 + RocksDB 6.20.3架构，仅在指定版本下保证数据一致性和特性加速效果。
- **侵入式修改**：OmniStateStore包含对Flink的轻量级修改，若用户也修改了Flink源码，需要处理冲突后方可使能特性。
- **加速效果**：OmniStateStore的加速效果与用例RocksDB占比成正比，且和用例状态类型相关。在RocksDB占比较低场景下仅保证性能无劣化，在不使用ValueState和MapState的用例上仅保证性能无劣化。

</font>



## 环境准备
<font size=3>

按照下表准备OmniStateStore的**编译环境**，以支持快速完成OmniStateStore安装。

**表1** OmniStateStore编译环境准备
<table>
  <thead>
    <tr>
      <th style="text-align: left;">准备内容</th>
      <th style="text-align: left;">说明</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">服务器</td>
      <td style="text-align: left;">Kunpeng 920系列服务器 </td>
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

按照下表搭建OmniStateStore的**运行环境**，以支持快速验证OmniStateStore在有状态用例上的加速效果。

**表2** OmniStateStore运行环境准备
<table>
  <thead>
    <tr>
      <th style="text-align: left;">准备内容</th>
      <th style="text-align: left;">说明</th>
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
</font>



## 操作步骤
<font size=3>

1. 下载[OmniStateStore源码](https://gitcode.com/openeuler/OmniStateStore.git)，选择falcon分支。

2. 编译OmniStateStore源码。<br>使用[sh build.sh](../../scripts/build.sh)编译OmniStateStore，在项目根目录生成BoostKit-omniruntime-omniStateStore-1.3.0.zip。OmniStateStore的详细安装流程请参见《[安装指南](./installation_guide.md)》。

3. 部署环境。<br>使用Docker部署Flink的容器化运行环境，包括一个JobManager容器和两个TaskManager容器，容器配置均为8C32GB。其中，JobManager分配8GB内存，单个TaskManager分配2个TaskSlot和8GB内存。

4. 设置环境变量。<br>在容器中部署指定版本的Flink和Nexmark，并配置JAVA_HOME，FLINK_HOME环境变量。此外，将LD_LIBRARY_PATH配置为：<br>
```
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/lib:$JAVA_HOME/jre/lib/aarch64:$JAVA_HOME/jre/lib/aarch64/server:/usr/local/lib
```

5. 安装OmniStateStore。<br>解压BoostKit-omniruntime-omniStateStore-1.3.0.zip，将librocksdb.so.6拷贝到“/usr/local/lib”目录下，将flink-alg-falcon.jar拷贝到“$FLINK_HOME/lib”目录下。<br>拷贝完成后即完成安装，无需额外配置。

6. 配置OmniStateStore参数。<br>参考[flink-conf.yaml](../../conf/flink-conf.yaml)，在“$FLINK_HOME/conf/flink-conf.yaml”中配置OmniStateStore参数以使能加速特性，并指定使用RocksDB状态后端。

7. 验证加速效果。<br>使用原生Flink运行Nexmark q4用例，记录任务的单核吞吐量。随后使能OmniStateStore运行相同用例，观察任务单核吞吐量相比原生Flink是否有明显提升。<br>用户可以在Flink的运行日志中搜索以下关键字，来确认OmniStateStore特性已启用成功。<br>

```
[FALCON] enable miniBatch process for StreaminJoinOperator.
[FALCON] accState is valueState, use HashLinkList as memTable structure.
[FALCON] left-records is map, use range filter.
[FALCON] <accState, VALUE> enable falcon cache.
[FALCON] merge operation is used for left-records.
[FALCON] enable lz4 compression for rocksdb level0 and level1.
```
若成功匹配到相关日志信息，说明OmniStateStore已生效，任务性能已得到优化。
</font>