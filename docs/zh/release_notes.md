# 版本说明书

提供OmniStateStore的版本信息和特性更新情况。

## 版本配套说明

### 产品版本信息

<table>
  <tbody>
    <tr>
      <td style="text-align: left;">产品名称</td>
      <td style="text-align: left;">Kunpeng BoostKit</td>
    </tr>
    <tr>
      <td style="text-align: left;">产品版本</td>
      <td style="text-align: left;">26.1.RC1</td>
    </tr>
    <tr>
      <td style="text-align: left;">软件名称和版本</td>
      <td style="text-align: left;">OmniStateStore 1.3.0</td>
    </tr>
  </tbody>
</table>

### 软件版本配套说明

<table>
  <thead>
    <tr>
      <th style="text-align: left;">项目</th>
      <th style="text-align: left;">版本</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">操作系统</td>
      <td style="text-align: left;">openEuler 22.03 LTS SP3</td>
    </tr>
    <tr>
      <td style="text-align: left;">GCC</td>
      <td style="text-align: left;">10.3.1</td>
    </tr>
    <tr>
      <td style="text-align: left;">JDK</td>
      <td style="text-align: left;">毕昇JDK 1.8.0_432</td>
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

### 硬件版本配套说明

<table>
  <tbody>
    <tr>
      <td style="text-align: left;">处理器</td>
      <td style="text-align: left;">鲲鹏920系列处理器</td>
    </tr>
    <tr>
      <td style="text-align: left;">内存大小</td>
      <td style="text-align: left;">32GB以上</td>
    </tr>
  </tbody>
</table>

### 病毒扫描结果

本软件包、版本文档、产品文档经过防病毒软件扫描，未发现病毒。详细信息如下：
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

## 版本更新情况说明

## V1.3.0

### 更新说明

当前版本旨在解决大数据场景下，状态压缩/解压缩CPU占比高的问题，降低状态compaction开销，提升Flink应用端到端吞吐。1.3.0版本基于1.2.0版本演进，主要新增特性如下：

### 新增特性

- **LZ4软算压缩优化**：将RocksDB L0/L1层的压缩格式修改为LZ4，结合软算LZ4压缩算法优化，提升状态压缩/解压缩性能，提升应用端到端吞吐。

### 修改特性

无 

### 删除特性

无

### 已解决的问题

- **hashMemTable在savepoint场景丢失状态的问题**：在为savepoint创建内存memTable的迭代器时，将读配置的total_order_seek配置为true，避免创建迭代器时丢失状态。

- **双流Join数据缓存算法在checkpoint场景丢失状态的问题**：在join算子触发checkpoint之前，处理数据缓冲区的数据，避免状态恢复后数据丢失。

### 遗留问题

无

## V1.2.0

### 更新说明

当前版本旨在解决大数据场景下，针对大状态下IO性能较差的问题，优化Flink对RocksDB的使用效率，提升Flink的IO性能。1.2.0版本进行了架构调整，与1.1.0以及1.0.0相互独立，主要新增特性如下：

### 新增特性

- **Flink语义状态缓存算法**：同Key状态优先在内存中完成聚合，减少状态对RocksDB的访问频次。

- **Flink智能多流感知算法**：对于仅需要点读、点写的状态，将memTable数据结构替换为HashLinkList, 提升状态点读和点写效率。

- **使用merge替换状态RMW**：减少Join算子的状态更新开销。

- **双流Join数据缓存算法**：减少StreamJoinOperator的状态范围查询次数。

- **动态Filter技术**：过滤冗余状态查询操作。

### 修改特性

无

### 删除特性

删除KV分离、Priority Queue持久化存储等特性。

### 已解决的问题

无

### 遗留问题

无

## V1.1.0

### 更新说明

当前版本解决了大数据场景下针对大状态下IO性能较差的问题，实现了一种新型的状态存储方式，提升了Flink的IO性能。

### 新增特性

- 支持对接Flink Metric框架，并实现部分常用的Metric指标。
- 支持Priority Queue持久化存储。
- 支持KV分离存储。

### 修改特性

无

### 删除特性

无

### 已解决的问题

无

### 遗留问题

无

## V1.0.0

### 更新说明

当前版本解决了大数据场景下针对大状态下IO性能较差的问题，实现了一种新型的状态存储方式，提升了Flink的IO性能。

### 新增特性

无

### 修改特性

无

### 删除特性

无

### 已解决的问题

无

### 遗留问题

无

## 1.3 版本配套文档

### 版本配套文档

<table>
  <thead>
    <tr>
      <th style="text-align: left;">文档名称</th>
      <th style="text-align: left;">内容简介</th>
      <th style="text-align: left;">交付形式</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">1.3.0 版本说明书</td>
      <td style="text-align: left;">提供OmniStateStore的版本更新内容与发布说明。</td>
      <td style="text-align: left;">开源仓</td>
    </tr>
    <tr>
      <td style="text-align: left;">快速入门</td>
      <td style="text-align: left;">提供OmniStateStore的快速上手教程，帮助用户快速了解和使用该组件。</td>
      <td style="text-align: left;">开源仓</td>
    </tr>
    <tr>
      <td style="text-align: left;">安装指南</td>
      <td style="text-align: left;">提供OmniStateStore的安装部署指导。</td>
      <td style="text-align: left;">开源仓</td>
    </tr>
    <tr>
      <td style="text-align: left;">用户指南</td>
      <td style="text-align: left;">提供OmniStateStore的使用操作指导。</td>
      <td style="text-align: left;">开源仓</td>
    </tr>
    <tr>
      <td style="text-align: left;">常见问题</td>
      <td style="text-align: left;">记录安装、部署和使用过程中可能遇到的问题及其解决方法。</td>
      <td style="text-align: left;">开源仓</td>
    </tr>
    <tr>
      <td style="text-align: left;">最佳实践</td>
      <td style="text-align: left;">提供OmniStateStore典型使用场景下的实践案例，帮助用户优化性能与使用体验。</td>
      <td style="text-align: left;">开源仓</td>
    </tr>
    <tr>
      <td style="text-align: left;">设计指南</td>
      <td style="text-align: left;">提供OmniStateStore的系统架构与加速机制，帮助开发者深入了解其设计原理。</td>
      <td style="text-align: left;">开源仓</td>
    </tr>
  </tbody>
</table>

**获取文档的方法**

您可以通过访问[开源仓](https://atomgit.com/openeuler/OmniStateStore/tree/falcon)浏览和获取相关文档。
