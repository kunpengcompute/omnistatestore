# 修订记录<a name="change_history"></a>

<table>
<thead>
  <tr>
    <th style="width: 30%">发布日期</th>
    <th style="width: 70%">修订记录</th>
  </tr>
  </thead>
  <tbody>
    <tr>
    <td>2026-06-30</td>
    <td>第四次正式发布。<br>发布OmniStateStore 1.3.0：<br>通过调整RocksDB不同SST层级的压缩格式，结合软算压缩算法优化，降低状态Compaction过程中的压缩/解压缩开销，提升应用端到端吞吐。 </td>
  </tr>
  <tr>
    <td>2026-03-30</td>
    <td>第三次正式发布。<br>发布OmniStateStore 1.2.0：<br>基于对接Flink和RocksDB的插件完成Flink有状态用例性能加速。对Flink进行轻量级修改，基于状态缓存和状态过滤技术，降低Flink对RocksDB的访问频次，提升有状态用例的IO性能。1.2.0版本进行了架构调整，与1.1.0以及1.0.0相互独立。 </td>
  </tr>
  <tr>
    <td>2025-12-30</td>
    <td>第二次正式发布。<br>发布OmniStateStore 1.1.0：<br>新增支持对接Flink Metric框架并实现部分常用的Metric指标；支持Priority Queue持久化存储；支持KV分离存储。</td>
  </tr>
  <tr>
    <td>2025-06-30</td>
    <td>第一次正式发布。<br>发布OmniStateStore 1.0.0：<br>解决了大数据场景下，针对大状态下IO性能较差的问题，实现了一种新型的状态存储方式，提升了Flink的IO性能。</td>
  </tr> 
  </tbody>
</table>
