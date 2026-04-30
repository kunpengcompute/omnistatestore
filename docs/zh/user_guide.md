# 用户指南
<font size=3>提供OmniStateStore的详细使用说明与操作指导，用户可以参阅该文档启动OmniStateStore加速功能。请确保已按照[安装指南](installation_guide.md)完成了OmniStateStore安装。</font>



## 使用OmniStateStore
<font size=3>

**步骤1**&emsp;根据业务使用情况和运行环境，设置$FLINK_HOME/conf/flink-conf.yaml文件中的相关配置项。请注意，需要在JobManager和所有TaskManager中同步进行修改。<br>

&emsp;&emsp;&emsp;&emsp;配置项格式为[配置项名称] + [英文冒号] + [空格] + [配置项值]，参数配置方法请参阅[配置项说明](#配置项说明)，配置样例如下：<br>
<div style="margin-left: 50px;">

```
## 配置使用Rocksdb状态后端
state.backend: rocksdb
state.backend.rocksdb.localdir: /data/rocksdb

## OmniStateStore配置参数
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

**步骤2**&emsp;启动Flink任务，查看日志中配置项是否正确配置，并在Flink日志中观测是否成功使能OmniStateStore。详细观测方式请参阅[OmniStateStore特性使能情况观测方式](#omnistatestore特性使能情况观测方式)。

</font>



## 配置项说明
<font size=3>

**表1** OmniStateStore配置项说明
<table>
  <thead>
    <tr>
      <th style="text-align: left;">配置项名称</th>
      <th style="text-align: left;">配置示例</th>
      <th style="text-align: left;">配置描述</th>
      <th style="text-align: left;">注意事项</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">state.backend</td>
      <td style="text-align: left;">rocksdb</td>
      <td style="text-align: left;">状态后端类型，可以选择内存状态后端或RocksDB状态后端。</td>
      <td style="text-align: left;">状态后端配置为RocksDB才可以使能OmniStateStore。</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.localdir</td>
      <td style="text-align: left;">/data/rocksdb</td>
      <td style="text-align: left;">状态存储的磁盘路径。</td>
      <td style="text-align: left;">建议配置为NVME磁盘路径，保证磁盘有足够的存储空间。</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.options-factory</td>
      <td style="text-align: left;">com.huawei.falcon.state.RocksDBOptOptionsFactory</td>
      <td style="text-align: left;">配置该参数表示打开动态Filter技术总开关，该技术的子特性支持通过参数单独配置，默认值为null。</td>
      <td style="text-align: left;">/</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.use-partition-filter</td>
      <td style="text-align: left;">true</td>
      <td style="text-align: left;">动态filter技术子特性1，用于优化状态点读点写操作，默认值为false。</td>
      <td style="text-align: left;">/</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.use-hash-memtable</td>
      <td style="text-align: left;">true</td>
      <td style="text-align: left;">动态filter技术子特性2，用于优化ValueState读写操作，默认值为false。</td>
      <td style="text-align: left;">/</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.use-range-filter</td>
      <td style="text-align: left;">true</td>
      <td style="text-align: left;">动态filter技术子特性3，用于优化MapState范围查询操作，默认值为false。</td>
      <td style="text-align: left;">/</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.prefix-extractor.length</td>
      <td style="text-align: left;">13</td>
      <td style="text-align: left;">动态filter技术子特性3配置参数，该值表示状态前缀filter的存储长度。该值配置的越大，可存储的filter数量越少，但状态过滤的准确度越高。默认值为13。 </td>
      <td style="text-align: left;">该值最大建议配置为21，不建议过度增大该配置项。</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.use-opt-join</td>
      <td style="text-align: left;">true</td>
      <td style="text-align: left;">StreamingJoinOperator数据缓存优化技术开关，用于减少该算子的mapState范围查询频次，默认值为false。</td>
      <td style="text-align: left;">/</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.use-merge</td>
      <td style="text-align: left;">true</td>
      <td style="text-align: left;">StreamingJoinOperator的merge读写优化开关，用于减少该算子的mapState读写开销，默认值为false。</td>
      <td style="text-align: left;">/</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.use-state-cache</td>
      <td style="text-align: left;">true</td>
      <td style="text-align: left;">ValueState状态缓存优化开关，用于RocksDBValueState读写开销，默认值为false。</td>
      <td style="text-align: left;">/</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.state-cache-sizeLimit</td>
      <td style="text-align: left;">20000</td>
      <td style="text-align: left;">TaskManager可以缓存的ValueState状态条目数。状态缓存将占用Flink的堆内存，用户需结合业务类型和数据特征，评估TaskManager的内存使用情况。默认值为12000，若状态KV大小均为200B，TaskManager将额外占用2.2MB内存。</td>
      <td style="text-align: left;">该值最大建议配置为20000，若用户的内存资源限制严格，可以按照需求降低该配置值。</td>
    </tr>
    <tr>
      <td style="text-align: left;">state.backend.rocksdb.falcon.state-cache-bypass-hitRatio</td>
      <td style="text-align: left;">0.2</td>
      <td style="text-align: left;">ValueState状态缓存的旁路规避阈值，若缓存命中率低于该配置项，将关闭状态缓存，回退回原生Flink的读写操作。默认值为-1，表示永不关闭状态缓存。</td>
      <td style="text-align: left;">该值最大建议配置为0.5，以保证大部分场景中使能状态缓存优化特性。该值最小建议配置为0.05，以保证在缓存命中率较低的场景规避掉状态缓存，以免引入额外性能开销。/td>
    </tr>
  </tbody>
</table>
</font>



## OmniStateStore特性使能情况观测方式
<font size=3>

启动Flink任务后，可以在Flink的运行日志中观察OmniStateStore的特性使能情况。各加速特性的适用场景和使能观测方式由下表所示：<br>

**表2** OmniStateStore特性使能情况观测方式
<table>
  <thead>
    <tr>
      <th style="text-align: left;">特性</th>
      <th style="text-align: left;">适用场景</th>
      <th style="text-align: left;">观测方式</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">动态Filter技术-Flink智能多流感知算法</td>
      <td style="text-align: left;">对于ValueState，将其memTable结构由SkipList修改为HashLinkList，优化状态点读点写效率。</td>
      <td style="text-align: left;">[FALCON] {StateName} is valueState, use HashLinkList as memTable structure，若观测到该关键字，表示特性使能成功。</td>
    </tr>
    <tr>
      <td style="text-align: left;">动态Filter技术-前缀filter技术</td>
      <td style="text-align: left;">对于MapState，使用前缀filter过滤冗余磁盘查找，优化状态范围查询效率。</td>
      <td style="text-align: left;">[FALCON] {StateName} is map, use range filter，若观测到该关键字，表示特性使能成功。</td>
    </tr>
    <tr>
      <td style="text-align: left;">Flink语义状态缓存-Join算子数据缓存</td>
      <td style="text-align: left;">对于StreamingJoinOperator，通过数据缓存减少mapState范围查询次数。</td>
      <td style="text-align: left;">[FALCON] enable miniBatch process for StreaminJoinOperator，若观测到该关键字，表示特性使能成功。</td>
    </tr>
    <tr>
      <td style="text-align: left;">Flink语义状态缓存-ValueState状态缓存</td>
      <td style="text-align: left;">对于ValueState，通过状态缓存减少valueState范围点查点写开销。</td>
      <td style="text-align: left;">[FALCON] <{StateName}, VALUE> enable falcon cache，若观测到该关键字，表示特性使能成功。</td>
    </tr>
    <tr>
      <td style="text-align: left;">Merge读写优化</td>
      <td style="text-align: left;">对于StreamingJoinOperator，使用rocksdb的merge接口替换状态RMW操作。</td>
      <td style="text-align: left;">[FALCON] merge operation is used for left-records，若观测到该关键字，表示特性使能成功。</td>
    </tr>
  </tbody>
</table>

</font>



## 维护特性
<font size=3>

若需要升级OmniStateStore，请参阅[安装指南](installation_guide.md/#12-安装omnistatestore)安装新版本omniStateStore，无需卸载旧版本。<br>
若需要卸载OmniStateStore，请参阅[卸载指南](installation_guide.md/#13-卸载omnistatestore)卸载omniStateStore，并删除$FLINK_HOME/conf/flink-conf.yaml文件中的相关配置项。
</font>