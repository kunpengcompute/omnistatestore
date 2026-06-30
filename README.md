# OmniStateStore介绍

## 最新消息

- [2026.06.30] 发布OmniStateStore 1.3.0。通过调整RocksDB不同SST层级的压缩格式，结合软算压缩算法优化，降低状态Compaction过程中的压缩/解压缩开销，提升应用端到端吞吐。
- [2026.03.30] 发布OmniStateStore 1.2.0。基于对接Flink和RocksDB的插件完成Flink有状态用例性能加速。对Flink进行轻量级修改，基于状态缓存和状态过滤技术，降低Flink对RocksDB的访问频次，提升有状态用例的IO性能。1.2.0版本进行了架构调整，与1.1.0以及1.0.0相互独立。
- [2025.12.30] 发布OmniStateStore 1.1.0。新增支持对接Flink Metric框架并实现部分常用的Metric指标；支持Priority Queue持久化存储；支持KV分离存储。
- [2025.06.30] 发布OmniStateStore 1.0.0。解决了大数据场景下，针对大状态下IO性能较差的问题，实现了一种新型的状态存储方式，提升了Flink的IO性能。

## 项目简介

### 简介

大数据OmniRuntime通过插件化的形式，提升数据加载、数据计算和数据交换性能，从而提升大数据分析端到端性能。

随着互联网的发展，数据规模出现了爆炸式的增长，需要处理的数据量越来越大，CPU算力的增长远远滞后于数据的增长。大数据开源生态也越来越丰富，但多样化的计算引擎和开源组件也同时带来了全生命周期数据处理性能提升难的问题。不同的大数据引擎采用各自独特的优化策略和技术来提高性能和效率，但有些优化项会在多个引擎中重复应用，可能存在差异或冲突，导致计算性能下降。此外，重复应用相同的优化项可能导致资源竞争和冲突，降低了整体计算性能。

大数据OmniRuntime是鲲鹏BoostKit大数据面向应用加速推出的一系列特性，通过插件化的形式，提升数据加载、数据计算和数据交换性能，从而提升大数据分析端到端性能。

OmniStateStore状态优化作为OmniRuntime的特性之一，通过对Flink进行轻量级修改，引入状态缓存、状态过滤、软算压缩优化等技术减少Flink对RocksDB的访问频次，降低Flink使用RocksDB的IO开销，同时降低状态Compaction过程中的压缩/解压缩开销，最终提升Flink有状态用例的端到端性能。已适配的开源组件及版本有：

- Flink1.16.3 + RocksDB6.20.3 (26.0.RC1)

## 特性介绍

状态存储(state store)是Flink的重要组成部分，主要由状态后端(state backend)来完成。随着状态(state)中数据量的增大，状态存储性能面临挑战。OmniStateStore对Flink进行轻量级修改，通过状态缓存、状态过滤、软算压缩优化等技术，加速Flink对RocksDB的使用效率，从而提升Flink的端到端性能。

OmniStateStore是对接Flink和RocksDB的中间层，包含动态Filter技术、Flink语义状态缓存、Merge读写优化和LZ4软算压缩优化，其整体架构设计图如下图所示：

- 动态Filter技术：使用状态前缀filter，过滤mapState范围查询时的冗余磁盘查找操作；对于仅需要点读、点写的状态，将memTable数据结构替换为HashLinkList, 提升状态点读和点写效率。
- Flink语义状态缓存：通过ValueState状态缓存，同Key状态优先在内存中完成聚合，减少状态对RocksDB的访问频次；通过Join算子数据缓存，减少双流Join操作的状态范围查询次数。
- Merge读写优化：使用RocksDB的merge接口，替换Flink SQL/DataStream的状态RMW操作。
- LZ4软算压缩优化：将RocksDB Level0/Level1的压缩格式修改为LZ4，结合LZ4软算压缩算法优化，提升状态压缩/解压缩效率。

**图1** OmniStateStore整体架构设计

<a href="./docs/zh/figures/OmniStateStore整体架构设计.png"><img src="./docs/zh/figures/OmniStateStore整体架构设计.png" alt="OmniStateStore整体架构设计" width="800" /></a>

### 典型部署配置

OmniStateStore作为对接Flink和RocksDB的插件，其部署方式和Flink保持一致，支持Yarn、Standalone以及容器化等多种部署模式。

典型部署场景在3个Docker容器内，容器配置均为8核、32GB内存，其中一个容器部署JobManager，另外两个容器中各部署4个TaskManager。JobManager分配8GB内存，单个TaskManager分配2个TaskSlot和8GB内存。

### 应用场景

OmniStateStore适用于Apache Flink流处理任务中的有状态场景，通常由于状态数据量增大导致IO性能成为主要性能瓶颈。 典型使用场景包括但不限于：

- 实时大数据处理任务：如实时ETL、流式聚合、窗口计算等，其中状态规模随数据持续流入不断增长。
- 复杂事件处理与有状态流计算：需长时间维护大规模状态（例如用户会话跟踪、实时风控模型状态）。

通过状态缓存、状态过滤、软算压缩优化等技术，OmniStateStore可以有效减少Flink对RocksDB的访问频次，同时降低状态压缩/解压缩开销，从而有效提升有状态作业的端到端吞吐。OmniStateStore适用于openEuler 22.03 LTS SP3等操作系统环境，并支持Flink1.16.3 + RocksDB6.20.3架构。

## 约束与限制

OmniStateStore的性能提升比依赖于用例的RocksDB占比和状态操作类型，对于RocksDB占比低的场景仅保证性能不劣化。

OmniStateStore作为Flink的加速组件，目前仅兼容华为鲲鹏计算平台，将在后续支持在通用X86服务器上运行。

## 目录结构

项目全量目录层级介绍如下：

```text
OmniStateStore/                       # 项目根目录
|—— conf/                             # flink配置文件 
|   └── flink-conf.yaml               # 使能omniStateStore的flink配置参数样例
|—— docs/                             # 项目文档目录
|   |—— zh                            # 中文文档目录
|   |   |—— figures                   # 中文文档图片资源目录
|   |   |—— quick_start.md            # 快速入门
|   |   |—— release_notes.md          # OmniStateStore版本说明书
|   |   |—— installation_guide.md     # OmniStateStore安装指导
|   |   |—— user_guide.md             # OmniStateStore使用指导
|   |   |—— best_practices.md         # OmniStateStore场景化应用最佳实践
|   |   |—— design_guide.md           # OmniStateStore设计指南
|   |   └── faq.md                    # OmniStateStore安装使用常见问题
|—— cpp/                              # omniStateStore核心代码-c++部分
|—— java/                             # omniStateStore核心代码-java部分
|—— scripts/                          # omniStateStore代码自动构建脚本
|   |—— build.sh                      # 自动构建脚本
|   └── Makefile.patch                # 用于编译rocksdb的编译脚本patch，合入rocksdb源码后完成编译
|—— .gitignore                        # 项目工程git配置
|—— LICENSE                           # LICENSE
└── README.md                         # README
```

## 版本说明

每个版本的特性变更详细信息，具体请参见《[版本说明书](./docs/zh/release_notes.md)》。

## 环境部署

介绍OmniStateStore的环境依赖及安装方式，具体请参见《[安装指南](./docs/zh/installation_guide.md)》。

## 快速入门

安装OmniStateStore后如何快速验证OmniStateStore是否生效，性能是否提升，具体请参见《[快速入门](./docs/zh/quick_start.md)》。

## 学习文档

**表1** 学习文档列表
<table>
  <thead>
    <tr>
      <th style="text-align: left;">名称</th>      
      <th style="text-align: left;">简介</th>
    </tr>
  </thead>
  <tbody>
    <tr>      
      <td style="text-align: left;"><a  href="./docs/zh/quick_start.md">快速入门</a></td>
      <td style="text-align: left;">提供快速使能并验证OmniStateStore加速能力的快速入门指导。</td>
    </tr>
    <tr>      
      <td style="text-align: left;"><a href="./docs/zh/release_notes.md">版本说明书</a></td>
      <td style="text-align: left;">提供OmniStateStore每个版本发布的基础信息和特性更新信息。</td>
    </tr>
    <tr>      
      <td style="text-align: left;"><a href="./docs/zh/design_guide.md">设计指南</a></td>
      <td style="text-align: left;">提供OmniStateStore的特性说明。</td>
    </tr>
    <tr>     
      <td style="text-align: left;"><a href="./docs/zh/installation_guide.md">安装指南</a></td>
      <td style="text-align: left;">提供安装OmniStateStore的详细安装指导。</td>
    </tr>
    <tr>      
      <td style="text-align: left;"><a href="./docs/zh/user_guide.md">用户指南</a></td>
      <td style="text-align: left;">提供使用OmniStateStore的详细使用指导。</td>
    </tr>
    <tr>      
      <td style="text-align: left;"><a href="./docs/zh/best_practices.md">最佳实践</a></td>
      <td style="text-align: left;">提供OmniStateStore的实践案例。</td>
    </tr>
   <tr>      
      <td style="text-align: left;"><a href="./docs/zh/faq.md">常见问题</a></td>
      <td style="text-align: left;">提供omniStateStore安装和运行过程中的场景问题和解决方案。</td>
    </tr>
    <tr>      
      <td style="text-align: left;">视频课程：<a href="https://www.hikunpeng.com/document/video-detail/2644?page=home&type=video&title=OmniRuntime%E7%89%B9%E6%80%A7%E5%A4%A7%E6%8F%AD%E7%A7%98">OmniRuntime特性大揭秘</a></td>
      <td style="text-align: left;">提供操作视频，帮助开发者在鲲鹏服务器上了解、使能OmniRuntime特性。</td>
    </tr>
  </tbody>
</table>

## 安全声明

### 防病毒软件例行检查

定期开展对集群和Flink组件的防病毒扫描，防病毒例行检查会帮助集群免受病毒、恶意代码、间谍软件以及恶意程序，降低系统瘫痪、信息泄露风险。建议使用业界主流防病毒软件进行防病毒检查。

### 日志控制

- 检查系统是否可以限制单个日志文件的大小。
- 检查日志空间占满后，是否存在机制进行清理。

### 漏洞修复

为了保证生产环境的安全，降低被攻击的风险，请开启防火墙，并定期修复以下漏洞：

- 操作系统漏洞
- JDK漏洞
- Flink漏洞
- ZooKeeper漏洞
- Kerberos漏洞
- OpenSSL漏洞
- 其他相关组件漏洞

以CVE-2021037317为例

漏洞描述：Netty 4.1.17版本存在两个Content-Length的http header可能发生混淆的风险通告，漏洞编号为CVE-2021037317。
本系统使用hdfs-ceph(version 3.2.0)服务作为存算分离的存储对象，它因依赖aws-java-sdk-bundle-1.11.375.jar而涉及该漏洞。建议用户及时更新漏洞补丁进行防护，以免遭受黑客攻击。

影响范围：Netty 4.1.68及以前版本。

修复建议：目前厂商已发布升级补丁以修复漏洞，请参见[GitHub](https://github.com/netty/netty/security/advisories/GHSA-9vjp-v76f-g363)修复漏洞。

### SSH加固

在部署安装过程中，需要通过SSH连接服务器。由于root用户拥有最高权限，直接使用root用户登录服务器可能会存在安全风险。建议您使用普通用户登录服务器进行安装部署，并建议您通过配置禁止root用户SSH登录的选项，来提升系统安全性。操作步骤：

用户登录系统后检查"/etc/ssh/sshd_config"配置项"PermitRootLogin"

- 如果显示no，说明禁止了root用户SSH登录。
- 如果显示yes，说明需要修改"PermitRootLogin"为no。

### 公网地址声明

**表2** 公网地址声明
<table>
  <tbody>
    <tr>
      <td style="text-align: left;">开源软件/第三方软件</td>
      <td style="text-align: left;">GCC, Maven</td>
    </tr>
    <tr>
      <td style="text-align: left;">类型</td>
      <td style="text-align: left;">开源软件</td>
    </tr>
    <tr>
      <td style="text-align: left;">公网IP地址/公网URL地址/域名/邮箱地址</td>
      <td style="text-align: left;"><a href="https://gcc.gun.org/bugs/">https://gcc.gun.org/bugs/</a></td>
    </tr>
    <tr>
      <td style="text-align: left;">所在文件类型</td>
      <td style="text-align: left;">二进制</td>
    </tr>
    <tr>
      <td style="text-align: left;">文件名</td>
      <td style="text-align: left;">flink-alg-falcon.jar, librocksdb.so.6</td>
    </tr>
    <tr>
      <td style="text-align: left;">用途描述</td>
      <td style="text-align: left;">对应的邮箱地址为GCC开源组件官网地址，为编译开源组件时被动引入，产品实际未使用</td>
    </tr>
    <tr>
      <td style="text-align: left;">软件包</td>
      <td style="text-align: left;">BoostKit-omniruntime-omniStateStore-1.3.0.zip</td>
    </tr>
  </tbody>
</table>

## 免责声明

**致OmniStateStore使用者**

- 本工具仅供调试和开发之用，使用者需自行承担使用风险，并理解以下内容：

  - 数据处理及删除：用户在使用本工具过程中产生的数据属于用户责任范畴。建议用户在使用完毕后及时删除相关数据，以防信息泄露。
  - 数据保密与传播：使用者了解并同意不得将通过本工具产生的数据随意外发或传播。对于由此产生的信息泄露、数据泄露或其他不良后果，本工具及其开发者概不负责。
  - 用户输入安全性：用户需自行保证输入命令行的安全性，并承担因输入不当导致的任何安全风险或损失。对于输入命令行不当所导致的问题，本工具及其开发者概不负责。
- 免责声明范围：本免责声明适用于所有使用本工具的个人或实体。使用本工具即表示您同意接受本声明的内容，并愿意承担因使用该功能而产生的风险和责任，如有异议请停止使用本工具。
- 在使用本工具之前，请**谨慎阅读并理解以上免责声明的内容**。对于使用本工具所产生的任何问题或疑问，请及时联系开发者。

**致数据所有者**

如果您不希望您的模型或数据等信息在OmniStateStore中被提及，或希望更新OmniStateStore中有关的描述，请在GitCode提交issue，我们将根据您的issue要求删除或更新您的描述。衷心感谢您对OmniStateStore的理解和贡献。

## License

OmniStateStore产品使用的许可证，具体请参见 [LICENSE](./LICENSE)。

OmniStateStore产品的文档使用的许可证，具体请参见 [LICENSE](./docs/LICENSE)。

## 贡献声明

1. 提交错误报告：如果您在OmniStateStore中发现了一个不存在安全问题的漏洞，请在OmniStateStore仓库中的Issues中搜索，以防该漏洞被重复提交，如果找不到可以创建一个新的Issue。如果发现了一个安全问题请不要将其公开，请参阅安全问题处理方式。提交错误报告时应包含完整信息。

2. 安全问题处理：本项目中对安全问题处理的形式，请通过邮箱通知项目核心人员确认编辑。

3. 解决现有问题：通过查看仓库的Issues列表可以发现需要处理的问题信息，可以尝试解决其中的某个问题。

4. 如何提出新功能：请使用issues的Feature标记进行标记，我们会定期处理和确认开发。

5. 开始贡献：

&emsp; a. Fork本项目的仓库。

&emsp; b. Clone到本地。

&emsp; c. 创建开发分支。

&emsp; d. 本地测试：提交前请通过所有单元测试，包括新增的测试用例。

&emsp; e. 提交代码。

&emsp; f. 新建pull request。

&emsp; g. 代码检视：您需要根据评审意见修改代码，并重新提交更新。此流程可能涉及多轮迭代。

&emsp; h. 当您的PR获得足够数量的检视者批准后，Committer会进行最终审核。

&emsp; i. 审核和测试通过后，CI会将您的PR合并到项目的主干分支。

## 建议与交流

欢迎大家为社区做贡献。如果有任何疑问或建议，请提交Issues，我们会尽快回复。感谢您的支持。

## 致谢

OmniStateStore由华为公司的下列部门联合贡献：

- 计算技术开发部
- 鲲鹏计算Boostkit产品部

感谢来自社区的每一个PR，欢迎贡献OmniStateStore！
