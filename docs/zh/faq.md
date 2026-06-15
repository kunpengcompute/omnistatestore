# 常见问题

记录OmniStateStore安装和使能过程中的常见问题及其解决方案。

## OmniStateStore和Flink版本不适配导致任务无法正常启动的解决方法

**问题现象**

使用Flink1.16.1运行nexmark0.2-Q4用例，并尝试使能OmniStateStore特性，出现类构造器未查询到的问题。如下图所示：

**图1** OmniStateStore运行报错截图

<a href="./figures/OmniStateStore faq1.png"><img src="./figures/OmniStateStore faq1.png" alt="faq" width="1200" /></a>

**问题原因**

目前，OmniStateStore只支持Flink 1.16.3 + FRocksDB 6.20.3。由于OmniStateStore需要对Flink进行轻量级代码修改，若用户也自行修改了Flink代码，二者可能产生冲突。

具体而言，OmniStateStore对RocksDBResourceContainer所在类进行了轻量级代码修改。而Flink1.16.1的同名类也在Flink 1.16.3的基础上也做了代码修改，这两部分修改存在冲突。在Flink任务执行时，优先加载了OmniStateStore的同名类文件，进而导致RocksDBResourceContainer的构造器无法找到。

**解决方案**

可以采取以下两种方案进行测试：
方案一：回退至Flink 1.16.3版本开展测试。
方案二：将Flink 1.16.1的修改内容合并到 OmniStateStore中，重新编译打包后进行测试。

## OmniStateStore未正确部署导致任务无法正常启动的解决方法

**问题现象**

Flink任务运行时未成功加载librocksdb.so.6，导致任务运行失败。提示如下图所示信息：

**图2** OmniStateStore JobManager运行报错截图

<a href="./figures/OmniStateStore faq2-1.png"><img src="./figures/OmniStateStore faq2-1.png" alt="faq" width="1200" /></a>


**图3** OmniStateStore TaskManager运行报错截图

<a href="./figures/OmniStateStore faq2-2.png"><img src="./figures/OmniStateStore faq2-2.png" alt="faq" width="1200" /></a>

**问题原因**

OmniStateStore运行时依赖librocksdb.so.6，而Flink任务会从LD_LIBRARY_PATH目录中加载该动态库。若未按照要求将动态库部署到指定路径中，或是未正确配置LD_LIBRARY_PATH环境变量，将导致任务运行失败。

**解决方案**

**步骤1**&emsp;检查LD_LIBRARY_PATH环境变量，并将librocksdb.so.6拷贝到指定目录下。
```shell
echo $LD_LIBRARY_PATH  # 输出结果实例：/usr/local/lib
ll /usr/local/lib  # 查看是否包含librocksdb.so.6
```
**步骤2**&emsp;如果没有包含librocksdb.so.6，则需要拷贝到指定目录下。
```shell
mv librocksdb.so.6 /usr/local/lib
```