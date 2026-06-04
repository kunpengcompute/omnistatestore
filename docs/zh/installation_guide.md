# 安装指南
<font size=3>提供OmniStateStore的安装说明，指导用户如何安装和部署OmniStateStore。</font>



## 环境要求
### 硬件要求
<font size=3>
在安装OmniStateStore之前，请确保硬件环境已经满足安装部署的要求，硬件配置要求如表1所示。

**表1** 硬件配置要求
<table>
  <tbody>
    <tr>
      <td style="text-align: left;">处理器</td>
      <td style="text-align: left;">鲲鹏920系列处理器，鲲鹏950处理器</td>
    </tr>
    <tr>
      <td style="text-align: left;">内存</td>
      <td style="text-align: left;">256GB以上</td>
    </tr>
    <tr>
      <td style="text-align: left;">内存频率</td>
      <td style="text-align: left;">4800 MT/s</td>
    </tr>
    <tr>
      <td style="text-align: left;">网卡</td>
      <td style="text-align: left;">NA</td>
    </tr>
    <tr>
      <td style="text-align: left;">磁盘</td>
      <td style="text-align: left;">至少一块3.6TB或7.68TB的NVME SSD</td>
    </tr>
  </tbody>
</table>
</font>

### 软件要求
<font size=3>

安装OmniStateStore之前，需要将前置依赖的软件安装成功，集群各节点的操作系统和软件要求如表2所示。

**表2** 软件配置要求
<table>
  <thead>
    <tr>
      <th style="text-align: left;">软件名称</th>
      <th style="text-align: left;">软件版本</th>
      <th style="text-align: left;">获取方式</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">OS</td>
      <td style="text-align: left;">openEuler 22.03 LTS SP3</td>
      <td style="text-align: left;"><a href="https://www.openeuler.openatom.cn/zh/download/archive/detail/?version=openEuler%2022.03%20LTS%20SP3">Link</a></td>
    </tr>
    <tr>
      <td style="text-align: left;">JDK</td>
      <td style="text-align: left;">JDK 1.8.0_432</td>
      <td style="text-align: left;"><a href="https://oraclelinux.pkgs.org/8/ol8-appstream-x86_64/java-1.8.0-openjdk-1.8.0.432.b06-2.0.1.el8.x86_64.rpm.html">Link</a></td>
    </tr>
    <tr>
      <td style="text-align: left;">Maven</td>
      <td style="text-align: left;">Apache Maven 3.6.3</td>
      <td style="text-align: left;"><a href="https://link.csdn.net/?from_id=119428896&target=https%3A%2F%2Farchive.apache.org%2Fdist%2Fmaven%2Fmaven-3%2F3.6.3%2Fbinaries%2Fapache-maven-3.6.3-bin.zip
">Link</a></td>
    </tr>
    <tr>
      <td style="text-align: left;">GCC</td>
      <td style="text-align: left;">10.3.1</td>
      <td style="text-align: left;"><a href="https://mirrors.huaweicloud.com/kunpeng/archive/compiler/kunpeng_gcc">Link</a></td>
    </tr>
    <tr>
      <td style="text-align: left;">Flink</td>
      <td style="text-align: left;">1.16.3</td>
      <td style="text-align: left;"><a href="https://flink.apache.org/zh/downloads/">Link</a></td>
    </tr>
    <tr>
      <td style="text-align: left;">Docker</td>
      <td style="text-align: left;">18.09.0</td>
      <td style="text-align: left;">/</td>
    </tr>
  </tbody>
</table>
</font>

### 获取软件包
<font size=3>

**表3** OmniStateStore状态优化软件获取列表
<table>
  <thead>
    <tr>
      <th style="text-align: left;">名称</th>
      <th style="text-align: left;">包名</th>
      <th style="text-align: left;">发布类型</th>
      <th style="text-align: left;">说明</th>
      <th style="text-align: left;">获取地址</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">OmniStateStore状态优化压缩包</td>
      <td style="text-align: left;">BoostKit-omniruntime-omnistatestore-1.3.0.zip</td>
      <td style="text-align: left;">开源</td>
      <td style="text-align: left;">OmniStateStore状态优化软件安装包</td>
      <td style="text-align: left;"><a href="https://atomgit.com/openeuler/OmniStateStore">Link</a></td>
    </tr>
  </tbody>
</table>
</font>



## 安装依赖
### 安装JDK
<font size=3>

1. 下载[JDK软件包](https://oraclelinux.pkgs.org/8/ol8-appstream-aarch64/java-1.8.0-openjdk-1.8.0.482.b08-1.0.1.el8.aarch64.rpm.html)。

2. 进入安装包的存放目录，执行以下命令安装JDK软件包。
```
sudo yum localinstall java-1.8.0-openjdk-1.8.0.482.b08-1.0.1.el8.aarch64.rpm
  
```
  安装完成后，可通过以下命令查看JDK默认安装目录下的文件情况。
 ```
  ls -l /usr/java/ 
```
  
3. 配置环境变量，在“/etc/profile”文件中添加如下信息。
```
export JAVA_HOME=/usr/java/jdk-1.8.0
export JRE_HOME=$JAVA_HOME/jre
export CLASSPATH=.:$JAVA_HOME/lib:$JRE_HOME/lib
export PATH=$JAVA_HOME/bin:$PATH
```
4. 更新环境变量。
```
source /etc/profile
java -version    
javac -version   # 查看JDK版本信息
```
若能正确显示版本信息，则表明JDK安装成功。
</font>

### 安装Maven
<font size=3>

1. 下载[Maven软件包](https://link.csdn.net/?from_id=119428896&target=https%3A%2F%2Farchive.apache.org%2Fdist%2Fmaven%2Fmaven-3%2F3.6.3%2Fbinaries%2Fapache-maven-3.6.3-bin.zip)。

2. 将Maven软件包放置于安装目录中(以“/opt”目录为例)，并完成软件包部署：
```
cd /opt
unzip apache-maven-3.6.3-bin.zip
rm -rf apache-maven-3.6.3-bin.zip
```
3. 配置环境变量，在“/etc/profile”文件中添加以下内容：
```
export MAVEN_HOME=/opt/apache-maven-3.6.3
export PATH=$MAVEN_HOME/bin:$PATH
```
4. 更新并验证环境变量。
```
source /etc/profile
mvn -version # 查看Maven版本信息
```
若能正确显示版本信息，则表明Maven安装成功。
</font>

### 安装GCC
<font size=3>

1. 下载[GCC二进制安装包](https://mirrors.huaweicloud.com/kunpeng/archive/compiler/kunpeng_gcc/gcc-10.3.1-2021.09-aarch64-linux.tar.gz)。

2. 将软件包放置于安装目录中（这里以“/opt”目录为例），完成软件包部署：
```
cd /opt
tar -zxvf gcc-10.3.1-2021.09-aarch64-linux.tar.gz
mv gcc-10.3.1-2021.09-aarch64-linux gcc-10.3.1
rm -rf gcc-10.3.1-2021.09-aarch64-linux.tar.gz
```
3. 配置环境变量，在“/etc/profile”文件中添加以下内容：
```
export GCC_HOME=/opt/gcc-10.3.1
export PATH=$GCC_HOME/bin:$PATH
export LD_LIBRARY_PATH=$GCC_HOME/lib64:$GCC_HOME/lib:$LD_LIBRARY_PATH
export CPLUS_INCLUDE_PATH=$GCC_HOME/include/c++/10.3.1:$GCC_HOME/include:$CPLUS_INCLUDE_PATH
```
4. 更新环境变量。
```
source /etc/profile
gcc --version
g++ --version  # 查看GCC和G++版本信息
```
若能正确显示版本信息，则表明GCC安装成功。
</font>

### 安装Flink
<font size=3>

1. 下载[Flink](https://archive.apache.org/dist/flink/flink-1.16.3)，此处以 Scala 2.12 版本为例，即flink-1.16.3-bin-scala_2.12.tgz。

2. 将软件包放置于安装目录中（这里以/opt目录为例），完成软件包部署：
```
cd /opt
tar -zxvf flink-1.16.3-bin-scala_2.12.tgz
mv flink-1.16.3-bin-scala_2.12 flink-1.16.3
rm -rf flink-1.16.3-bin-scala_2.12.tgz
```
3. 配置环境变量，在/etc/profile文件中添加：
```
export FLINK_HOME=/opt/flink-1.16.3
export PATH=$FLINK_HOME/bin:$PATH
```
4. 更新环境变量。
```
source /etc/profile
```
</font>

### 安装Docker
<font size=3>

安装Docker并部署多个容器以搭建Flink环境。如果服务器无法连接外网，请根据实际情况配置本地Yum源，确保安装过程顺利。

1. 请参见《[Docker 安装指南（CentOS&openEuler）](https://www.hikunpeng.com/document/detail/zh/kunpengcpfs/ecosystemEnable/Docker/kunpengdocker_03_0001.html)》安装Docker，并导入基础镜像。
```
cd /opt
wget --no-check-certificate https://mirrors.huaweicloud.com/openeuler/openEuler-22.03-LTS-SP4/docker_img/aarch64/openEuler-docker.aarch64.tar.xz
docker load < openEuler-docker.aarch64.tar.xz
```

2. 创建bridge模式网络，确认网络是否创建成功。
```
docker network create -d bridge flink-network
docker network ls
```

3. 创建并启动3个Docker容器。创建的容器规格为8c32g，分别命名为flink\_jm\_8c32g、flink\_tm1\_8c32g、flink\_tm2\_8c32g。所有容器启动完成后自动退出命令执行流程。
```
docker run -it -d --name flink_jm_8c32g --cpus=8 --memory=32g --network flink-network openeuler-22.03-lts-sp4 /bin/bash 
docker run -it -d --name flink_tm1_8c32g --cpus=8 --memory=32g --network flink-network openeuler-22.03-lts-sp4 /bin/bash 
docker run -it -d --name flink_tm2_8c32g --cpus=8 --memory=32g --network flink-network openeuler-22.03-lts-sp4 /bin/bash
docker ps 
```

4. 登录所有容器，容器内启动SSH服务，并配置免密登录。
```
docker exec -it flink_jm_8c32g /bin/bash
docker exec -it flink_tm1_8c32g /bin/bash
docker exec -it flink_tm2_8c32g /bin/bash
yum -y install openssh-clients openssh-server passwd vim findutils net-tools libXext libXrender gcc cmake make gcc-c++ unzip wget libXtst # 安装SSH服务依赖
ssh-keygen -A # 生成RSA密钥
/usr/sbin/sshd -D & # 启动容器内SSH服务
passwd [user password] # 为容器设置密码
ssh-keygen -t rsa # 再次生成RSA密钥，遇到提示时，按“Enter“
exit # 退出容器
docker exec -it flink_jm_8c32g /bin/bash
ssh-copy-id -i ~/.ssh/id_rsa.pub root@flink_tm1_8c32g
ssh-copy-id -i ~/.ssh/id_rsa.pub root@flink_tm2_8c32g # 在flink\_jm\_8c32g容器上配置对其他容器的SSH免密登录
```
</font>

## 安装OmniStateStore
<font size=3>

1. 请参见[OmniStateStore状态优化软件获取列表](#获取软件包)获取软件包BoostKit-omniruntime-omnistatestore-1.3.0.zip。

2. 配置环境变量，指定FLINK_HOME和JAVA_HOME，并配置LD_LIBRARY_PATH。
```
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/lib:$JAVA_HOME/jre/lib/aarch64:$JAVA_HOME/jre/lib/aarch64/server:/usr/local/lib
```
3. 登录安装节点，将BoostKit-omniruntime-omnistatestore-1.3.0.zip解压到$FLINK_HOME/lib目录下，将librocksdb.so.6拷贝到/usr/local/lib目录，将flink-alg-falcon.jar保留至当前目录中。

```
unzip BoostKit-omniruntime-omnistatestore-1.3.0.zip
mv librocksdb.so.6 /usr/local/lib
rm -rf BoostKit-omniruntime-omnistatestore-1.3.0.zip
```
</font>



## 卸载OmniStateStore
<font size=3>

使用完成OmniStateStore后，如果需要卸载OmniStateStore，请进入“$FLINK_HOME/lib”目录卸载相关软件包。
```
rm -rf /usr/local/lib/librocksdb.so.6
rm -rf flink-alg-falcon.jar
```
</font>



