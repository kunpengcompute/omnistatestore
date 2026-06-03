# Installation Guide
<font size=3>Learn how to install and deploy OmniStateStore.</font>

## Environment Requirements
### Hardware Requirements
<font size=3>
Before installing OmniStateStore, ensure that the hardware environment meets the requirements described in the following table.

Table 1 Hardware requirements
<table>
  <tbody>
    <tr>
      <td style="text-align: left;">Processor</td>
      <td style="text-align: left;">Kunpeng 920 or Kunpeng 950</td>
    </tr>
    <tr>
      <td style="text-align: left;">Memory size</td>
      <td style="text-align: left;">256 GB or above</td>
    </tr>
    <tr>
      <td style="text-align: left;">Memory frequency</td>
      <td style="text-align: left;">4,800 MT/s</td>
    </tr>
    <tr>
      <td style="text-align: left;">NIC</td>
      <td style="text-align: left;">NA</td>
    </tr>
    <tr>
      <td style="text-align: left;">Drive</td>
      <td style="text-align: left;">At least one 3.6 TB or 7.68 TB NVMe SSD</td>
    </tr>
  </tbody>
</table>
</font>

### Software Requirements
<font size=3>

The following table describes the OS and dependency software installation requirements.

**Table  2**  Software requirements
<table>
  <thead>
    <tr>
      <th style="text-align: left;">Software Name</th>
      <th style="text-align: left;">Software Version</th>
      <th style="text-align: left;">How to Obtain</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">OS</td>
      <td style="text-align: left;">openEuler 22.03 LTS SP3</td>
      <td style="text-align: left;"><a href="https://www.openeuler.org/en/download/archive/detail/?version=openEuler%2022.03%20LTS%20SP3">Link</a></td>
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
      <td style="text-align: left;"><a href="https://flink.apache.org/downloads/">Link</a></td>
    </tr>
    <tr>
      <td style="text-align: left;">Docker</td>
      <td style="text-align: left;">18.09.0</td>
      <td style="text-align: left;">-</td>
    </tr>
  </tbody>
</table>
</font>

### Obtaining the Software Package
<font size=3>

**Table 3** OmniStateStore software list
<table>
  <thead>
    <tr>
      <th style="text-align: left;">Software Package</th>
      <th style="text-align: left;">File Name</th>
      <th style="text-align: left;">Release Type</th>
      <th style="text-align: left;">Description</th>
      <th style="text-align: left;">How to Obtain</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align: left;">OmniStateStore package</td>
      <td style="text-align: left;">BoostKit-omniruntime-omnistatestore-1.3.0.zip</td>
      <td style="text-align: left;">Open source</td>
      <td style="text-align: left;">OmniStateStore software installation package</td>
      <td style="text-align: left;"><a href="https://atomgit.com/openeuler/OmniStateStore">Link</a></td>
    </tr>
  </tbody>
</table>
</font>



## Installing Dependencies
### Installing the JDK
<font size=3>

1. Download the [JDK software package](https://oraclelinux.pkgs.org/8/ol8-appstream-aarch64/java-1.8.0-openjdk-1.8.0.482.b08-1.0.1.el8.aarch64.rpm.html).

2. Go to the directory where the installation package is stored and execute the following command to install the JDK software.
```
sudo yum localinstall java-1.8.0-openjdk-1.8.0.482.b08-1.0.1.el8.aarch64.rpm
  
```
  After the installation is complete, run the following command to view the files in the default JDK installation directory.
 ```
  ls -l /usr/java/ 
 ```

3. Configure the environment variables by adding the following information to the **/etc/profile** file.

```
export JAVA_HOME=/usr/java/jdk-1.8.0
export JRE_HOME=$JAVA_HOME/jre
export CLASSPATH=.:$JAVA_HOME/lib:$JRE_HOME/lib
export PATH=$JAVA_HOME/bin:$PATH
```
4. Update the environment variables.

```
source /etc/profile
java -version    
javac -version   # View the JDK version.
```
If the correct version is displayed, the installation is successful.
</font>

### Installing Maven
<font size=3>

1. Download the [Maven software package](https://link.csdn.net/?from_id=119428896&target=https%3A%2F%2Farchive.apache.org%2Fdist%2Fmaven%2Fmaven-3%2F3.6.3%2Fbinaries%2Fapache-maven-3.6.3-bin.zip).

2. Place the Maven software package in the installation directory (for example, **/opt**) and deploy the software package.
```
cd /opt
unzip apache-maven-3.6.3-bin.zip
rm -rf apache-maven-3.6.3-bin.zip
```
3. Configure the environment variables by adding the following information to the **/etc/profile** file.
```
export MAVEN_HOME=/opt/apache-maven-3.6.3
export PATH=$MAVEN_HOME/bin:$PATH
```
4. Update and verify the environment variables.
```
source /etc/profile
mvn -version # View the Maven version.
```
If the correct version is displayed, the installation is successful.
</font>

### Installing the GCC
<font size=3>

1. Download the [GCC binary installation package](https://mirrors.huaweicloud.com/kunpeng/archive/compiler/kunpeng_gcc/gcc-10.3.1-2021.09-aarch64-linux.tar.gz).

2. Place the software package in the installation directory (for example, **/opt**) and deploy the software package.
```
cd /opt
tar -zxvf gcc-10.3.1-2021.09-aarch64-linux.tar.gz
mv gcc-10.3.1-2021.09-aarch64-linux gcc-10.3.1
rm -rf gcc-10.3.1-2021.09-aarch64-linux.tar.gz
```
3. Configure the environment variables by adding the following information to the **/etc/profile** file.
```
export GCC_HOME=/opt/gcc-10.3.1
export PATH=$GCC_HOME/bin:$PATH
export LD_LIBRARY_PATH=$GCC_HOME/lib64:$GCC_HOME/lib:$LD_LIBRARY_PATH
export CPLUS_INCLUDE_PATH=$GCC_HOME/include/c++/10.3.1:$GCC_HOME/include:$CPLUS_INCLUDE_PATH
```
4. Update the environment variables.
```
source /etc/profile
gcc --version
g++ --version  # View the GCC and G++ version.
```
If the correct version is displayed, the installation is successful.
</font>

### Installing Flink
<font size=3>

1. Download [Flink](https://archive.apache.org/dist/flink/flink-1.16.3), for example, **flink-1.16.3-bin-scala_2.12.tgz** for Scala 2.12.

2. Place the software package in the installation directory (for example, **/opt**) and deploy the software package.
```
cd /opt
tar -zxvf flink-1.16.3-bin-scala_2.12.tgz
mv flink-1.16.3-bin-scala_2.12 flink-1.16.3
rm -rf flink-1.16.3-bin-scala_2.12.tgz
```
3. Configure the environment variables by adding the following information to the **/etc/profile** file.
```
export FLINK_HOME=/opt/flink-1.16.3
export PATH=$FLINK_HOME/bin:$PATH
```
4. Update the environment variables.
```
source /etc/profile
```
</font>

### Installing Docker
<font size=3>

Install Docker and deploy multiple containers to set up the Flink environment. If the server cannot connect to the Internet, configure a local yum repository according to your environment to ensure a smooth installation.

1. Install Docker and import the base image. For details, see the [Docker Installation Guide (CentOS & openEuler)](https://www.hikunpeng.com/document/detail/en/kunpengcpfs/ecosystemEnable/Docker/kunpengdocker_03_0001.html).

```
cd /opt
wget --no-check-certificate https://mirrors.huaweicloud.com/openeuler/openEuler-22.03-LTS-SP4/docker_img/aarch64/openEuler-docker.aarch64.tar.xz
docker load < openEuler-docker.aarch64.tar.xz
```

2. Create a network in bridge mode and check whether the network is successfully created.
```
docker network create -d bridge flink-network
docker network ls
```

3. Create and start three Docker containers. The container flavor is 8C32G, and the containers are named **flink\_jm\_8c32g**, **flink\_tm1\_8c32g**, and **flink\_tm2\_8c32g**. After all containers are started, the command execution process automatically exits.
```
docker run -it -d --name flink_jm_8c32g --cpus=8 --memory=32g --network flink-network openeuler-22.03-lts-sp4 /bin/bash 
docker run -it -d --name flink_tm1_8c32g --cpus=8 --memory=32g --network flink-network openeuler-22.03-lts-sp4 /bin/bash 
docker run -it -d --name flink_tm2_8c32g --cpus=8 --memory=32g --network flink-network openeuler-22.03-lts-sp4 /bin/bash
docker ps 
```

4. Log in to all containers, enable the SSH service in the containers, and configure password-free login.
```
docker exec -it flink_jm_8c32g /bin/bash
docker exec -it flink_tm1_8c32g /bin/bash
docker exec -it flink_tm2_8c32g /bin/bash
yum -y install openssh-clients openssh-server passwd vim findutils net-tools libXext libXrender gcc cmake make gcc-c++ unzip wget libXtst # Install the dependencies for the SSH service.
ssh-keygen -A # Generate the RSA key.
/usr/sbin/sshd -D & # Start the SSH service in the container.
passwd [user password] # Set a password for the container.
ssh-keygen -t rsa # Generate the RSA key again. Press "Enter" when prompted.
exit # Exit the container.
docker exec -it flink_jm_8c32g /bin/bash
ssh-copy-id -i ~/.ssh/id_rsa.pub root@flink_tm1_8c32g
ssh-copy-id -i ~/.ssh/id_rsa.pub root@flink_tm2_8c32g # Configure SSH password-free login from the "flink\_jm\_8c32g" container to the other containers.
```
</font>

## Installing OmniStateStore
<font size=3>

1. Obtain the software package **BoostKit-omniruntime-omnistatestore-1.3.0.zip** based on [OmniStateStore software list](#Obtaining the software package).

2. Configure the environment variable by specifying **FLINK_HOME**, **JAVA_HOME**, and **LD_LIBRARY_PATH**.

```
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/lib:$JAVA_HOME/jre/lib/aarch64:$JAVA_HOME/jre/lib/aarch64/server:/usr/local/lib
```
3. Log in to the installation node, extract **BoostKit-omniruntime-omnistatestore-1.3.0.zip** to **$FLINK_HOME/lib**, copy **librocksdb.so.6** to **/usr/local/lib**, and save **flink-alg-falcon.jar** to the current directory.

```
unzip BoostKit-omniruntime-omnistatestore-1.3.0.zip
mv librocksdb.so.6 /usr/local/lib
rm -rf BoostKit-omniruntime-omnistatestore-1.3.0.zip
```
</font>

## Uninstalling OmniStateStore
<font size=3>

If you need to uninstall OmniStateStore, go to the **$FLINK_HOME/lib** directory to uninstall related software packages.
```
rm -rf /usr/local/lib/librocksdb.so.6
rm -rf flink-alg-falcon.jar
```
</font>
