#!/bin/bash
set -ex

# step1: build rocksdb
mkdir 3rdparty && cd 3rdparty

git config --global http.postBuffer 524288000
git config --global http.lowSpeedLimit 0
git config --global http.lowSpeedTime 999999
git config --global core.compression 0
git clone https://gitee.com/mirrors_ververica/frocksdb.git
#git clone https://github.com/ververica/frocksdb.git
cd frocksdb && git checkout FRocksDB-6.20.3

git clone https://gitee.com/gooray/snappy.git
#git clone https://github.com/google/snappy.git
cd snappy && git checkout tags/1.1.8 && cd ..
mv snappy snappy-1.1.8 && tar -czvf snappy-1.1.8.tar.gz snappy-1.1.8/ && rm -rf snappy-1.1.8

git clone https://gitee.com/loswdarmy/lz4.git
#git clone https://github.com/lz4/lz4.git
cd lz4 && git checkout tags/v1.9.3 && cd ..
mv lz4 lz4-1.9.3 && tar -czvf lz4-1.9.3.tar.gz lz4-1.9.3/ && rm -rf lz4-1.9.3

git clone https://gitee.com/langxm2006/zlib.git
#git clone https://github.com/madler/zlib.git
cd zlib && git checkout tags/v1.2.13 && cd ..
mv zlib zlib-1.2.13 && tar -czvf zlib-1.2.13.tar.gz zlib-1.2.13/ && rm -rf zlib-1.2.13

git clone https://gitee.com/ak17/bzip2.git
#git clone https://gitlab.com/bzip2/bzip2.git
cd bzip2 && git checkout tags/bzip2-1.0.8 && cd ..
mv bzip2 bzip2-1.0.8 && tar -czvf bzip2-1.0.8.tar.gz bzip2-1.0.8/ && rm -rf bzip2-1.0.8

git clone https://gitee.com/ak17/zstd.git
#git clone https://github.com/facebook/zstd.git
cd zstd && git checkout tags/v1.4.9 && cd ..
mv zstd zstd-1.4.9 && tar -czvf zstd-1.4.9.tar.gz zstd-1.4.9/ && rm -rf zstd-1.4.9

mkdir -p java/test-libs/ && cd java/test-libs/
wget https://repo1.maven.org/maven2/junit/junit/4.13.1/junit-4.13.1.jar
wget https://repo1.maven.org/maven2/org/hamcrest/hamcrest/2.2/hamcrest-2.2.jar
wget https://repo1.maven.org/maven2/org/mockito/mockito-all/1.10.19/mockito-all-1.10.19.jar
wget https://repo1.maven.org/maven2/cglib/cglib/3.3.0/cglib-3.3.0.jar
wget https://repo1.maven.org/maven2/org/assertj/assertj-core/2.9.0/assertj-core-2.9.0.jar
cd ../../

cp -r ../../scripts/Makefile.patch ./
patch -p1 < Makefile.patch

chmod 777 -R build_tools
make -j 8 rocksdbjavastatic
cp -r java/target/librocksdbjni-linux-aarch64.so ../

mkdir build && cd build
cmake .. -DWITH_GFLAGS=OFF -DWITH_JEMALLOC=OFF -DUSE_RTTI=OFF -DWITH_SNAPPY=ON -DWITH_ZLIB=ON -DWITH_LZ4=ON -DWITH_ZSTD=ON -DROCKSDB_BUILD_SHARED=ON -DCMAKE_BUILD_TYPE=Release && make -j8
cd ..
sudo cp -r build/librocksdb.* /usr/local/lib && cp -r build/librocksdb.so.6.20.3 ../librocksdb.so.6
sudo cp -r include/rocksdb /usr/local/include

# step1b: build & install GoogleTest 1.14.0 into /usr/local.
cd ..    # 3rdparty/frocksdb -> 3rdparty
git clone https://gitee.com/hglcode/googletest.git
#git clone https://github.com/google/googletest.git
cd googletest && git checkout tags/v1.14.0
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j8
sudo make install
cd ..    # 3rdparty/googletest/build -> 3rdparty/googletest

# step2: build falcon dynamic library + run cpp unit tests via ctest.
cd ../../cpp && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DFALCON_BUILD_TESTS=ON && make -j8
ctest --output-on-failure
cd .. && cp -r build/libfalcon.so ../3rdparty

# step3: build flink-alg-falcon.jar.
cd ../java && mvn clean package -DskipTests
cp -r target/flink-alg-falcon.jar ../3rdparty
mvn test # step3b: run the java test suite.

# get result files
cd ../3rdparty && mkdir result
mv flink-alg-falcon.jar result/ && mv librocksdbjni-linux-aarch64.so result/ && mv libfalcon.so result/
cd result && jar -xvf flink-alg-falcon.jar && rm -rf flink-alg-falcon.jar
jar -cvf flink-alg-falcon.jar .

# move to output dir
mv flink-alg-falcon.jar ../ && cd ..

touch version.txt
echo "Product Name: Kunpeng BoostKit" >> version.txt
echo "Product Version: ${Product_Version}" >> version.txt
echo "Component Name: BoostKit-omniStateStore" >> version.txt
echo "Component Version: 1.3.0" >> version.txt
zip -r BoostKit-omniruntime-omniStateStore-1.3.0.zip flink-alg-falcon.jar librocksdb.so.6 version.txt
mv BoostKit-omniruntime-omniStateStore-1.3.0.zip ../