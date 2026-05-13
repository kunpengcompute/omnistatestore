cmake_minimum_required(VERSION 3.5.1)
project(falcon VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON) # use -f PIC to build jni

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv8.2-a+sve2+crc -D__ARM_FEATURE_CRC32=1 -D__ARM_FEATURE_SVE=1")

# set include path, include falcon header files, and java、rocksdb header files
find_package(JNI REQUIRED)
if(NOT JNI_FOUND)
    message(FATAL_ERROR "JNI not found. Set JAVA_HOME environment variable.")
endif()
set(ROCKSDB_INCLUDE_PATH "/usr/local/include/")
set(ROCKSDB_LIBRARY_PATH "/usr/local/lib/librocksdb.so")
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include        # Falcon C++ header files
    ${CMAKE_CURRENT_SOURCE_DIR}/jni_headers    # Falcon JNI header files given by java
    ${ROCKSDB_INCLUDE_PATH}                    # RocksDB header file path
    ${JNI_INCLUDE_DIRS}                        # java header files
)

# set files to be built
add_library(falcon SHARED
    src/cache/FalconCache.cpp
    src/cache/FalconValueStateJNI.cpp
    src/cache/FalconRocksDBHelper.cpp
)

# link rocksdb library and jvm library. avoid linking JNI_LIBRARY, which contains libwat.so and so on.
target_link_libraries(falcon ${ROCKSDB_LIBRARY_PATH} ${JAVA_JVM_LIBRARY})

# Optional GoogleTest-based unit tests. Enable with `-DFALCON_BUILD_TESTS=ON`.
option(FALCON_BUILD_TESTS "Build Falcon C++ unit tests" OFF)
if(FALCON_BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()