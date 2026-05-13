cmake_minimum_required(VERSION 3.5.1)

# ----------------------------------------------------------------------------
# GoogleTest.
# ----------------------------------------------------------------------------
find_package(GTest REQUIRED)
include(GoogleTest)
enable_testing()

# ----------------------------------------------------------------------------
# RocksDB
# ----------------------------------------------------------------------------
set(FALCON_TEST_ROCKSDB_INCLUDE_DIR "/usr/local/include" CACHE PATH "RocksDB include dir for tests")
set(FALCON_TEST_ROCKSDB_LIBRARY "/usr/local/lib/librocksdb.so" CACHE FILEPATH "RocksDB library for tests")

# ----------------------------------------------------------------------------
# Falcon test binary.
# ----------------------------------------------------------------------------
set(FALCON_SOURCES_UNDER_TEST
    ${CMAKE_SOURCE_DIR}/src/cache/FalconCache.cpp
    ${CMAKE_SOURCE_DIR}/src/cache/FalconRocksDBHelper.cpp
    # Tier-2 coverage: include the JNI entry layer so test_falcon_value_state_jni
    # can call Java_* symbols. JNIEXPORT / JNICALL are pure visibility attributes
    # and don't interfere with the test binary.
    ${CMAKE_SOURCE_DIR}/src/cache/FalconValueStateJNI.cpp
)
# Glob test sources + support helpers
# CONFIGURE_DEPENDS makes CMake re-glob on every build.
file(GLOB FALCON_TEST_SUPPORT_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/support/*.cpp")
file(GLOB FALCON_TEST_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/test_*.cpp")

add_executable(falcon_test
    ${FALCON_SOURCES_UNDER_TEST}
    ${FALCON_TEST_SUPPORT_SOURCES}
    ${FALCON_TEST_SOURCES}
)

# FalconMap.h uses ARM SVE intrinsics (svld1, svwhilelt, etc.). The test
# target compiles FalconCache.cpp which transitively includes FalconMap.h,
# so we must enable SVE here.
target_compile_options(falcon_test PRIVATE -march=armv8.2-a+sve)

# Optional coverage instrumentation (gcov/lcov). Enable with
# `-DFALCON_TEST_COVERAGE=ON`. Authoritative numbers come from aarch64 Linux
# CI; local macOS builds use a FalconMap shim and should exclude that header
# from any coverage report.
option(FALCON_TEST_COVERAGE "Enable gcov/lcov coverage instrumentation" OFF)
if(FALCON_TEST_COVERAGE)
    target_compile_options(falcon_test PRIVATE --coverage -fprofile-arcs -ftest-coverage -O0 -g)
    target_link_options(falcon_test PRIVATE --coverage)
endif()

target_include_directories(falcon_test PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/jni_headers
    ${FALCON_TEST_ROCKSDB_INCLUDE_DIR}
    ${JNI_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/support
)

target_link_libraries(falcon_test PRIVATE
    GTest::gtest
    GTest::gtest_main
    ${FALCON_TEST_ROCKSDB_LIBRARY}
    ${JAVA_JVM_LIBRARY}
)

gtest_discover_tests(falcon_test)
