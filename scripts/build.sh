#!/bin/bash

# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# omnistatestore is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan
# PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
# KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

# script for build and package waas framework

set -o errtrace
set -o errexit
unset module

usage() {
    local exit_code=${1:-1}
    echo "Usage: $0 [-h | --help] [-t | --type <build_type>] [--fv <flink_version>]"
    echo "          [-j | --jobs <jobs>] [--ut] [--sve]"
    echo "build_type: [debug, release, blend]"
    echo "flink_version: [1.16.1, 1.16.3, 1.17.1, 1.20.0]"
    echo "jobs: positive integer (default: BSS_BUILD_JOBS or 8)"
    echo "Examples:"
    echo " 1 ./build.sh -t release "
    echo " 2 ./build.sh -t debug [--ut] // 限制仅DT构建脚本使用"
    echo " 3 ./build.sh -t release --fv 1.20.0 --jobs 8"
    echo
    exit "${exit_code}"
}

CMAKE_FLAGS=""
BSS_OUTPUT_DIR=BoostKit-omnistatestore_1.1.0
CURRENT_PATH="$(dirname "${BASH_SOURCE[0]}")"
PROJ_DIR="$(realpath "${CURRENT_PATH}/..")"
OUTPUT_DIR=${PROJ_DIR}/dist
BUILD_DIR=${PROJ_DIR}/build
LOG_FILE=${PROJ_DIR}/scripts/build.log

arch=$(uname -m)
BUILD_TYPE=release
BUILD_UT=OFF
FLINK_VERSION=all
BUILD_JOBS=${BSS_BUILD_JOBS:-8}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t | --type )
            [[ $# -lt 2 ]] && echo "Missing value for $1" && usage
            type=$2
            type=${type,,}
            [[ "$type" != "debug" && "$type" != "release" && "$type" != "blend" ]] && echo "Invalid build type $2" && usage
            if [[ "$type" == 'debug' ]]; then
                BUILD_TYPE=debug
                CMAKE_FLAGS+='-DCMAKE_BUILD_TYPE=Debug '
            elif [[ "$type" == 'release' ]]; then
                BUILD_TYPE=release
                CMAKE_FLAGS+='-DCMAKE_BUILD_TYPE=Release '
            elif [[ "$type" == 'blend' ]]; then
                BUILD_TYPE=blend
                CMAKE_FLAGS+='-DCMAKE_BUILD_TYPE=Blend '
            fi
            shift 2
            ;;
        --fv )
            [[ $# -lt 2 ]] && echo "Missing value for $1" && usage
            version=$2
            version="${version// /}"
            [[ "${version}" != "1.16.1" && "${version}" != "1.16.3" && "${version}" != "1.17.1" && \
                "${version}" != "1.20.0" ]] && echo "Invalid flink version $2" && usage
            FLINK_VERSION=${version}
            CMAKE_FLAGS+="-DFLINK_VERSION=${version} "
            shift 2
            ;;
        -j | --jobs )
            [[ $# -lt 2 ]] && echo "Missing value for $1" && usage
            BUILD_JOBS=$2
            shift 2
            ;;
        --ut )
            BUILD_UT=ON
            CMAKE_FLAGS+='-DBUILD_TESTS=ON '
            shift ;;
        --sve )
            CMAKE_FLAGS+='-DBUILD_SVE=ON '
            shift ;;
        -h | --help | -help )
            usage 0
            ;;
        * )
            echo "Unknown argument: $1"
            usage
            ;;
    esac
done

if ! [[ "${BUILD_JOBS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid jobs value: ${BUILD_JOBS}"
    usage
fi

CMAKE_FLAGS+="-DBSS_BUILD_JOBS=${BUILD_JOBS} "

CMAKE_CMD="cmake .. $CMAKE_FLAGS-DCMAKE_INSTALL_PREFIX=../dist"

# $LINENO是发生错误的行号，${FUNCNAME}是发生错误的函数名，$BASH_LINENO是调用该函数的行号
trap 'trap_error $LINENO ${FUNCNAME} $BASH_LINENO' ERR
trap 'cd $PROJ_DIR' EXIT

SUCCESS='[  \033[1;32mOK\033[0;39m  ]'
FAILURE='[\033[1;31mFAILED\033[0;39m]'

function log_info()
{
    if [ $# -lt 1 ]; then
        return
    fi
    echo "$(date +"%F %T") [INFO] $*" >> $LOG_FILE
    echo "$(date +"%F %T") [INFO] $*"
}

function trap_error()
{
    local err=$?
    local lineno=$1
    local funcname=$2
    local bash_lineno=$3

    log_info "Error occurred in function '$funcname' at line $lineno \
        Bash internal line: ${bash_lineno} \
        Command exit status: $err"
}

function echo_failure()
{
    echo -e "bss project : $FAILURE"
}

function build_cmake()
{
  log_info "***** Start build_cmake with dir: ${PWD} *****"
  log_info "CMAKE_CMD = ${CMAKE_CMD}"
  cd build
  # configure
  $CMAKE_CMD
  if [[ "${BUILD_UT}" == "ON" ]]; then
    make -j"${BUILD_JOBS}" build_cpp
  elif [[ "${FLINK_VERSION}" != "all" ]]; then
    make -j"${BUILD_JOBS}" build_version
  else
    make -j"${BUILD_JOBS}" build_all
  fi

  local ret=$?
  cd -

  if [ $ret -ne 0 ]; then
    log_info "build_cmake failed"
    echo_failure
    exit 1
  fi
}

function build_package() {
  if [[ "${BUILD_UT}" == "ON" ]]; then
    return
  fi

  log_info "***** Package jar with dir: ${OUTPUT_DIR} *****"
  cd ${OUTPUT_DIR}
  tar -zcvf BoostKit-omnistatestore_1.1.0_$(arch)_${BUILD_TYPE}.tar.gz ${BSS_OUTPUT_DIR} || {
    echo "Failed to generate BSS dist package!"
    exit 1
  }
}

function parse_args()
{
    build_cmake && build_package
}

function clean() {
  rm -rf ${BUILD_DIR}/*
  rm -rf ${OUTPUT_DIR}/*
}

function check_glibc() {
    MIN_MAJOR=2
    MIN_MINOR=10

    # 可靠获取 glibc 版本
    GLIBC_VERSION=$(getconf GNU_LIBC_VERSION 2>/dev/null | awk '{print $2}')
    if [[ -z "$GLIBC_VERSION" ]]; then
        GLIBC_VERSION=$(ldd --version 2>&1 | awk '/GNU libc/{print $NF; exit}')
    fi

    # 提取主次版本号
    CURRENT_MAJOR=$(echo "$GLIBC_VERSION" | cut -d. -f1)
    CURRENT_MINOR=$(echo "$GLIBC_VERSION" | cut -d. -f2)

    # 检查是否为数字
    if ! [[ "$CURRENT_MAJOR" =~ ^[0-9]+$ ]] || ! [[ "$CURRENT_MINOR" =~ ^[0-9]+$ ]]; then
        echo "Error: Failed to parse Glibc version: $GLIBC_VERSION"
        exit 1
    fi

    # 版本比较
    if [[ "$CURRENT_MAJOR" -lt "$MIN_MAJOR" ]] || \
       [[ "$CURRENT_MAJOR" -eq "$MIN_MAJOR" && "$CURRENT_MINOR" -lt "$MIN_MINOR" ]]; then
        echo "Error: Glibc version must be >= $MIN_MAJOR.$MIN_MINOR. Current version: $GLIBC_VERSION"
        exit 1
    fi
}

#### MAIN ####
echo $(date +"[%Y-%m-%d %H:%M]"): $0 $@

# check glibc version >=2.10
check_glibc

if [ ! -d "${BUILD_DIR}" ]; then
    mkdir -p "${BUILD_DIR}"
fi

# CI_BUILD是一个环境变量
if [ -z "${CI_BUILD}" ];then
    echo "update submodules ... "
    cd $PROJ_DIR && git submodule update --init --recursive
fi

export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF8"
cd ${PROJ_DIR} && clean && parse_args $*
