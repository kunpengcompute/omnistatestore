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

# Run OmniStateStore LLT repeatedly while preserving reproducible evidence.

set -o errexit
set -o nounset
set -o pipefail

usage()
{
    local exit_code=${1:-1}
    echo "Usage: $0 [--mode focused|full] [--filter <gtest_filter>] [--repeat <count>]"
    echo "          [--timeout <duration>] [--binary <path>] [--output-dir <path>]"
    echo "Focused mode defaults to the remediation regression tests and verifies that all are present in XML."
    echo "Full mode rejects --filter. Timeout defaults to 30m."
    exit "${exit_code}"
}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SOURCE_DIR=${BSS_SOURCE_DIR:-$(realpath "${SCRIPT_DIR}/..")}
MODE=full
FILTER=""
REPEAT=1
TIMEOUT=30m
BINARY_PATH="${SOURCE_DIR}/build/test/llt/bss_ut"
RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
OUTPUT_DIR="${SOURCE_DIR}/build/test-evidence/${RUN_ID}"
OUTPUT_DIR_EXPLICIT=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            [[ $# -lt 2 ]] && echo "Missing value for $1" && usage
            MODE=$2
            shift 2
            ;;
        --filter)
            [[ $# -lt 2 ]] && echo "Missing value for $1" && usage
            FILTER=$2
            shift 2
            ;;
        --repeat)
            [[ $# -lt 2 ]] && echo "Missing value for $1" && usage
            REPEAT=$2
            shift 2
            ;;
        --timeout)
            [[ $# -lt 2 ]] && echo "Missing value for $1" && usage
            TIMEOUT=$2
            shift 2
            ;;
        --binary)
            [[ $# -lt 2 ]] && echo "Missing value for $1" && usage
            BINARY_PATH=$2
            shift 2
            ;;
        --output-dir)
            [[ $# -lt 2 ]] && echo "Missing value for $1" && usage
            OUTPUT_DIR=$2
            OUTPUT_DIR_EXPLICIT=true
            shift 2
            ;;
        -h | --help)
            usage 0
            ;;
        *)
            echo "Unknown argument: $1"
            usage
            ;;
    esac
done

[[ "${MODE}" != "focused" && "${MODE}" != "full" ]] && echo "Invalid mode: ${MODE}" && usage
if [[ "${MODE}" == "full" && -n "${FILTER}" ]]; then
    echo "Full mode does not accept --filter"
    usage
fi
if ! [[ "${TIMEOUT}" =~ ^[1-9][0-9]*([smhd])?$ ]]; then
    echo "Invalid timeout: ${TIMEOUT}"
    usage
fi

FOCUSED_TESTS=(
    "ConfigTest.DefaultConstructorDisablesLocalRecovery"
    "ConfigTest.ParameterizedConstructorDisablesLocalRecovery"
    "ConfigTest.ExplicitlyEnablesLocalRecovery"
    "TestDB.SameTaskSlotDifferentMaxParallelismKeepsDbACodec"
)
if [[ "${MODE}" == "focused" && -z "${FILTER}" ]]; then
    FILTER=$(IFS=:; echo "${FOCUSED_TESTS[*]}")
fi
if ! [[ "${REPEAT}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid repeat count: ${REPEAT}"
    usage
fi

SOURCE_DIR=$(realpath "${SOURCE_DIR}")
BINARY_PATH=$(realpath -m "${BINARY_PATH}")
OUTPUT_DIR=$(realpath -m "${OUTPUT_DIR}")
[[ ! -d "${SOURCE_DIR}/.git" && ! -f "${SOURCE_DIR}/.git" ]] && echo "Not a Git worktree: ${SOURCE_DIR}" && exit 1
[[ ! -x "${BINARY_PATH}" ]] && echo "Test binary is not executable: ${BINARY_PATH}" && exit 1
command -v timeout >/dev/null || { echo "GNU timeout is required"; exit 1; }

mkdir -p "$(dirname "${OUTPUT_DIR}")"
if [[ "${OUTPUT_DIR_EXPLICIT}" == "true" ]]; then
    [[ -e "${OUTPUT_DIR}" ]] && echo "Output directory already exists: ${OUTPUT_DIR}" && exit 1
    mkdir "${OUTPUT_DIR}"
else
    OUTPUT_DIR=$(mktemp -d "${OUTPUT_DIR}.XXXXXX")
fi
git -C "${SOURCE_DIR}" rev-parse HEAD > "${OUTPUT_DIR}/source_commit.txt"
git -C "${SOURCE_DIR}" rev-parse 'HEAD^{tree}' > "${OUTPUT_DIR}/source_tree.txt"
git -C "${SOURCE_DIR}" status --short > "${OUTPUT_DIR}/source_status.txt"
git -C "${SOURCE_DIR}" diff --binary HEAD > "${OUTPUT_DIR}/source.patch"
sha256sum "${OUTPUT_DIR}/source.patch" > "${OUTPUT_DIR}/source_patch.sha256"

(
    cd "${SOURCE_DIR}"
    git ls-files --cached --others --exclude-standard | LC_ALL=C sort | while IFS= read -r source_file; do
        if [[ -f "${source_file}" ]]; then
            sha256sum "${source_file}"
        fi
    done
) > "${OUTPUT_DIR}/source_files.sha256"
sha256sum "${OUTPUT_DIR}/source_files.sha256" > "${OUTPUT_DIR}/source_files_manifest.sha256"
sha256sum "${BINARY_PATH}" > "${OUTPUT_DIR}/binary.sha256"

{
    echo "utc_started=${RUN_ID}"
    echo "mode=${MODE}"
    echo "filter=${FILTER}"
    echo "repeat=${REPEAT}"
    echo "timeout=${TIMEOUT}"
    echo "binary=${BINARY_PATH}"
    echo "source_dir=${SOURCE_DIR}"
    echo "output_dir=${OUTPUT_DIR}"
} > "${OUTPUT_DIR}/run_metadata.txt"

printf "run\texit_code\txml\tlog\n" > "${OUTPUT_DIR}/runs.tsv"
BINARY_DIR=$(dirname "${BINARY_PATH}")
BINARY_NAME=$(basename "${BINARY_PATH}")

for ((run = 1; run <= REPEAT; run++)); do
    run_name=$(printf "run-%03d" "${run}")
    log_path="${OUTPUT_DIR}/${run_name}.log"
    xml_path="${OUTPUT_DIR}/${run_name}.xml"
    exit_path="${OUTPUT_DIR}/${run_name}.exit_code"
    command_path="${OUTPUT_DIR}/${run_name}.command"
    command=("./${BINARY_NAME}" "--gtest_output=xml:${xml_path}")
    if [[ "${MODE}" == "focused" ]]; then
        command+=("--gtest_filter=${FILTER}")
    fi
    execution_command=(timeout --foreground "${TIMEOUT}" "${command[@]}")
    printf '%q ' "${execution_command[@]}" > "${command_path}"
    printf '\n' >> "${command_path}"

    set +o errexit
    (cd "${BINARY_DIR}" && "${execution_command[@]}") > "${log_path}" 2>&1
    exit_code=$?
    set -o errexit
    echo "${exit_code}" > "${exit_path}"
    printf "%s\t%s\t%s\t%s\n" "${run_name}" "${exit_code}" "${xml_path}" "${log_path}" >> "${OUTPUT_DIR}/runs.tsv"

    if [[ ${exit_code} -ne 0 ]]; then
        if [[ ${exit_code} -eq 124 ]]; then
            echo "${run_name} timed out after ${TIMEOUT}; see ${log_path}" >&2
            exit 124
        fi
        echo "${run_name} failed with exit code ${exit_code}; see ${log_path}" >&2
        exit "${exit_code}"
    fi
    if [[ ! -s "${xml_path}" ]]; then
        echo "${run_name} did not produce a non-empty XML report: ${xml_path}" >&2
        exit 1
    fi

    if ! python3 - "${xml_path}" "${MODE}" "${FOCUSED_TESTS[@]}" \
        > "${OUTPUT_DIR}/${run_name}.xml_check.txt" <<'PY'
import sys
import xml.etree.ElementTree as ET

xml_path = sys.argv[1]
mode = sys.argv[2]
required_tests = set(sys.argv[3:])
root = ET.parse(xml_path).getroot()
if root.tag == "testsuites":
    tests = int(root.attrib.get("tests", "0"))
    failures = int(root.attrib.get("failures", "0"))
    errors = int(root.attrib.get("errors", "0"))
else:
    suites = list(root.iter("testsuite"))
    tests = sum(int(suite.attrib.get("tests", "0")) for suite in suites)
    failures = sum(int(suite.attrib.get("failures", "0")) for suite in suites)
    errors = sum(int(suite.attrib.get("errors", "0")) for suite in suites)
print(f"tests={tests} failures={failures} errors={errors}")
executed_tests = {
    f"{case.attrib.get('classname', '')}.{case.attrib.get('name', '')}"
    for case in root.iter("testcase")
}
missing_tests = sorted(required_tests - executed_tests) if mode == "focused" else []
if missing_tests:
    print("missing_required_tests=" + ",".join(missing_tests))
if tests == 0 or failures != 0 or errors != 0 or missing_tests:
    raise SystemExit(1)
PY
    then
        echo "${run_name} XML reports zero tests, failures, errors, or invalid XML: ${xml_path}" >&2
        exit 1
    fi
done

echo "All ${REPEAT} run(s) passed. Evidence: ${OUTPUT_DIR}"
