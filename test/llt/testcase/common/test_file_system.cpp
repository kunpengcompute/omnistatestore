/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistatestore is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

#include "test_file_system.h"

namespace ock {
namespace bss {
namespace test {
TEST_F(TestFileSystem, test_file_system_write)
{
    auto *tempBuf = new (std::nothrow) uint8_t[NO_128];
    ASSERT_EQ(fileSystem->Write(tempBuf, NO_10), BSS_OK);
    delete tempBuf;
}

TEST_F(TestFileSystem, test_file_system_write_with_offset)
{
    auto *tempBuf = new (std::nothrow) uint8_t[NO_128];
    ASSERT_EQ(fileSystem->Write(tempBuf, NO_10, NO_0), BSS_OK);
    delete tempBuf;
}

TEST_F(TestFileSystem, test_file_system_sync)
{
    ASSERT_EQ(fileSystem->Sync(), BSS_OK);
}

TEST_F(TestFileSystem, test_file_system_flush)
{
    ASSERT_EQ(fileSystem->Flush(), BSS_OK);
}

}  // namespace test
}  // namespace bss
}  // namespace ock