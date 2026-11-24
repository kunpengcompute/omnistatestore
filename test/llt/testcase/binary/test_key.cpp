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

#include "binary/key/key.h"
#include "gtest/gtest.h"

using namespace ock::bss;

TEST(TestKey, test_key_size)
{
    /**
     * NOTICE: Align according to the size of the CPU Cache Line, which is 64 bytes. The key cannot exceed 64 bytes!!!
     */
    ASSERT_EQ(sizeof(Key), NO_64 /*CAN'T MODIFY THIS VALUE!!!*/);
}