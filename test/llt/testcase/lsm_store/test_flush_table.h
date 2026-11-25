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

#ifndef BOOST_SS_TEST_FLUSH_TABLE_H
#define BOOST_SS_TEST_FLUSH_TABLE_H

#include "test_lsm_store.h"

using namespace ock::bss;

class TestFlushTable : public TestLsmStore {
public:
    void SetUp() override;
};

#endif  // BOOST_SS_TEST_FLUSH_TABLE_H
