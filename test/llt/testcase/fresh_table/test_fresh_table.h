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

#ifndef BOOST_SS_TEST_FRESH_TABLE_H
#define BOOST_SS_TEST_FRESH_TABLE_H

#include "fresh_table.h"
#include "gtest/gtest.h"
#include "include/boost_state_db.h"
#include "include/boost_state_table.h"

using namespace ock::bss;

class TestFreshTable : public testing::Test {
public:
    void SetUp() override;
    void TearDown() override;
    BoostStateDB *mDB;
    FreshTableRef mFreshTable;
    KVTableRef kVTable;
};

#endif  // BOOST_SS_TEST_FRESH_TABLE_H
