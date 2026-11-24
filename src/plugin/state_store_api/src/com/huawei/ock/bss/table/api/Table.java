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

package com.huawei.ock.bss.table.api;

import com.huawei.ock.bss.common.conf.TableConfig;

import java.util.stream.Stream;

/**
 * Table interface
 *
 * @since BeiMing 25.0.T1
 */
public interface Table<K> {
    /**
     * getTableDescription
     *
     * @return com.huawei.ock.bss.table.api.TableDescription
     */
    TableDescription getTableDescription();

    /**
     * getKeys
     *
     * @param paramN paramN
     * @return java.util.stream.Stream<PK>
     */
    <N, PK> Stream<PK> getKeys(N paramN);

    /**
     * updateTableConfig
     *
     * @param paramTableConfig paramTableConfig
     */
    void updateTableConfig(TableConfig paramTableConfig);

    /**
     * 数据迁移，从oldTable中读取所有数据写入新表
     *
     * @param oldTable restore的表
     */
    void migrateValue(Table<K> oldTable);
}
