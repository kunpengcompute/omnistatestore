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

import java.util.Collection;
import java.util.List;

/**
 * KListTable
 *
 * @param <K> key
 * @param <E> element
 * @since BeiMing 25.0.T1
 */
public interface KListTable<K, E> extends KVTable<K, List<E>> {
    /**
     * add
     *
     * @param paramK paramK
     * @param paramE paramE
     */
    void add(K paramK, E paramE);

    /**
     * addAll
     *
     * @param paramK    paramK
     * @param paramList paramList
     */
    void addAll(K paramK, Collection<? extends E> paramList);
}
