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

import java.util.Map;

/**
 * NsKVTable
 *
 * @param <K> key
 * @param <N> namespace
 * @param <V> value
 * @since BeiMing 25.0.T1
 */
public interface NsKVTable<K, N, V> extends KMapTable<K, N, V, Map<N, V>> {
    /**
     * getSerializedValue
     *
     * @param paramK paramK
     * @param paramN paramN
     * @return byte[]
     */
    byte[] getSerializedValue(K paramK, N paramN);
}
