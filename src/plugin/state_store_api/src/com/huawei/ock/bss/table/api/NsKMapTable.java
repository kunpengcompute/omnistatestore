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

import com.huawei.ock.bss.table.KeyPair;

/**
 * NsKMapTable
 *
 * @param <K>  key
 * @param <N>  namespace
 * @param <MK> mapKey
 * @param <MV> mapValue
 * @param <M>  map
 * @since BeiMing 25.0.T1
 */
public interface NsKMapTable<K, N, MK, MV, M extends java.util.Map<MK, MV>> extends
    KMapTable<KeyPair<K, N>, MK, MV, M>, TableWithKeyPair<K, N> {
}
