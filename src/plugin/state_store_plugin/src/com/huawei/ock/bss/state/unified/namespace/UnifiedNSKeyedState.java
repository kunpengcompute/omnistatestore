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

package com.huawei.ock.bss.state.unified.namespace;

import org.apache.flink.runtime.state.internal.InternalKvState;

/**
 * UnifiedNSKeyedState
 *
 * @since 2025年1月14日14:54:35
 */
public abstract class UnifiedNSKeyedState <K, N, V> implements InternalKvState<K, N, V> {
    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException("getStateIncrementalVisitor not supported now.");
    }
}
