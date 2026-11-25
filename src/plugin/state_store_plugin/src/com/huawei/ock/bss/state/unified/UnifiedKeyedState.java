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

package com.huawei.ock.bss.state.unified;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;

/**
 * UnifiedKeyedState
 *
 * @param <K> key
 * @param <V> value
 * @since 2025年1月14日14:54:16
 */
public abstract class UnifiedKeyedState<K, V> implements InternalKvState<K, VoidNamespace, V> {
    @Override
    public void setCurrentNamespace(VoidNamespace voidNamespace) {
        if (voidNamespace.equals(VoidNamespace.INSTANCE)) {
            return;
        }
        throw new UnsupportedOperationException("setCurrentNamespace is not supported in keyedState.");
    }

    @Override
    public StateIncrementalVisitor<K, VoidNamespace, V> getStateIncrementalVisitor(
        int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException("getStateIncrementalVisitor not supported now.");
    }

    @Override
    public TypeSerializer<VoidNamespace> getNamespaceSerializer() {
        return VoidNamespaceSerializer.INSTANCE;
    }
}
