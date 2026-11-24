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

package com.huawei.ock.bss.state.internal.descriptor.keyed;

import com.huawei.ock.bss.state.internal.descriptor.InternalStateType;
import com.huawei.ock.bss.state.internal.KeyedValueState;

import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * KeyedValueStateDescriptor
 *
 * @param <K> key
 * @param <V> value
 * @since 2025年1月14日14:55:063
 */
public class KeyedValueStateDescriptor<K, V> extends KeyedStateDescriptor<K, V, KeyedValueState<K, V>> {
    private static final long serialVersionUID = 1L;

    public KeyedValueStateDescriptor(String name, TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer,
        InternalStateType stateType) {
        super(name, stateType, keySerializer, valueSerializer);
    }

    /**
     * 返回一个新的相同descriptor对象
     *
     * @return 复制对象
     */
    @Override
    public KeyedValueStateDescriptor<K, V> duplicate() {
        KeyedValueStateDescriptor<K, V> descriptor = new KeyedValueStateDescriptor<>(getName(),
            getKeySerializer().duplicate(), getValueSerializer().duplicate(), getStateType());
        descriptor.setStateTtlConfig(this.ttlConfig);
        return descriptor;
    }
}
