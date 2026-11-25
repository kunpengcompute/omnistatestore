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

package com.huawei.ock.bss.common.conf;

import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import java.util.Objects;

public class TableConfig {
    private final long ttl;

    private final TtlTimeProvider ttlTimeProvider;

    private final boolean listTtlCompactionEnabled;

    public boolean isListTtlCompactionEnabled() { return listTtlCompactionEnabled; }

    public TtlTimeProvider getTtlTimeProvider() {
        return ttlTimeProvider;
    }

    private final boolean isKVSeparateEnable;

    private TableConfig(long tableTtl, boolean isKVSeparateEnable, TtlTimeProvider ttlTimeProvider, boolean listTtlCompactionEnabled) {
        this.ttl = tableTtl;
        this.isKVSeparateEnable = isKVSeparateEnable;
        this.ttlTimeProvider = ttlTimeProvider;
        this.listTtlCompactionEnabled = listTtlCompactionEnabled;
    }

    public long getTableTtl() {
        return this.ttl;
    }

    public boolean isKVSeparateEnable() {
        return this.isKVSeparateEnable;
    }

    @Override
    public String toString() {
        return "TableConfig {ttl=" + this.ttl + "}";
    }

    public static class Builder {
        private long ttl = -1L;

        private TtlTimeProvider ttlTimeProvider = TtlTimeProvider.DEFAULT;

        private boolean listTtlCompactionEnabled = true;

        public Builder setListTtlCompactionEnabled(boolean enabled) {
            this.listTtlCompactionEnabled = enabled;
            return this;
        }

        /** When native TTL filtering is enabled, only {@link TtlTimeProvider#DEFAULT} is supported. */
        public Builder setTtlTimeProvider(TtlTimeProvider provider) {
            this.ttlTimeProvider = Objects.requireNonNull(provider);
            return this;
        }

        private boolean isKVSeparateEnable;

        public Builder setTableTtl(long ttl) {
            this.ttl = ttl;
            return this;
        }

        public Builder setIsEnableKVSeparate(boolean enable) {
            this.isKVSeparateEnable = enable;
            return this;
        }

        public TableConfig build() {
            return new TableConfig(this.ttl, this.isKVSeparateEnable, this.ttlTimeProvider, this.listTtlCompactionEnabled);
        }
    }
}
