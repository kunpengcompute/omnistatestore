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

package com.huawei.ock.bss.jni;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractNativeReference implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractNativeReference.class);
    /**
      是否close标志位
     */
    protected final AtomicBoolean owningHandle;

    protected AbstractNativeReference(final boolean owningHandle) {
        this.owningHandle = new AtomicBoolean(owningHandle);
    }

    public boolean isNativeHandleClosed() {
        if (!owningHandle.get()) {
            LOG.debug("the native handle already release.");
        }
        return !owningHandle.get();
    }

    @Override
    public void close() {
        LOG.debug("close native reference.");
        if (owningHandle.compareAndSet(true, false)) {
            closeInternal();
        }
    }

    protected abstract void closeInternal();
}
