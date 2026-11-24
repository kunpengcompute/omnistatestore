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

package com.huawei.ock.bss.restore;

import org.apache.flink.runtime.state.RestoreOperation;

/**
 * 恢复
 *
 * @since 2025年1月14日14:19:33
 */
public interface BoostRestoreOperation<K> extends RestoreOperation<BoostRestoreResult>, AutoCloseable {
    /**
     * 恢复
     *
     * @return BoostRestoreResult
     * @throws Exception exception
     */
    BoostRestoreResult restore() throws Exception;
}
