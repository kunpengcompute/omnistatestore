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

/**
 * 非恢复场景
 *
 * @param <K> key
 * @since BeiMing 25.0.T1
 */
public class BoostNoneRestoreOperation<K> extends AbstractBoostRestoreOperation<K> implements BoostRestoreOperation<K> {
    public BoostNoneRestoreOperation() {
    }

    /**
     * 初始化
     *
     * @return BoostRestoreResult
     */
    public BoostRestoreResult restore() {
        // 初始化在创建db实例时完成
        return BoostRestoreResult.EMPTY_RESULT;
    }

    @Override
    public void close() throws Exception {

    }
}
