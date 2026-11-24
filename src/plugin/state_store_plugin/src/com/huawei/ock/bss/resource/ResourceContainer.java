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

package com.huawei.ock.bss.resource;

/**
 * ResourceContainer类
 *
 * @since 2025年1月12日16:26:41
 */
public class ResourceContainer implements AutoCloseable {
    /**
     * 打桩类
     */
    public static final ResourceContainer INSTANCE = new ResourceContainer();

    @Override
    public void close() throws Exception {

    }
}
