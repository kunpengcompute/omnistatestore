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

package com.huawei.ock.bss.common.exception;

public class BSSRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 8596437539536367377L;

    public BSSRuntimeException() {
    }

    public BSSRuntimeException(String message) {
        super(message);
    }

    public BSSRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public BSSRuntimeException(Throwable cause) {
        super(cause);
    }
}
