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

#ifndef BOOST_SS_LAZYDOWNLOADTASK_H
#define BOOST_SS_LAZYDOWNLOADTASK_H

#include "executor_service.h"
#include "file_holder.h"
#include "snapshot_downloader.h"

namespace ock {
namespace bss {
class LazyDownloadTask : public Runnable {
public:
    LazyDownloadTask(const std::vector<FileHolderRef> &fileHolders,
                     const std::function<void(std::vector<FileHolderRef> &innerFileHolders)> &function)
        : mFileHolders(fileHolders), mFunction(function)
    {
    }

    void Run() override
    {
        mFunction(mFileHolders);
    }

private:
    std::vector<FileHolderRef> mFileHolders;
    std::function<void(std::vector<FileHolderRef> &fileHolders)> mFunction;
};
using LazyDownloadTaskRef = std::shared_ptr<LazyDownloadTask>;

}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_LAZYDOWNLOADTASK_H