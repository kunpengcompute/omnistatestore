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

#ifndef BOOST_SS_SNAPSHOT_DOWNLOADER_H
#define BOOST_SS_SNAPSHOT_DOWNLOADER_H

#include <jni.h>

#include <memory>

#include "common/path_transform.h"
#include "fs/hdfs/hadoop_file_system.h"
#include "lsm_store/file/file_address_util.h"

namespace ock {
namespace bss {

class SnapshotDownloader {
public:
    BResult static DownloadFile(const std::string &remotePath, const std::string &localPath)
    {
        PathRef path = std::make_shared<Path>(remotePath);
        if (UNLIKELY(path == nullptr)) {
            LOG_ERROR("Failed to make sharedPtr for Path, remotePath: " << PathTransform::ExtractFileName(remotePath));
            return BSS_ERR;
        }
        HadoopFileSystemRef fileSystem = std::make_shared<HadoopFileSystem>(path);
        if (UNLIKELY(fileSystem == nullptr)) {
            LOG_ERROR("Failed to make sharedPtr for HadoopFileSystem, remotePath: "
                      << PathTransform::ExtractFileName(remotePath));
            return BSS_ERR;
        }
        auto ret = fileSystem->Open(0);
        if (UNLIKELY(ret != BSS_OK)) {
            LOG_ERROR("Failed to open HadoopFileSystem, remotePath: " << PathTransform::ExtractFileName(remotePath));
            return ret;
        }
        return fileSystem->Download(std::make_shared<Path>(localPath));
    }
};

}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_SNAPSHOT_DOWNLOADER_H