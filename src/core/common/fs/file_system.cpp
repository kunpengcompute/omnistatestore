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

#include "file_system.h"

#include "common/fs/hdfs/hadoop_file_system.h"
#include "common/fs/local/local_file_system.h"

namespace ock {
namespace bss {
FileSystemRef FileSystem::CreateFileSystem(FileSystemType type, const PathRef &path)
{
    RETURN_NULLPTR_AS_NULLPTR(path);
    std::string scheme = (type == FileSystemType::LOCAL) ? "file" : "hdfs";
    FileSystemRef fileSystem = nullptr;
    // 创建文件系统.
    if (scheme == "file") {  // 本地文件系统.
        fileSystem = std::make_shared<LocalFileSystem>(path);
    } else if (scheme == "hdfs") {  // hdfs远端文件系统.
        fileSystem = std::make_shared<HadoopFileSystem>(path);
    } else {
        LOG_ERROR("Invalid scheme " << scheme);
        return nullptr;
    }
    LOG_DEBUG("Create file system success, scheme:" << scheme << ", path:" << path->ExtractFileName());
    return fileSystem;
}

}  // namespace bss
}  // namespace ock
