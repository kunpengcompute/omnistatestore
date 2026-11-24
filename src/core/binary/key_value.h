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

#ifndef BOOST_SS_KEY_VALUE_H
#define BOOST_SS_KEY_VALUE_H

#include "common/util/iterator.h"
#include "key/key.h"
#include "value/value.h"

namespace ock {
namespace bss {
struct KeyValue : public Referable {
    Key key;
    Value value;

    inline std::string ToString() const
    {
        std::ostringstream oss;
        oss << "Key: {" << key.ToString() << "}, Value: {" << value.ToString() << "}";
        return oss.str();
    }
};

using KeyValueRef = Ref<KeyValue>;
using KeyValueIterator = Iterator<KeyValueRef>;
using KeyValueIteratorRef = std::shared_ptr<KeyValueIterator>;
using KeyValueVectorIterator = VectorIterator<KeyValueRef>;

using KeyValueMap = std::unordered_map<KeyPtr, KeyValueRef, KeyHash, KeyEqual>;  // todo: do we need a fast key
                                                                                 // equal function?
using PriKeyValueMap = std::unordered_map<PriKeyNodePtr, KeyValueRef, PriKeyNodeHash, PriKeyNodeEqual>;
using SecKeyValueMap = std::unordered_map<SecKeyNodePtr, KeyValueRef, SecKeyNodeHash, SecKeyNodeEqual>;
}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_KEY_VALUE_H
