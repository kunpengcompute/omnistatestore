#ifndef BOOST_SS_FRESH_RESTORE_MEMORY_H
#define BOOST_SS_FRESH_RESTORE_MEMORY_H

#include <cstdint>

#include "memory/memory_segment.h"

namespace ock {
namespace bss {
BResult AllocateFreshRestoreCompressedSegment(uint32_t length, MemorySegmentRef &segment);
}  // namespace bss
}  // namespace ock

#endif  // BOOST_SS_FRESH_RESTORE_MEMORY_H
