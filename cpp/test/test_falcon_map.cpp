#include <gtest/gtest.h>

// Tests target Kunpeng 920 (aarch64 Linux). The build environment is
// expected to provide the right -march so __ARM_FEATURE_SVE is set;
// FalconMap.h hard-#errors otherwise.
#include <arm_sve.h>
#include <rocksdb/slice.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <unordered_set>

#include "FalconMap.h"

namespace {

// 8-byte-aligned heap buffer. Used to make SVE-loop boundary tests
// deterministic about whether the hash's main loop fires — the production
// stride is `svcntd()` 64-bit elements, gated on `num_qwords >= svcntd()`,
// which only holds when the data pointer is already 8-byte aligned (offset
// of the alignment fix-up is 0). std::string's data() pointer alignment is
// implementation-defined, so tests that want guaranteed loop entry must
// allocate explicitly.
struct AlignedBuf {
    char* data{nullptr};
    size_t size{0};

    AlignedBuf(size_t n, char fill) : size(n) {
        // aligned_alloc historically required size to be a multiple of
        // alignment; round up to be portable.
        size_t alloc = (n + 7) & ~size_t{7};
        if (alloc == 0) alloc = 8;
        data = static_cast<char*>(std::aligned_alloc(8, alloc));
        if (n > 0) std::memset(data, fill, n);
    }
    ~AlignedBuf() { std::free(data); }
    AlignedBuf(const AlignedBuf&) = delete;
    AlignedBuf& operator=(const AlignedBuf&) = delete;

    rocksdb::Slice slice() const { return rocksdb::Slice(data, size); }
};

// Compare hashes of two independently-allocated buffers carrying the same
// bytes. This is the right shape for a determinism test — `EXPECT_EQ(h(s),
// h(s))` on the same Slice object passes for any pure function and proves
// nothing.
inline void ExpectSameContentSameHash(rocksdb::Slice a, rocksdb::Slice b) {
    FalconHash<rocksdb::Slice> hasher;
    EXPECT_EQ(hasher(a), hasher(b))
        << "hash differs for two buffers with identical " << a.size()
        << "-byte content";
}

// ============================================================================
// FalconHash tests
// ============================================================================

TEST(FalconHashTest, EmptyKeyHashesToZero) {
    FalconHash<rocksdb::Slice> hasher;
    rocksdb::Slice empty("", 0);
    EXPECT_EQ(hasher(empty), 0u);
}

TEST(FalconHashTest, SameInputProducesSameHash) {
    std::string a = "the quick brown fox jumps over the lazy dog";
    std::string b = a;
    ExpectSameContentSameHash(rocksdb::Slice(a.data(), a.size()),
                              rocksdb::Slice(b.data(), b.size()));
}

// Length = svcntb() - 1: shorter than one full SVE vector, so num_qwords <
// svcntd() and the SVE main loop never executes regardless of alignment.
// Verify content-defined determinism and length-content distinguishability.
TEST(FalconHashTest, BoundaryLength_VectorBytesMinusOne) {
    const size_t N = svcntb() - 1;
    std::string a(N, 'A');
    std::string b(N, 'A');
    ExpectSameContentSameHash(rocksdb::Slice(a.data(), N),
                              rocksdb::Slice(b.data(), N));

    FalconHash<rocksdb::Slice> hasher;
    std::string c(N, 'B');
    EXPECT_NE(hasher(rocksdb::Slice(a.data(), N)),
              hasher(rocksdb::Slice(c.data(), N)));
}

// Length = svcntb() with 8-byte-aligned data: offset == 0, num_qwords ==
// svcntd(), main loop fires exactly once, no tail.
TEST(FalconHashTest, BoundaryLength_VectorBytesExact_Aligned) {
    const size_t N = svcntb();
    AlignedBuf a(N, 'C');
    AlignedBuf b(N, 'C');
    ExpectSameContentSameHash(a.slice(), b.slice());
}

// Length = svcntb() + 1 with aligned data: main loop runs once, then a 1-byte
// CRC tail finalises the hash.
TEST(FalconHashTest, BoundaryLength_VectorBytesPlusOne_Aligned) {
    const size_t N = svcntb() + 1;
    AlignedBuf a(N, 'D');
    AlignedBuf b(N, 'D');
    ExpectSameContentSameHash(a.slice(), b.slice());
}

// Length = 4 * svcntb() with aligned data: the main loop runs four times and
// there is no scalar tail.
TEST(FalconHashTest, MultipleVectorsExact_Aligned) {
    const size_t N = 4 * svcntb();
    AlignedBuf a(N, 'E');
    AlignedBuf b(N, 'E');
    ExpectSameContentSameHash(a.slice(), b.slice());
}

// Length = 4 * svcntb() + 7 with aligned data: vector loop runs four times,
// then a 7-byte CRC tail finalises.
TEST(FalconHashTest, MultipleVectorsPlusTail_Aligned) {
    const size_t N = 4 * svcntb() + 7;
    AlignedBuf a(N, 'F');
    AlignedBuf b(N, 'F');
    ExpectSameContentSameHash(a.slice(), b.slice());
}

// 1024 distinct random 32-byte strings should produce at most ~1% hash
// collisions. Lenient threshold — catches obviously broken hashes (always-
// zero, only-first-byte) but not a hash that loses entropy.
TEST(FalconHashTest, DistributionSanity) {
    FalconHash<rocksdb::Slice> hasher;
    std::mt19937_64 rng(0xDEADBEEFCAFEBABEULL);
    std::uniform_int_distribution<uint8_t> byte_dist(0, 255);

    const int NUM_STRINGS = 1024;
    const int STR_LEN = 32;
    std::vector<std::string> strings(NUM_STRINGS, std::string(STR_LEN, '\0'));
    std::unordered_set<std::size_t> seen;

    for (int i = 0; i < NUM_STRINGS; ++i) {
        for (int j = 0; j < STR_LEN; ++j) {
            strings[i][j] = static_cast<char>(byte_dist(rng));
        }
        rocksdb::Slice s(strings[i].data(), strings[i].size());
        seen.insert(hasher(s));
    }

    const int min_distinct = static_cast<int>(NUM_STRINGS * 0.99);
    EXPECT_GE(static_cast<int>(seen.size()), min_distinct)
        << "Too many hash collisions among 1024 random 32-byte strings";
}

// Length must influence the hash so that "a", "ab", "abc" map to distinct
// values. Subsumes a single-byte determinism check.
TEST(FalconHashTest, LengthDistinguishesShortPrefixes) {
    FalconHash<rocksdb::Slice> hasher;
    rocksdb::Slice sa("a", 1);
    rocksdb::Slice sab("ab", 2);
    rocksdb::Slice sabc("abc", 3);
    EXPECT_NE(hasher(sa), hasher(sab));
    EXPECT_NE(hasher(sab), hasher(sabc));
    EXPECT_NE(hasher(sa), hasher(sabc));
}

// ============================================================================
// FalconEqual tests
// ============================================================================

TEST(FalconEqualTest, IdenticalSlicesAreEqual) {
    FalconEqual<rocksdb::Slice, rocksdb::Slice> eq;
    std::string a(128, 'x');
    std::string b(128, 'x');
    rocksdb::Slice sa(a.data(), a.size());
    rocksdb::Slice sb(b.data(), b.size());
    EXPECT_TRUE(eq(sa, sb));
}

TEST(FalconEqualTest, DifferentLengthsAreNotEqual) {
    FalconEqual<rocksdb::Slice, rocksdb::Slice> eq;
    std::string a = "abc";
    std::string b = "abcd";
    rocksdb::Slice sa(a.data(), a.size());
    rocksdb::Slice sb(b.data(), b.size());
    EXPECT_FALSE(eq(sa, sb));
}

// Both slices are svcntb() bytes long; differ only at byte index 5, inside
// the first SVE-vector compare.
TEST(FalconEqualTest, DifferenceInsideFirstVector) {
    FalconEqual<rocksdb::Slice, rocksdb::Slice> eq;
    const size_t N = svcntb();
    std::string a(N, 'A');
    std::string b(N, 'A');
    b[5] = 'Z';
    EXPECT_FALSE(eq(rocksdb::Slice(a.data(), N), rocksdb::Slice(b.data(), N)));
}

// Length svcntb() + 4: the difference is at byte svcntb() — first byte of
// the trailing scalar memcmp segment in FalconEqual.
TEST(FalconEqualTest, DifferenceAtVectorBoundary) {
    FalconEqual<rocksdb::Slice, rocksdb::Slice> eq;
    const size_t VB = svcntb();
    const size_t N = VB + 4;
    std::string a(N, 'A');
    std::string b(N, 'A');
    b[VB] = 'Z';
    EXPECT_FALSE(eq(rocksdb::Slice(a.data(), N), rocksdb::Slice(b.data(), N)));
}

// Length 2 * svcntb() + 5: two full SVE vectors then a 5-byte memcmp tail;
// difference at the 4th byte of the tail (absolute index 2*VB + 3).
TEST(FalconEqualTest, DifferenceInScalarTail) {
    FalconEqual<rocksdb::Slice, rocksdb::Slice> eq;
    const size_t VB = svcntb();
    const size_t N = 2 * VB + 5;
    std::string a(N, 'B');
    std::string b(N, 'B');
    b[2 * VB + 3] = 'Y';
    EXPECT_FALSE(eq(rocksdb::Slice(a.data(), N), rocksdb::Slice(b.data(), N)));
}

// 8 * svcntb() bytes, all equal: eight iterations of the SVE compare loop,
// no tail.
TEST(FalconEqualTest, LongIdenticalStrings) {
    FalconEqual<rocksdb::Slice, rocksdb::Slice> eq;
    const size_t N = 8 * svcntb();
    std::string a(N, 'C');
    std::string b(N, 'C');
    EXPECT_TRUE(eq(rocksdb::Slice(a.data(), N), rocksdb::Slice(b.data(), N)));
}

// Empty vs non-empty: false via the length-mismatch shortcut.
TEST(FalconEqualTest, OneEmptyOneNonEmpty) {
    FalconEqual<rocksdb::Slice, rocksdb::Slice> eq;
    std::string nonempty(16, 'D');
    rocksdb::Slice sempty("", 0);
    rocksdb::Slice sne(nonempty.data(), nonempty.size());
    EXPECT_FALSE(eq(sempty, sne));
    EXPECT_FALSE(eq(sne, sempty));
}

TEST(FalconEqualTest, BothEmpty_Equal) {
    FalconEqual<rocksdb::Slice, rocksdb::Slice> eq;
    rocksdb::Slice a("", 0);
    rocksdb::Slice b("", 0);
    EXPECT_TRUE(eq(a, b));
}

// 200 random pairs: ~1/3 with mismatched lengths, ~1/3 equal-length-equal-
// content, ~1/3 equal-length-with-flipped-byte. FalconEqual must agree with
// the reference predicate (same length AND memcmp == 0) on every pair.
TEST(FalconEqualTest, EquivalentTo_Memcmp_FuzzedSamples) {
    FalconEqual<rocksdb::Slice, rocksdb::Slice> eq;
    std::mt19937_64 rng(0xFEEDFACEDEADC0DEULL);
    std::uniform_int_distribution<int> len_dist(0, static_cast<int>(4 * svcntb() + 13));
    std::uniform_int_distribution<uint8_t> byte_dist(0, 255);
    // 0 = same content; 1 = different length; 2 = same length, flipped byte.
    std::uniform_int_distribution<int> kind_dist(0, 2);

    int seen_kinds[3] = {0, 0, 0};
    for (int trial = 0; trial < 200; ++trial) {
        const int len_a = len_dist(rng);
        std::string a(len_a, '\0');
        for (int i = 0; i < len_a; ++i) {
            a[i] = static_cast<char>(byte_dist(rng));
        }

        const int kind = kind_dist(rng);
        seen_kinds[kind]++;
        std::string b;
        if (kind == 0) {
            b = a;  // identical
        } else if (kind == 1) {
            // Different length: pick a length that's not equal to len_a.
            int len_b;
            do {
                len_b = len_dist(rng);
            } while (len_b == len_a);
            b.assign(len_b, '\0');
            for (int i = 0; i < len_b; ++i) {
                b[i] = static_cast<char>(byte_dist(rng));
            }
        } else {
            b = a;
            if (len_a > 0) {
                std::uniform_int_distribution<int> pos_dist(0, len_a - 1);
                int pos = pos_dist(rng);
                b[pos] = static_cast<char>(static_cast<uint8_t>(b[pos]) ^ 0xFF);
            } else {
                // Length 0 with "flipped byte" degenerates to same-content;
                // fall through with b == a.
            }
        }

        rocksdb::Slice sa(a.data(), a.size());
        rocksdb::Slice sb(b.data(), b.size());

        const bool expected =
            (a.size() == b.size()) &&
            (a.size() == 0 ||
             std::memcmp(a.data(), b.data(), a.size()) == 0);
        EXPECT_EQ(eq(sa, sb), expected)
            << "Trial " << trial << ": kind=" << kind
            << " len_a=" << a.size() << " len_b=" << b.size();
    }
    // Confirm we actually exercised every kind — guards against future
    // distribution drift silently dropping a category.
    EXPECT_GT(seen_kinds[0], 0);
    EXPECT_GT(seen_kinds[1], 0);
    EXPECT_GT(seen_kinds[2], 0);
}

}  // namespace
