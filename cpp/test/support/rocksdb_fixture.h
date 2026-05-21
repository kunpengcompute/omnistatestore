#ifndef FALCON_TESTS_ROCKSDB_FIXTURE_H
#define FALCON_TESTS_ROCKSDB_FIXTURE_H

#include <gtest/gtest.h>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <jni.h>
#include <string>
#include <system_error>

namespace falcon_test {

/**
 * Fixture base that spins up a real RocksDB instance in an mkdtemp temp dir
 * and tears it down on test exit. Falcon helpers reinterpret_cast jlong
 * handles back to `rocksdb::DB*` / `rocksdb::ColumnFamilyHandle*`, so the
 * fixture exposes those handles directly.
 *
 * Hoisted from test_falcon_rocksdb_helper.cpp so the JNI-entry tests
 * (test_falcon_value_state_jni.cpp) and FalconCache fixture can share it.
 */
class RocksDBFixture : public ::testing::Test {
protected:
    void SetUp() override {
        char tmpl[] = "/tmp/falcon_gtest_rocksdb_XXXXXX";
        ASSERT_NE(mkdtemp(tmpl), nullptr) << "mkdtemp failed";
        db_path_ = tmpl;

        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::Status s = rocksdb::DB::Open(options, db_path_, &db_);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    void TearDown() override {
        delete db_;
        db_ = nullptr;
        std::error_code ec;
        std::filesystem::remove_all(db_path_, ec);
    }

    jlong dbHandle()           const { return reinterpret_cast<jlong>(db_); }
    jlong cfHandle()           const { return reinterpret_cast<jlong>(db_->DefaultColumnFamily()); }
    jlong writeOptionsHandle() const { return reinterpret_cast<jlong>(&write_options_); }

    rocksdb::DB* db_{nullptr};
    rocksdb::WriteOptions write_options_{};
    std::string db_path_;
};

}  // namespace falcon_test

#endif  // FALCON_TESTS_ROCKSDB_FIXTURE_H
