//  Copyright (c) 2017-present, Rockset

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <vector>

#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/utilities/transaction_db.h"

namespace ROCKSDB_NAMESPACE {

//
// Database with Cloud support.
//
// Important: The caller is responsible for ensuring that only one database at
// a time is running with the same cloud destination bucket and path. Running
// two databases concurrently with the same destination path will lead to
// corruption if it lasts for more than couple of minutes.
class DBCloud : public StackableDB {
 public:
  // This API is to open a DB when key-values are to be made durable by
  // backing up database state into a cloud-storage system like S3.
  // All kv updates are persisted in cloud-storage.
  // options.env is an object of type ROCKSDB_NAMESPACE::CloudEnv and the cloud
  // buckets are specified there.
  static Status Open(const Options& options, const std::string& name,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb, DBCloud** dbptr,
                     bool read_only = false);

  // This is for advanced users who can comprehend column families.
  // If you want sst files from S3 to be cached in local SSD/disk, then
  // persistent_cache_path should be the pathname of the local
  // cache storage.
  // TODO(igor/dhruba) The first argument here should be DBOptions, just like in
  // DB class.
  static Status Open(const Options& options, const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb,
                     std::vector<ColumnFamilyHandle*>* handles, DBCloud** dbptr,
                     bool read_only = false);

  // Synchronously copy all relevant files (if any) from source cloud storage to
  // destination cloud storage.
  virtual Status Savepoint() = 0;

  // Synchronously copy all local files to the cloud destination given by
  // 'destination' parameter.
  // Important: This will overwrite the database in 'destination', if any.
  // This feature should be considered experimental.
  virtual Status CheckpointToCloud(const BucketOptions& destination,
                                   const CheckpointToCloudOptions& options) = 0;

  // Executes an external compaction request on this cloud  database.
  // The output pathnames returned in PluggableCompactionResult are the
  // cloud path names.
  virtual Status ExecuteRemoteCompactionRequest(
      const PluggableCompactionParam& inputParams,
      PluggableCompactionResult* result, bool sanitize) = 0;

  // ListColumnFamilies will open the DB specified by argument name
  // and return the list of all column families in that DB
  // through column_families argument. The ordering of
  // column families in column_families is unspecified.
  static Status ListColumnFamilies(const DBOptions& db_options,
                                   const std::string& name,
                                   std::vector<std::string>* column_families);

  virtual ~DBCloud() {}
  // Open a TransactionDB similar to DB::Open().
  // Internally call PrepareWrap() and WrapDB()

  using StackableDB::Write;
  virtual Status Write(const WriteOptions& opts,
                       const TransactionDBWriteOptimizations&,
                       WriteBatch* updates) {
    // The default implementation ignores TransactionDBWriteOptimizations and
    // falls back to the un-optimized version of ::Write
    return Write(opts, updates);
  }
  // Transactional `DeleteRange()` is not yet supported.
  // However, users who know their deleted range does not conflict with
  // anything can still use it via the `Write()` API. In all cases, the
  // `Write()` overload specifying `TransactionDBWriteOptimizations` must be
  // used and `skip_concurrency_control` must be set. When using either
  // WRITE_PREPARED or WRITE_UNPREPARED , `skip_duplicate_key_check` must
  // additionally be set.
  using StackableDB::DeleteRange;
  virtual Status DeleteRange(const WriteOptions&, ColumnFamilyHandle*,
                             const Slice&, const Slice&) override {
    return Status::NotSupported();
  }
  static Status Open(const Options& options, const TransactionDBOptions& txn_db_options, const std::string& name,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb, DBCloud** dbptr,
                     bool read_only = false);

  static Status Open(const Options& options, const TransactionDBOptions& txn_db_options, const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb,
                     std::vector<ColumnFamilyHandle*>* handles, DBCloud** dbptr,
                     bool read_only = false);
  // The following functions are used to open a TransactionDB internally using
  // an opened DB or StackableDB.
  // 1. Call prepareWrap(), passing an empty std::vector<size_t> to
  // compaction_enabled_cf_indices.
  // 2. Open DB or Stackable DB with db_options and column_families passed to
  // prepareWrap()
  // Note: PrepareWrap() may change parameters, make copies before the
  // invocation if needed.
  // 3. Call Wrap*DB() with compaction_enabled_cf_indices in step 1 and handles
  // of the opened DB/StackableDB in step 2
  static void PrepareWrap(DBOptions* db_options,
                          std::vector<ColumnFamilyDescriptor>* column_families,
                          std::vector<size_t>* compaction_enabled_cf_indices);
  static Status WrapDB(DB* db, const TransactionDBOptions& txn_db_options,
                       const std::vector<size_t>& compaction_enabled_cf_indices,
                       const std::vector<ColumnFamilyHandle*>& handles,
                       DBCloud** dbptr);
  static Status WrapStackableDB(
      StackableDB* db, const TransactionDBOptions& txn_db_options,
      const std::vector<size_t>& compaction_enabled_cf_indices,
      const std::vector<ColumnFamilyHandle*>& handles, DBCloud** dbptr);

  // Starts a new Transaction.
  //
  // Caller is responsible for deleting the returned transaction when no
  // longer needed.
  //
  // If old_txn is not null, BeginTransaction will reuse this Transaction
  // handle instead of allocating a new one.  This is an optimization to avoid
  // extra allocations when repeatedly creating transactions.
  virtual Transaction* BeginTransaction(
      const WriteOptions& write_options,
      const TransactionOptions& txn_options = TransactionOptions(),
      Transaction* old_txn = nullptr) = 0;

  virtual Transaction* GetTransactionByName(const TransactionName& name) = 0;
  virtual void GetAllPreparedTransactions(std::vector<Transaction*>* trans) = 0;

  // Returns set of all locks held.
  //
  // The mapping is column family id -> KeyLockInfo
  virtual std::unordered_multimap<uint32_t, KeyLockInfo>
  GetLockStatusData() = 0;
  virtual std::vector<DeadlockPath> GetDeadlockInfoBuffer() = 0;
  virtual void SetDeadlockInfoBufferSize(uint32_t target_size) = 0;

 protected:
  explicit DBCloud(DB* db) : StackableDB(db) {}
  // No copying allowed
  DBCloud(const DBCloud&) = delete;
  void operator=(const DBCloud&) = delete;
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
