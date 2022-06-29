// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <string>
#include <vector>

#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include <mutex>
#include <queue>
#include <set>
#include <unordered_map>

#include "db/db_iter.h"
#include "db/read_callback.h"
#include "db/snapshot_checker.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/transactions/pessimistic_transaction.h"
//#include "utilities/transactions/transaction_lock_mgr.h"
#include "cloud/cloud_lock_mgr.h"
#include "cloud/cloud_transaction.h"
#include "utilities/transactions/write_prepared_txn.h"
#include "utilities/transactions/lock/lock_manager.h"

namespace ROCKSDB_NAMESPACE {
class CloudLockManager;
class CloudEnvImpl;
//
// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class DBCloudImpl : public DBCloud {
  friend DBCloud;

 public:
  DBCloudImpl(DB* db,
                                    const TransactionDBOptions& txn_db_options);

  DBCloudImpl(StackableDB* db,
                                    const TransactionDBOptions& txn_db_options);
  virtual ~DBCloudImpl();
  Status Savepoint() override;

  Status CheckpointToCloud(const BucketOptions& destination,
                           const CheckpointToCloudOptions& options) override;

  Status ExecuteRemoteCompactionRequest(
      const PluggableCompactionParam& inputParams,
      PluggableCompactionResult* result, bool sanitize) override;

  virtual const Snapshot* GetSnapshot() override { return db_->GetSnapshot(); }
  virtual Status Initialize(
      const std::vector<size_t>& compaction_enabled_cf_indices,
      const std::vector<ColumnFamilyHandle*>& handles);

  Transaction* BeginTransaction(const WriteOptions& write_options,
                                const TransactionOptions& txn_options,
                                Transaction* old_txn) override;

  using StackableDB::Put;
  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& val) override;

  using StackableDB::Delete;
  virtual Status Delete(const WriteOptions& wopts,
                        ColumnFamilyHandle* column_family,
                        const Slice& key) override;

  using StackableDB::SingleDelete;
  virtual Status SingleDelete(const WriteOptions& wopts,
                              ColumnFamilyHandle* column_family,
                              const Slice& key) override;

  using StackableDB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) override;

  using DBCloud::Write;
  virtual Status Write(const WriteOptions& opts,
                       const TransactionDBWriteOptimizations& optimizations,
                       WriteBatch* updates) override;
  virtual Status Write(const WriteOptions& opts, WriteBatch* updates) override;
  inline Status WriteWithConcurrencyControl(const WriteOptions& opts,
                                            WriteBatch* updates) {
    // Need to lock all keys in this batch to prevent write conflicts with
    // concurrent transactions.
    Transaction* txn = BeginInternalTransaction(opts);
    txn->DisableIndexing();
    auto txn_impl = static_cast_with_check<CloudTransaction>(txn);
    // Since commitBatch sorts the keys before locking, concurrent Write()
    // operations will not cause a deadlock.
    // In order to avoid a deadlock with a concurrent Transaction, Transactions
    // should use a lock timeout.
    Status s = txn_impl->CommitBatch(updates);
    delete txn;
    return s;
  }
  using StackableDB::CreateColumnFamily;
  virtual Status CreateColumnFamily(const ColumnFamilyOptions& options,
                                    const std::string& column_family_name,
                                    ColumnFamilyHandle** handle) override;

  using StackableDB::DropColumnFamily;
  virtual Status DropColumnFamily(ColumnFamilyHandle* column_family) override;

  Status TryLock(CloudTransaction* txn, uint32_t cfh_id,
                 const std::string& key, bool exclusive);
  Status TryRangeLock(CloudTransaction* txn, uint32_t cfh_id,
                      const Endpoint& start_endp, const Endpoint& end_endp);

  void UnLock(CloudTransaction* txn, const LockTracker& keys);
  void UnLock(CloudTransaction* txn, uint32_t cfh_id,
              const std::string& key);

  void AddColumnFamily(const ColumnFamilyHandle* handle);

  static TransactionDBOptions ValidateTxnDBOptions(
      const TransactionDBOptions& txn_db_options);

  const TransactionDBOptions& GetTxnDBOptions() const {
    return txn_db_options_;
  }

  void InsertExpirableTransaction(TransactionID tx_id,
                                  CloudTransaction* tx);
  void RemoveExpirableTransaction(TransactionID tx_id);

  // If transaction is no longer available, locks can be stolen
  // If transaction is available, try stealing locks directly from transaction
  // It is the caller's responsibility to ensure that the referred transaction
  // is expirable (GetExpirationTime() > 0) and that it is expired.
  bool TryStealingExpiredTransactionLocks(TransactionID tx_id);

  Transaction* GetTransactionByName(const TransactionName& name) override;

  void RegisterTransaction(Transaction* txn);
  void UnregisterTransaction(Transaction* txn);
  // not thread safe. current use case is during recovery (single thread)
  void GetAllPreparedTransactions(std::vector<Transaction*>* trans) override;

  CloudLockManager::PointLockStatus GetLockStatusData() override;

  std::vector<DeadlockPath> GetDeadlockInfoBuffer() override;
  void SetDeadlockInfoBufferSize(uint32_t target_size) override;
  // The default implementation does nothing. The actual implementation is moved
  // to the child classes that actually need this information. This was due to
  // an odd performance drop we observed when the added std::atomic member to
  // the base class even when the subclass do not read it in the fast path.
  virtual void UpdateCFComparatorMap(const std::vector<ColumnFamilyHandle*>&) {}
  virtual void UpdateCFComparatorMap(ColumnFamilyHandle*) {}
  // Use the returned factory to create LockTrackers in transactions.
  const LockTrackerFactory& GetLockTrackerFactory() const {
    return lock_manager_->GetLockTrackerFactory();
  }

 protected:
  // The CloudEnv used by this open instance.
  CloudEnv* cenv_;
  DBImpl* db_impl_;
  std::shared_ptr<Logger> info_log_;
  const TransactionDBOptions txn_db_options_;
  static Status FailIfBatchHasTs(const WriteBatch* wb);

  static Status FailIfCfEnablesTs(const DB* db,
                                  const ColumnFamilyHandle* column_family);

  void ReinitializeTransaction(
      Transaction* txn, const WriteOptions& write_options,
      const TransactionOptions& txn_options = TransactionOptions());
  virtual Status VerifyCFOptions(const ColumnFamilyOptions& cf_options);
  private:
  friend class WritePreparedTxnDB;
  friend class WritePreparedTxnDBMock;
  friend class WriteUnpreparedTxn;
  std::shared_ptr<CloudLockManager> lock_manager_;

  // Must be held when adding/dropping column families.
  InstrumentedMutex column_family_mutex_;
  Transaction* BeginInternalTransaction(const WriteOptions& options);

  // Used to ensure that no locks are stolen from an expirable transaction
  // that has started a commit. Only transactions with an expiration time
  // should be in this map.
  std::mutex map_mutex_;
  std::unordered_map<TransactionID, CloudTransaction*>
      expirable_transactions_map_;

  // map from name to two phase transaction instance
  std::mutex name_map_mutex_;
  std::unordered_map<TransactionName, Transaction*> transactions_;

 private:
  Status DoCheckpointToCloud(const BucketOptions& destination,
                             const CheckpointToCloudOptions& options);

  // Maximum manifest file size
  static const uint64_t max_manifest_file_size = 4 * 1024L * 1024L;

  explicit DBCloudImpl(DB* db);
};
inline Status DBCloudImpl::FailIfBatchHasTs(
    const WriteBatch* batch) {
  if (batch != nullptr && WriteBatchInternal::HasKeyWithTimestamp(*batch)) {
    return Status::NotSupported(
        "Writes with timestamp must go through transaction API instead of "
        "TransactionDB.");
  }
  return Status::OK();
}

inline Status DBCloudImpl::FailIfCfEnablesTs(
    const DB* db, const ColumnFamilyHandle* column_family) {
  assert(db);
  column_family = column_family ? column_family : db->DefaultColumnFamily();
  assert(column_family);
  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  if (ucmp->timestamp_size() > 0) {
    return Status::NotSupported(
        "Write operation with user timestamp must go through the transaction "
        "API instead of TransactionDB.");
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
