// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/types.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/transactions/lock/lock_tracker.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "cloud/cloud_transaction.h"
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "monitoring/instrumented_mutex.h"
#include "util/autovector.h"
#include "util/hash_map.h"
#include "util/thread_local.h"
#include "utilities/transactions/lock/lock_manager.h"
#include "utilities/transactions/lock/point/point_lock_tracker.h"
#include "utilities/transactions/lock/point/point_lock_manager.h"

namespace ROCKSDB_NAMESPACE {

class DBCloudImpl;
class ColumnFamilyHandle;
struct LockInfo;
struct LockMap;
struct LockMapStripe;
class CloudLockManager;


class CloudLockManager {
 public:
  virtual ~CloudLockManager() {}

  // Whether supports locking a specific key.
  virtual bool IsPointLockSupported() const = 0;

  // Whether supports locking a range of keys.
  virtual bool IsRangeLockSupported() const = 0;

  // Locks acquired through this LockManager should be tracked by
  // the LockTrackers created through the returned factory.
  virtual const LockTrackerFactory& GetLockTrackerFactory() const = 0;

  // Enable locking for the specified column family.
  // Caller should guarantee that this column family is not already enabled.
  virtual void AddColumnFamily(const ColumnFamilyHandle* cf) = 0;

  // Disable locking for the specified column family.
  // Caller should guarantee that this column family is no longer used.
  virtual void RemoveColumnFamily(const ColumnFamilyHandle* cf) = 0;

  // Attempt to lock a key or a key range.  If OK status is returned, the caller
  // is responsible for calling UnLock() on this key.
  virtual Status TryLock(CloudTransaction* txn,
                         ColumnFamilyId column_family_id,
                         const std::string& key, Env* env, bool exclusive) = 0;
  // The range [start, end] are inclusive at both sides.
  virtual Status TryLock(CloudTransaction* txn,
                         ColumnFamilyId column_family_id, const Endpoint& start,
                         const Endpoint& end, Env* env, bool exclusive) = 0;

  // Unlock a key or a range locked by TryLock().  txn must be the same
  // Transaction that locked this key.
  virtual void UnLock(CloudTransaction* txn, const LockTracker& tracker,
                      Env* env) = 0;
  virtual void UnLock(CloudTransaction* txn,
                      ColumnFamilyId column_family_id, const std::string& key,
                      Env* env) = 0;
  virtual void UnLock(CloudTransaction* txn,
                      ColumnFamilyId column_family_id, const Endpoint& start,
                      const Endpoint& end, Env* env) = 0;

  using PointLockStatus = std::unordered_multimap<ColumnFamilyId, KeyLockInfo>;
  virtual PointLockStatus GetPointLockStatus() = 0;

  using RangeLockStatus =
      std::unordered_multimap<ColumnFamilyId, RangeLockInfo>;
  virtual RangeLockStatus GetRangeLockStatus() = 0;

  virtual std::vector<DeadlockPath> GetDeadlockInfoBuffer() = 0;

  virtual void Resize(uint32_t new_size) = 0;
};

// LockManager should always be constructed through this factory method,
// instead of constructing through concrete implementations' constructor.
// Caller owns the returned pointer.
std::shared_ptr<CloudLockManager> NewLockManager(DBCloudImpl* db,
                                            const TransactionDBOptions& opt);

class PointCloudLockManager : public CloudLockManager {
 public:
  PointCloudLockManager(DBCloudImpl* db,
                   const TransactionDBOptions& opt);
  // No copying allowed
  PointCloudLockManager(const PointCloudLockManager&) = delete;
  PointCloudLockManager& operator=(const PointCloudLockManager&) = delete;

  ~PointCloudLockManager() override {}

  bool IsPointLockSupported() const override { return true; }

  bool IsRangeLockSupported() const override { return false; }

  const LockTrackerFactory& GetLockTrackerFactory() const override {
    return PointLockTrackerFactory::Get();
  }

  // Creates a new LockMap for this column family.  Caller should guarantee
  // that this column family does not already exist.
  void AddColumnFamily(const ColumnFamilyHandle* cf) override;
  // Deletes the LockMap for this column family.  Caller should guarantee that
  // this column family is no longer in use.
  void RemoveColumnFamily(const ColumnFamilyHandle* cf) override;

  Status TryLock(CloudTransaction* txn, ColumnFamilyId column_family_id,
                 const std::string& key, Env* env, bool exclusive) override;
  Status TryLock(CloudTransaction* txn, ColumnFamilyId column_family_id,
                 const Endpoint& start, const Endpoint& end, Env* env,
                 bool exclusive) override;

  void UnLock(CloudTransaction* txn, const LockTracker& tracker,
              Env* env) override;
  void UnLock(CloudTransaction* txn, ColumnFamilyId column_family_id,
              const std::string& key, Env* env) override;
  void UnLock(CloudTransaction* txn, ColumnFamilyId column_family_id,
              const Endpoint& start, const Endpoint& end, Env* env) override;

  PointLockStatus GetPointLockStatus() override;

  RangeLockStatus GetRangeLockStatus() override;

  std::vector<DeadlockPath> GetDeadlockInfoBuffer() override;

  void Resize(uint32_t new_size) override;

 private:
  DBCloudImpl* txn_db_impl_;

  // Default number of lock map stripes per column family
  const size_t default_num_stripes_;

  // Limit on number of keys locked per column family
  const int64_t max_num_locks_;

  // The following lock order must be satisfied in order to avoid deadlocking
  // ourselves.
  //   - lock_map_mutex_
  //   - stripe mutexes in ascending cf id, ascending stripe order
  //   - wait_txn_map_mutex_
  //
  // Must be held when accessing/modifying lock_maps_.
  InstrumentedMutex lock_map_mutex_;

  // Map of ColumnFamilyId to locked key info
  using LockMaps = std::unordered_map<uint32_t, std::shared_ptr<LockMap>>;
  LockMaps lock_maps_;

  // Thread-local cache of entries in lock_maps_.  This is an optimization
  // to avoid acquiring a mutex in order to look up a LockMap
  std::unique_ptr<ThreadLocalPtr> lock_maps_cache_;

  // Must be held when modifying wait_txn_map_ and rev_wait_txn_map_.
  std::mutex wait_txn_map_mutex_;

  // Maps from waitee -> number of waiters.
  HashMap<TransactionID, int> rev_wait_txn_map_;
  // Maps from waiter -> waitee.
  HashMap<TransactionID, TrackedTrxInfo> wait_txn_map_;
  DeadlockInfoBuffer dlock_buffer_;

  // Used to allocate mutexes/condvars to use when locking keys
  std::shared_ptr<TransactionDBMutexFactory> mutex_factory_;

  bool IsLockExpired(TransactionID txn_id, const LockInfo& lock_info, Env* env,
                     uint64_t* wait_time);

  std::shared_ptr<LockMap> GetLockMap(uint32_t column_family_id);

  Status AcquireWithTimeout(CloudTransaction* txn, LockMap* lock_map,
                            LockMapStripe* stripe, uint32_t column_family_id,
                            const std::string& key, Env* env, int64_t timeout,
                            LockInfo&& lock_info);

  Status AcquireLocked(LockMap* lock_map, LockMapStripe* stripe,
                       const std::string& key, Env* env,
                       LockInfo&& lock_info, uint64_t* wait_time,
                       autovector<TransactionID>* txn_ids);

  void UnLockKey(CloudTransaction* txn, const std::string& key,
                 LockMapStripe* stripe, LockMap* lock_map, Env* env);

  bool IncrementWaiters(const CloudTransaction* txn,
                        const autovector<TransactionID>& wait_ids,
                        const std::string& key, const uint32_t& cf_id,
                        const bool& exclusive, Env* const env);
  void DecrementWaiters(const CloudTransaction* txn,
                        const autovector<TransactionID>& wait_ids);
  void DecrementWaitersImpl(const CloudTransaction* txn,
                            const autovector<TransactionID>& wait_ids);
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE

