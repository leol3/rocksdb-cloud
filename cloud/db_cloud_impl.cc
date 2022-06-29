// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "cloud/db_cloud_impl.h"

#include <cinttypes>

#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "file/sst_file_manager_impl.h"
#include "logging/auto_roll_logger.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/xxhash.h"
#include "utilities/persistent_cache/block_cache_tier.h"
#include "utilities/transactions/pessimistic_transaction_db.h"

#include <inttypes.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "test_util/sync_point.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"
#include "utilities/transactions/write_prepared_txn_db.h"

namespace ROCKSDB_NAMESPACE {

namespace {
/**
 * This ConstantSstFileManager uses the same size for every sst files added.
 */
class ConstantSizeSstFileManager : public SstFileManagerImpl {
 public:
  ConstantSizeSstFileManager(int64_t constant_file_size,
                             const std::shared_ptr<SystemClock>& clock,
                             const std::shared_ptr<FileSystem>& fs,
                             std::shared_ptr<Logger> logger,
                             int64_t rate_bytes_per_sec,
                             double max_trash_db_ratio,
                             uint64_t bytes_max_delete_chunk)
      : SstFileManagerImpl(clock, fs, std::move(logger), rate_bytes_per_sec,
                           max_trash_db_ratio, bytes_max_delete_chunk),
        constant_file_size_(constant_file_size) {
    assert(constant_file_size_ >= 0);
  }

  Status OnAddFile(const std::string& file_path) override {
    return SstFileManagerImpl::OnAddFile(
        file_path, uint64_t(constant_file_size_));
  }

 private:
  const int64_t constant_file_size_;
};
}  // namespace

//DBCloudImpl::DBCloudImpl(DB* db) : DBCloud(db), cenv_(nullptr) {}
DBCloudImpl::DBCloudImpl(
    DB* db, const TransactionDBOptions& txn_db_options)
    : DBCloud(db),
      db_impl_(static_cast_with_check<DBImpl, DB>(db)),
      txn_db_options_(txn_db_options),
      lock_manager_(NewLockManager(this, txn_db_options)) {
  assert(db_impl_ != nullptr);
  info_log_ = db_impl_->GetDBOptions().info_log;
}

DBCloudImpl::DBCloudImpl(
    StackableDB* db, const TransactionDBOptions& txn_db_options)
    : DBCloud(db),
      db_impl_(static_cast_with_check<DBImpl, DB>(db->GetRootDB())),
      txn_db_options_(txn_db_options),
      lock_manager_(NewLockManager(this, txn_db_options)) {
  assert(db_impl_ != nullptr);
}

Status DBCloudImpl::VerifyCFOptions(
    const ColumnFamilyOptions& cf_options) {
  const Comparator* const ucmp = cf_options.comparator;
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  if (0 == ts_sz) {
    return Status::OK();
  }
  if (ts_sz != sizeof(TxnTimestamp)) {
    std::ostringstream oss;
    oss << "Timestamp of transaction must have " << sizeof(TxnTimestamp)
        << " bytes. CF comparator " << std::string(ucmp->Name())
        << " timestamp size is " << ts_sz << " bytes";
    return Status::InvalidArgument(oss.str());
  }
  if (txn_db_options_.write_policy != WRITE_COMMITTED) {
    return Status::NotSupported("Only WriteCommittedTxn supports timestamp");
  }
  return Status::OK();
}

Status DBCloudImpl::Initialize(
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles) {
  for (auto cf_ptr : handles) {
    AddColumnFamily(cf_ptr);
  }

  // Verify cf options
  for (auto handle : handles) {
    ColumnFamilyDescriptor cfd;
    Status s = handle->GetDescriptor(&cfd);
    if (!s.ok()) {
      return s;
    }
    //s = VerifyCFOptions(cfd.options);
    if (!s.ok()) {
      return s;
    }
  }
  
  // Re-enable compaction for the column families that initially had
  // compaction enabled.
  std::vector<ColumnFamilyHandle*> compaction_enabled_cf_handles;
  compaction_enabled_cf_handles.reserve(compaction_enabled_cf_indices.size());
  for (auto index : compaction_enabled_cf_indices) {
    compaction_enabled_cf_handles.push_back(handles[index]);
  }

  Status s = EnableAutoCompaction(compaction_enabled_cf_handles);

  // create 'real' transactions from recovered shell transactions
  //auto dbimpl = reinterpret_cast<DBImpl*>(GetRootDB());
  auto dbimpl = static_cast_with_check<DBImpl, DB>(GetRootDB());
  assert(dbimpl != nullptr);
  auto rtrxs = dbimpl->recovered_transactions();

  for (auto it = rtrxs.begin(); it != rtrxs.end(); it++) {
    auto recovered_trx = it->second;
    assert(recovered_trx);
    //assert(recovered_trx->log_number_);
    assert(recovered_trx->name_.length());
    const auto& seq = recovered_trx->batches_.begin()->first;
    const auto& batch_info = recovered_trx->batches_.begin()->second;

    WriteOptions w_options;
    w_options.sync = true;
    TransactionOptions t_options;
    t_options.skip_concurrency_control = true;

    Transaction* real_trx = BeginTransaction(w_options, t_options, nullptr);
    assert(real_trx);
    real_trx->SetLogNumber(batch_info.log_number_);
    assert(seq != kMaxSequenceNumber);
    //real_trx->SetId(recovered_trx->seq_);
    if (GetTxnDBOptions().write_policy != WRITE_COMMITTED) {
      real_trx->SetId(seq);
    }

    s = real_trx->SetName(recovered_trx->name_);
    if (!s.ok()) {
      break;
    }

    s = real_trx->RebuildFromWriteBatch(batch_info.batch_);
    //assert(batch_info.batch_cnt_ == 0 ||
    //       real_trx->GetWriteBatch()->SubBatchCnt() == batch_info.batch_cnt_);
    real_trx->SetState(Transaction::PREPARED);
    if (!s.ok()) {
      break;
    }
  }
  if (s.ok()) {
    dbimpl->DeleteAllRecoveredTransactions();
  }
  return s;
}



Transaction* DBCloudImpl::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new CloudTransaction(this, write_options, txn_options);
  }
}

TransactionDBOptions DBCloudImpl::ValidateTxnDBOptions(
    const TransactionDBOptions& txn_db_options) {
  TransactionDBOptions validated = txn_db_options;

  if (txn_db_options.num_stripes == 0) {
    validated.num_stripes = 1;
  }

  return validated;
}

// Let TransactionLockMgr know that this column family exists so it can
// allocate a LockMap for it.
void DBCloudImpl::AddColumnFamily(
    const ColumnFamilyHandle* handle) {
  lock_manager_->AddColumnFamily(handle);
}

Status DBCloudImpl::CreateColumnFamily(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    ColumnFamilyHandle** handle) {
  InstrumentedMutexLock l(&column_family_mutex_);
  Status s = VerifyCFOptions(options);
  if (!s.ok()) {
    return s;
  }

  s = db_->CreateColumnFamily(options, column_family_name, handle);
  if (s.ok()) {
    lock_manager_->AddColumnFamily((*handle));
    UpdateCFComparatorMap(*handle);
  }

  return s;
}

// Let TransactionLockMgr know that it can deallocate the LockMap for this
// column family.
Status DBCloudImpl::DropColumnFamily(
    ColumnFamilyHandle* column_family) {
  InstrumentedMutexLock l(&column_family_mutex_);

  Status s = db_->DropColumnFamily(column_family);
  if (s.ok()) {
    lock_manager_->RemoveColumnFamily(column_family);
  }

  return s;
}

Status DBCloudImpl::TryLock(CloudTransaction* txn,
                                         uint32_t cfh_id,
                                         const std::string& key,
                                         bool exclusive) {
  return lock_manager_->TryLock(txn, cfh_id, key, GetEnv(), exclusive);
}

Status DBCloudImpl::TryRangeLock(CloudTransaction* txn,
                                              uint32_t cfh_id,
                                              const Endpoint& start_endp,
                                              const Endpoint& end_endp) {
  return lock_manager_->TryLock(txn, cfh_id, start_endp, end_endp, GetEnv(),
                                /*exclusive=*/true);
}

void DBCloudImpl::UnLock(CloudTransaction* txn,
                                      const LockTracker& keys) {
  lock_manager_->UnLock(txn, keys, GetEnv());
}

void DBCloudImpl::UnLock(CloudTransaction* txn,
                                      uint32_t cfh_id, const std::string& key) {
  lock_manager_->UnLock(txn, cfh_id, key, GetEnv());
}

// Used when wrapping DB write operations in a transaction
Transaction* DBCloudImpl::BeginInternalTransaction(
    const WriteOptions& options) {
  TransactionOptions txn_options;
  Transaction* txn = BeginTransaction(options, txn_options, nullptr);

  // Use default timeout for non-transactional writes
  txn->SetLockTimeout(txn_db_options_.default_lock_timeout);
  return txn;
}

// All user Put, Merge, Delete, and Write requests must be intercepted to make
// sure that they lock all keys that they are writing to avoid causing conflicts
// with any concurrent transactions. The easiest way to do this is to wrap all
// write operations in a transaction.
//
// Put(), Merge(), and Delete() only lock a single key per call.  Write() will
// sort its keys before locking them.  This guarantees that TransactionDB write
// methods cannot deadlock with eachother (but still could deadlock with a
// Transaction).
Status DBCloudImpl::Put(const WriteOptions& options,
                                     ColumnFamilyHandle* column_family,
                                     const Slice& key, const Slice& val) {
  Status s;

  Transaction* txn = BeginInternalTransaction(options);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do PutUntracked().
  s = txn->PutUntracked(column_family, key, val);

  if (s.ok()) {
    s = txn->Commit();
  }

  delete txn;

  return s;
}

Status DBCloudImpl::Delete(const WriteOptions& wopts,
                                        ColumnFamilyHandle* column_family,
                                        const Slice& key) {
  Status s;

  Transaction* txn = BeginInternalTransaction(wopts);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do
  // DeleteUntracked().
  s = txn->DeleteUntracked(column_family, key);

  if (s.ok()) {
    s = txn->Commit();
  }

  delete txn;

  return s;
}

Status DBCloudImpl::SingleDelete(const WriteOptions& wopts,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key) {
  Status s;

  Transaction* txn = BeginInternalTransaction(wopts);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do
  // SingleDeleteUntracked().
  s = txn->SingleDeleteUntracked(column_family, key);

  if (s.ok()) {
    s = txn->Commit();
  }

  delete txn;

  return s;
}

Status DBCloudImpl::Merge(const WriteOptions& options,
                                       ColumnFamilyHandle* column_family,
                                       const Slice& key, const Slice& value) {
  Status s;

  Transaction* txn = BeginInternalTransaction(options);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do
  // MergeUntracked().
  s = txn->MergeUntracked(column_family, key, value);

  if (s.ok()) {
    s = txn->Commit();
  }

  delete txn;

  return s;
}

/*Status DBCloudImpl::Write(const WriteOptions& opts,
                                       WriteBatch* updates) {
  // Need to lock all keys in this batch to prevent write conflicts with
  // concurrent transactions.
  Transaction* txn = BeginInternalTransaction(opts);
  txn->DisableIndexing();
  // TODO(myabandeh): indexing being disabled we need another machanism to
  // detect duplicattes in the input patch

  auto txn_impl =
      static_cast_with_check<CloudTransaction, Transaction>(txn);

  // Since commitBatch sorts the keys before locking, concurrent Write()
  // operations will not cause a deadlock.
  // In order to avoid a deadlock with a concurrent Transaction, Transactions
  // should use a lock timeout.
  Status s = txn_impl->CommitBatch(updates);

  delete txn;

  return s;
}*/

Status DBCloudImpl::Write(const WriteOptions& opts,
                                  WriteBatch* updates) {
  Status s = FailIfBatchHasTs(updates);
  if (!s.ok()) {
    return s;
  }
  if (txn_db_options_.skip_concurrency_control) {
    return db_impl_->Write(opts, updates);
  } else {
    return WriteWithConcurrencyControl(opts, updates);
  }
}
Status DBCloudImpl::Write(
    const WriteOptions& opts,
    const TransactionDBWriteOptimizations& optimizations, WriteBatch* updates) {
  Status s = FailIfBatchHasTs(updates);
  if (!s.ok()) {
    return s;
  }
  if (optimizations.skip_concurrency_control) {
    return db_impl_->Write(opts, updates);
  } else {
    return WriteWithConcurrencyControl(opts, updates);
  }
}


void DBCloudImpl::InsertExpirableTransaction(
    TransactionID tx_id, CloudTransaction* tx) {
  assert(tx->GetExpirationTime() > 0);
  std::lock_guard<std::mutex> lock(map_mutex_);
  expirable_transactions_map_.insert({tx_id, tx});
}

void DBCloudImpl::RemoveExpirableTransaction(TransactionID tx_id) {
  std::lock_guard<std::mutex> lock(map_mutex_);
  expirable_transactions_map_.erase(tx_id);
}

bool DBCloudImpl::TryStealingExpiredTransactionLocks(
    TransactionID tx_id) {
  std::lock_guard<std::mutex> lock(map_mutex_);

  auto tx_it = expirable_transactions_map_.find(tx_id);
  if (tx_it == expirable_transactions_map_.end()) {
    return true;
  }
  CloudTransaction& tx = *(tx_it->second);
  return tx.TryStealingLocks();
}

void DBCloudImpl::ReinitializeTransaction(
    Transaction* txn, const WriteOptions& write_options,
    const TransactionOptions& txn_options) {
  auto txn_impl =
      static_cast_with_check<CloudTransaction, Transaction>(txn);

  txn_impl->Reinitialize(this, write_options, txn_options);
}

Transaction* DBCloudImpl::GetTransactionByName(
    const TransactionName& name) {
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  auto it = transactions_.find(name);
  if (it == transactions_.end()) {
    return nullptr;
  } else {
    return it->second;
  }
}

void DBCloudImpl::GetAllPreparedTransactions(
    std::vector<Transaction*>* transv) {
  assert(transv);
  transv->clear();
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  for (auto it = transactions_.begin(); it != transactions_.end(); it++) {
    if (it->second->GetState() == Transaction::PREPARED) {
      transv->push_back(it->second);
    }
  }
}

/*TransactionLockMgr::LockStatusData
DBCloudImpl::GetLockStatusData() {
  return lock_manager_.GetLockStatusData();
}*/
LockManager::PointLockStatus DBCloudImpl::GetLockStatusData() {
  return lock_manager_->GetPointLockStatus();
}

std::vector<DeadlockPath> DBCloudImpl::GetDeadlockInfoBuffer() {
  return lock_manager_->GetDeadlockInfoBuffer();
}

void DBCloudImpl::SetDeadlockInfoBufferSize(uint32_t target_size) {
  lock_manager_->Resize(target_size);
}

void DBCloudImpl::RegisterTransaction(Transaction* txn) {
  assert(txn);
  assert(txn->GetName().length() > 0);
  assert(GetTransactionByName(txn->GetName()) == nullptr);
  assert(txn->GetState() == Transaction::STARTED);
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  transactions_[txn->GetName()] = txn;
}

void DBCloudImpl::UnregisterTransaction(Transaction* txn) {
  assert(txn);
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  auto it = transactions_.find(txn->GetName());
  assert(it != transactions_.end());
  transactions_.erase(it);
}


//DBCloudImpl::DBCloudImpl(DB* db) : DBCloud(db), cenv_(nullptr) {}

DBCloudImpl::~DBCloudImpl() {
  while (!transactions_.empty()) {
    delete transactions_.begin()->second;
    // TODO(myabandeh): this seems to be an unsafe approach as it is not quite
    // clear whether delete would also remove the entry from transactions_.
  }
}

Status DBCloud::Open(const Options& options, const std::string& dbname,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb, DBCloud** dbptr,
                     bool read_only) {
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  DBCloud* dbcloud = nullptr;
  Status s =
      DBCloud::Open(options, dbname, column_families, persistent_cache_path,
                    persistent_cache_size_gb, &handles, &dbcloud, read_only);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
    *dbptr = dbcloud;
  }
  return s;
}

Status DBCloud::Open(const Options& opt, const std::string& local_dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb,
                     std::vector<ColumnFamilyHandle*>* handles, DBCloud** dbptr,
                     bool read_only) {
  Status st;
  Options options = opt;

  // Created logger if it is not already pre-created by user.
  if (!options.info_log) {
    CreateLoggerFromOptions(local_dbname, options, &options.info_log);
  }

  CloudEnvImpl* cenv = static_cast<CloudEnvImpl*>(options.env);
  if (!cenv->info_log_) {
    cenv->info_log_ = options.info_log;
  }
  // Use a constant sized SST File Manager if necesary.
  // NOTE: if user already passes in an SST File Manager, we will respect user's
  // SST File Manager instead.
  auto constant_sst_file_size =
      cenv->GetCloudEnvOptions().constant_sst_file_size_in_sst_file_manager;
  if (constant_sst_file_size >= 0 && options.sst_file_manager == nullptr) {
    // rate_bytes_per_sec, max_trash_db_ratio, bytes_max_delete_chunk are
    // default values in NewSstFileManager.
    // If users don't use Options.sst_file_manager, then these values are used
    // currently when creating an SST File Manager.
    options.sst_file_manager = std::make_shared<ConstantSizeSstFileManager>(
        constant_sst_file_size, options.env->GetSystemClock(),
        options.env->GetFileSystem(), options.info_log,
        0 /* rate_bytes_per_sec */, 0.25 /* max_trash_db_ratio */,
        64 * 1024 * 1024 /* bytes_max_delete_chunk */);
  }

  Env* local_env = cenv->GetBaseEnv();
  if (!read_only) {
    local_env->CreateDirIfMissing(
        local_dbname);  // MJR: TODO: Move into sanitize
  }

  bool new_db = false;
  // If cloud manifest is already loaded, this means the directory has been
  // sanitized (possibly by the call to ListColumnFamilies())
  if (cenv->GetCloudManifest() == nullptr) {
    st = cenv->SanitizeDirectory(options, local_dbname, read_only);

    if (st.ok()) {
      st = cenv->LoadCloudManifest(local_dbname, read_only);
    }
    if (st.IsNotFound()) {
      Log(InfoLogLevel::INFO_LEVEL, options.info_log,
          "CLOUDMANIFEST not found in the cloud, assuming this is a new "
          "database");
      new_db = true;
      st = Status::OK();
    } else if (!st.ok()) {
      return st;
    }
  }
  if (new_db) {
    if (read_only) {
      return Status::NotFound("CLOUDMANIFEST not found and read_only is set.");
    }
    st = cenv->CreateCloudManifest(local_dbname);
    if (!st.ok()) {
      return st;
    }
  }

  // If a persistent cache path is specified, then we set it in the options.
  if (!persistent_cache_path.empty() && persistent_cache_size_gb) {
    // Get existing options. If the persistent cache is already set, then do
    // not make any change. Otherwise, configure it.
    auto* tableopt =
        options.table_factory->GetOptions<BlockBasedTableOptions>();
    if (tableopt != nullptr && !tableopt->persistent_cache) {
      PersistentCacheConfig config(
          local_env, persistent_cache_path,
          persistent_cache_size_gb * 1024L * 1024L * 1024L, options.info_log);
      auto pcache = std::make_shared<BlockCacheTier>(config);
      st = pcache->Open();
      if (st.ok()) {
        tableopt->persistent_cache = pcache;
        Log(InfoLogLevel::INFO_LEVEL, options.info_log,
            "Created persistent cache %s with size %" PRIu64 "GB",
            persistent_cache_path.c_str(), persistent_cache_size_gb);
      } else {
        Log(InfoLogLevel::INFO_LEVEL, options.info_log,
            "Unable to create persistent cache %s. %s",
            persistent_cache_path.c_str(), st.ToString().c_str());
        return st;
      }
    }
  }
  // We do not want a very large MANIFEST file because the MANIFEST file is
  // uploaded to S3 for every update, so always enable rolling of Manifest file
  options.max_manifest_file_size = DBCloudImpl::max_manifest_file_size;

  DB* db = nullptr;
  std::string dbid;
  if (read_only) {
    st = DB::OpenForReadOnly(options, local_dbname, column_families, handles,
                             &db);
  } else {
    st = DB::Open(options, local_dbname, column_families, handles, &db);
  }

  if (new_db && st.ok() && cenv->HasDestBucket()) {
    // This is a new database, upload the CLOUDMANIFEST after all MANIFEST file
    // was already uploaded. It is at this point we consider the database
    // committed in the cloud.
    st = cenv->GetStorageProvider()->PutCloudObject(
        CloudManifestFile(local_dbname), cenv->GetDestBucketName(),
        CloudManifestFile(cenv->GetDestObjectPath()));
  }

  // now that the database is opened, all file sizes have been verified and we
  // no longer need to verify file sizes for each file that we open. Note that
  // this might have a data race with background compaction, but it's not a big
  // deal, since it's a boolean and it does not impact correctness in any way.
  if (cenv->GetCloudEnvOptions().validate_filesize) {
    *const_cast<bool*>(&cenv->GetCloudEnvOptions().validate_filesize) = false;
  }

  if (st.ok()) {
    //DBCloudImpl* cloud = new DBCloudImpl(db);
    TransactionDBOptions txn_db_options;
    txn_db_options.transaction_lock_timeout = 2000;  // 2 seconds
    //tx_db_options.custom_mutex_factory = std::make_shared<Rdb_mutex_factory>();
    txn_db_options.write_policy =
      static_cast<rocksdb::TxnDBWritePolicy>(rocksdb::TxnDBWritePolicy::WRITE_COMMITTED);
    DBCloudImpl* cloud = new DBCloudImpl(db, txn_db_options);
    *dbptr = cloud;
    db->GetDbIdentity(dbid);
  }
  Log(InfoLogLevel::INFO_LEVEL, options.info_log,
      "Opened cloud db with local dir %s dbid %s. %s", local_dbname.c_str(),
      dbid.c_str(), st.ToString().c_str());
  return st;
}

Status DBCloudImpl::Savepoint() {
  std::string dbid;
  Options default_options = GetOptions();
  Status st = GetDbIdentity(dbid);
  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
        "Savepoint could not get dbid %s", st.ToString().c_str());
    return st;
  }
  CloudEnvImpl* cenv = static_cast<CloudEnvImpl*>(GetEnv());

  // If there is no destination bucket, then nothing to do
  if (!cenv->HasDestBucket()) {
    Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
        "Savepoint on cloud dbid %s has no destination bucket, nothing to do.",
        dbid.c_str());
    return st;
  }

  Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
      "Savepoint on cloud dbid  %s", dbid.c_str());

  // find all sst files in the db
  std::vector<LiveFileMetaData> live_files;
  GetLiveFilesMetaData(&live_files);

  auto provider = cenv->GetStorageProvider();
  // If an sst file does not exist in the destination path, then remember it
  std::vector<std::string> to_copy;
  for (auto onefile : live_files) {
    auto remapped_fname = cenv->RemapFilename(onefile.name);
    std::string destpath = cenv->GetDestObjectPath() + "/" + remapped_fname;
    if (!provider->ExistsCloudObject(cenv->GetDestBucketName(), destpath)
             .ok()) {
      to_copy.push_back(remapped_fname);
    }
  }

  // copy all files in parallel
  std::atomic<size_t> next_file_meta_idx(0);
  int max_threads = default_options.max_file_opening_threads;

  std::function<void()> load_handlers_func = [&]() {
    while (true) {
      size_t idx = next_file_meta_idx.fetch_add(1);
      if (idx >= to_copy.size()) {
        break;
      }
      auto& onefile = to_copy[idx];
      Status s = provider->CopyCloudObject(
          cenv->GetSrcBucketName(), cenv->GetSrcObjectPath() + "/" + onefile,
          cenv->GetDestBucketName(), cenv->GetDestObjectPath() + "/" + onefile);
      if (!s.ok()) {
        Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
            "Savepoint on cloud dbid  %s error in copying srcbucket %s srcpath "
            "%s dest bucket %s dest path %s. %s",
            dbid.c_str(), cenv->GetSrcBucketName().c_str(),
            cenv->GetSrcObjectPath().c_str(), cenv->GetDestBucketName().c_str(),
            cenv->GetDestObjectPath().c_str(), s.ToString().c_str());
        if (st.ok()) {
          st = s;  // save at least one error
        }
        break;
      }
    }
  };

  if (max_threads <= 1) {
    load_handlers_func();
  } else {
    std::vector<port::Thread> threads;
    for (int i = 0; i < max_threads; i++) {
      threads.emplace_back(load_handlers_func);
    }
    for (auto& t : threads) {
      t.join();
    }
  }
  return st;
}

Status DBCloudImpl::CheckpointToCloud(const BucketOptions& destination,
                                      const CheckpointToCloudOptions& options) {
  DisableFileDeletions();
  auto st = DoCheckpointToCloud(destination, options);
  EnableFileDeletions(false);
  return st;
}

Status DBCloudImpl::DoCheckpointToCloud(
    const BucketOptions& destination, const CheckpointToCloudOptions& options) {
  std::vector<std::string> live_files;
  uint64_t manifest_file_size{0};
  auto cenv = static_cast<CloudEnvImpl*>(GetEnv());
  auto base_env = cenv->GetBaseEnv();

  auto st =
      GetLiveFiles(live_files, &manifest_file_size, options.flush_memtable);
  if (!st.ok()) {
    return st;
  }

  std::vector<std::pair<std::string, std::string>> files_to_copy;
  for (auto& f : live_files) {
    uint64_t number = 0;
    FileType type;
    auto ok = ParseFileName(f, &number, &type);
    if (!ok) {
      return Status::InvalidArgument("Unknown file " + f);
    }
    if (type != kTableFile) {
      // ignore
      continue;
    }
    auto remapped_fname = cenv->RemapFilename(f);
    files_to_copy.emplace_back(remapped_fname, remapped_fname);
  }

  // IDENTITY file
  std::string dbid;
  st = ReadFileToString(cenv, IdentityFileName(GetName()), &dbid);
  if (!st.ok()) {
    return st;
  }
  dbid = rtrim_if(trim(dbid), '\n');
  files_to_copy.emplace_back(IdentityFileName(""), IdentityFileName(""));

  // MANIFEST file
  auto current_epoch = cenv->GetCloudManifest()->GetCurrentEpoch().ToString();
  auto manifest_fname = ManifestFileWithEpoch("", current_epoch);
  auto tmp_manifest_fname = manifest_fname + ".tmp";
  auto fs = base_env->GetFileSystem();
  st =
      CopyFile(fs.get(), GetName() + "/" + manifest_fname,
               GetName() + "/" + tmp_manifest_fname, manifest_file_size, false,
               nullptr, Temperature::kUnknown);
  if (!st.ok()) {
    return st;
  }
  files_to_copy.emplace_back(tmp_manifest_fname, std::move(manifest_fname));

  // CLOUDMANIFEST file
  files_to_copy.emplace_back(CloudManifestFile(""), CloudManifestFile(""));

  std::atomic<size_t> next_file_to_copy{0};
  int thread_count = std::max(1, options.thread_count);
  std::vector<Status> thread_statuses;
  thread_statuses.resize(thread_count);

  auto do_copy = [&](size_t threadId) {
    auto provider = cenv->GetStorageProvider();
    while (true) {
      size_t idx = next_file_to_copy.fetch_add(1);
      if (idx >= files_to_copy.size()) {
        break;
      }

      auto& f = files_to_copy[idx];
      auto copy_st = provider->PutCloudObject(
          GetName() + "/" + f.first, destination.GetBucketName(),
          destination.GetObjectPath() + "/" + f.second);
      if (!copy_st.ok()) {
        thread_statuses[threadId] = std::move(copy_st);
        break;
      }
    }
  };

  if (thread_count == 1) {
    do_copy(0);
  } else {
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
      threads.emplace_back([&, i]() { do_copy(i); });
    }
    for (auto& t : threads) {
      t.join();
    }
  }

  for (auto& s : thread_statuses) {
    if (!s.ok()) {
      st = s;
      break;
    }
  }

  if (!st.ok()) {
    return st;
  }

  // Ignore errors
  base_env->DeleteFile(tmp_manifest_fname);

  st = cenv->SaveDbid(destination.GetBucketName(), dbid,
                      destination.GetObjectPath());
  return st;
}

Status DBCloudImpl::ExecuteRemoteCompactionRequest(
    const PluggableCompactionParam& inputParams,
    PluggableCompactionResult* result, bool doSanitize) {
  auto cenv = static_cast<CloudEnvImpl*>(GetEnv());

  // run the compaction request on the underlying local database
  Status status = GetBaseDB()->ExecuteRemoteCompactionRequest(
      inputParams, result, doSanitize);
  if (!status.ok()) {
    return status;
  }

  // convert the local pathnames to the cloud pathnames
  for (unsigned int i = 0; i < result->output_files.size(); i++) {
    OutputFile* outfile = &result->output_files[i];
    outfile->pathname = cenv->RemapFilename(outfile->pathname);
  }
  return Status::OK();
}

Status DBCloud::ListColumnFamilies(const DBOptions& db_options,
                                   const std::string& name,
                                   std::vector<std::string>* column_families) {
  CloudEnvImpl* cenv = static_cast<CloudEnvImpl*>(db_options.env);

  Env* local_env = cenv->GetBaseEnv();
  local_env->CreateDirIfMissing(name);

  Status st;
  st = cenv->SanitizeDirectory(db_options, name, false);
  if (st.ok()) {
    st = cenv->LoadCloudManifest(name, false);
  }
  if (st.ok()) {
    st = DB::ListColumnFamilies(db_options, name, column_families);
  }

  return st;
}

Status DBCloud::Open(const Options& options, const TransactionDBOptions& txn_db_options, const std::string& dbname,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb, DBCloud** dbptr,
                     bool read_only){
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  DBCloud* dbcloud = nullptr;
  Status s =
      DBCloud::Open(options, txn_db_options, dbname, column_families, persistent_cache_path,
                    persistent_cache_size_gb, &handles, &dbcloud, read_only);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
    *dbptr = dbcloud;
  }
  return s;
}

Status DBCloud::Open(const Options& opt, const TransactionDBOptions& txn_db_options, const std::string& local_dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb,
                     std::vector<ColumnFamilyHandle*>* handles, DBCloud** dbptr,
                     bool read_only){
  Status st;
  Options options = opt;

  // Created logger if it is not already pre-created by user.
  if (!options.info_log) {
    CreateLoggerFromOptions(local_dbname, options, &options.info_log);
  }

  CloudEnvImpl* cenv = static_cast<CloudEnvImpl*>(options.env);
  if (!cenv->info_log_) {
    cenv->info_log_ = options.info_log;
  }
  // Use a constant sized SST File Manager if necesary.
  // NOTE: if user already passes in an SST File Manager, we will respect user's
  // SST File Manager instead.
  auto constant_sst_file_size =
      cenv->GetCloudEnvOptions().constant_sst_file_size_in_sst_file_manager;
  if (constant_sst_file_size >= 0 && options.sst_file_manager == nullptr) {
    // rate_bytes_per_sec, max_trash_db_ratio, bytes_max_delete_chunk are
    // default values in NewSstFileManager.
    // If users don't use Options.sst_file_manager, then these values are used
    // currently when creating an SST File Manager.
    options.sst_file_manager = std::make_shared<ConstantSizeSstFileManager>(
        constant_sst_file_size, options.env->GetSystemClock(),
        options.env->GetFileSystem(), options.info_log,
        0 /* rate_bytes_per_sec */, 0.25 /* max_trash_db_ratio */,
        64 * 1024 * 1024 /* bytes_max_delete_chunk */);
  }

  Env* local_env = cenv->GetBaseEnv();
  if (!read_only) {
    local_env->CreateDirIfMissing(local_dbname);
  }

  /*st = DBCloudImpl::SanitizeDirectory(options, local_dbname, read_only);
  if (!st.ok()) {
    return st;
  }
  printf("SanitizeDirectory, st.ok(): %d\n", (int)st.ok());
  if (!read_only && cenv->GetCloudType() != CloudType::kCloudNone) {
    st = DBCloudImpl::MaybeMigrateManifestFile(local_env, local_dbname);
    if (st.ok()) {
      // Init cloud manifest
      st = DBCloudImpl::FetchCloudManifest(options, local_dbname);
    }*/
  // If cloud manifest is already loaded, this means the directory has been
  // sanitized (possibly by the call to ListColumnFamilies())
  //if (cenv->GetCloudManifest() == nullptr) {
    //printf("SanitizeDirectory, st.ok(): %d\n", (int)st.ok());
    //st = cenv->SanitizeDirectory(options, local_dbname, read_only);
    //if (st.ok()) {
      // Inits CloudEnvImpl::cloud_manifest_, which will enable us to read files
      // from the cloud
      //st = cenv->LoadCloudManifest(local_dbname, read_only);
    //}
    /*if (st.ok()) {
      // Rolls the new epoch in CLOUDMANIFEST
      st = DBCloudImpl::RollNewEpoch(cenv, local_dbname);
    }*/
    //printf("LoadLocalCloudManifest, st.ok(): %d\n", (int)st.ok());
    //if (!st.ok()) {
      //return st;
    //}

    // Do the cleanup, but don't fail if the cleanup fails.
    /*st = cenv->DeleteInvisibleFiles(local_dbname);
    if (!st.ok()) {
      Log(InfoLogLevel::INFO_LEVEL, options.info_log,
          "Failed to delete invisible files: %s", st.ToString().c_str());
      // Ignore the fail
      st = Status::OK();
    }*/
  //}
  bool new_db = false;
  // If cloud manifest is already loaded, this means the directory has been
  // sanitized (possibly by the call to ListColumnFamilies())
  if (cenv->GetCloudManifest() == nullptr) {
    st = cenv->SanitizeDirectory(options, local_dbname, read_only);

    if (st.ok()) {
      st = cenv->LoadCloudManifest(local_dbname, read_only);
    }
    if (st.IsNotFound()) {
      Log(InfoLogLevel::INFO_LEVEL, options.info_log,
          "CLOUDMANIFEST not found in the cloud, assuming this is a new "
          "database");
      new_db = true;
      st = Status::OK();
    } else if (!st.ok()) {
      return st;
    }
  }
  if (new_db) {
    if (read_only) {
      return Status::NotFound("CLOUDMANIFEST not found and read_only is set.");
    }
    st = cenv->CreateCloudManifest(local_dbname);
    if (!st.ok()) {
      return st;
    }
  }
  printf("persistent_cache_path, st.ok(): %d\n", (int)st.ok());
  // If a persistent cache path is specified, then we set it in the options.
  //if (!persistent_cache_path.empty() && persistent_cache_size_gb) {
    // Get existing options. If the persistent cache is already set, then do
    // not make any change. Otherwise, configure it.
    /*void* bopt = options.table_factory->GetOptions();
    if (bopt != nullptr) {
      BlockBasedTableOptions* tableopt =
          static_cast<BlockBasedTableOptions*>(bopt);
      if (!tableopt->persistent_cache) {
        std::shared_ptr<PersistentCache> pcache;
        st =
            NewPersistentCache(options.env, persistent_cache_path,
                               persistent_cache_size_gb * 1024L * 1024L * 1024L,
                               options.info_log, false, &pcache);*/
    /*auto* tableopt =
        options.table_factory->GetOptions<BlockBasedTableOptions>();
    if (tableopt != nullptr && !tableopt->persistent_cache) {
      PersistentCacheConfig config(
          local_env, persistent_cache_path,
          persistent_cache_size_gb * 1024L * 1024L * 1024L, options.info_log);
      auto pcache = std::make_shared<BlockCacheTier>(config);
      st = pcache->Open();
        if (st.ok()) {
          tableopt->persistent_cache = pcache;
          Log(InfoLogLevel::INFO_LEVEL, options.info_log,
              "Created persistent cache %s with size %ld GB",
              persistent_cache_path.c_str(), persistent_cache_size_gb);
        } else {
          Log(InfoLogLevel::INFO_LEVEL, options.info_log,
              "Unable to create persistent cache %s. %s",
              persistent_cache_path.c_str(), st.ToString().c_str());
          return st;
        }
      }
    }*/
  if (!persistent_cache_path.empty() && persistent_cache_size_gb) {
    // Get existing options. If the persistent cache is already set, then do
    // not make any change. Otherwise, configure it.
    //void* bopt = options.table_factory->GetOptions();
    //if (bopt != nullptr) {
    //  BlockBasedTableOptions* tableopt =
    //      static_cast<BlockBasedTableOptions*>(bopt);
    auto* tableopt =
        options.table_factory->GetOptions<BlockBasedTableOptions>();
    if (tableopt != nullptr && !tableopt->persistent_cache) {
      if (!tableopt->persistent_cache) {
        PersistentCacheConfig config(
            local_env, persistent_cache_path,
            persistent_cache_size_gb * 1024L * 1024L * 1024L, options.info_log);
        auto pcache = std::make_shared<BlockCacheTier>(config);
        st = pcache->Open();
        if (st.ok()) {
          tableopt->persistent_cache = pcache;
          Log(InfoLogLevel::INFO_LEVEL, options.info_log,
              "Created persistent cache %s with size %" PRIu64 "GB",
              persistent_cache_path.c_str(), persistent_cache_size_gb);
        } else {
          Log(InfoLogLevel::INFO_LEVEL, options.info_log,
              "Unable to create persistent cache %s. %s",
              persistent_cache_path.c_str(), st.ToString().c_str());
          return st;
        }
      }
    }
  }

  // We do not want a very large MANIFEST file because the MANIFEST file is
  // uploaded to S3 for every update, so always enable rolling of Manifest file
  options.max_manifest_file_size = DBCloudImpl::max_manifest_file_size;

  //ROCKS_LOG_WARN(options.info_log, "Transaction write_policy is " PRId32,
                 //static_cast<int>(txn_db_options.write_policy));
  std::vector<ColumnFamilyDescriptor> column_families_copy = column_families;
  std::vector<size_t> compaction_enabled_cf_indices;
  DBOptions db_options_2pc = options;
  PrepareWrap(&db_options_2pc, &column_families_copy,
              &compaction_enabled_cf_indices);
  //const bool use_seq_per_batch = txn_db_options.write_policy == WRITE_PREPARED;
  DB* db = nullptr;
  std::string dbid;
  /*if (read_only) {
    st = DB::OpenForReadOnly(db_options_2pc, local_dbname, column_families_copy, handles,
                             &db);
  } else {
    st = DB::Open(db_options_2pc, local_dbname, column_families_copy, handles, &db);
  }*/
  const bool use_seq_per_batch =
      txn_db_options.write_policy == WRITE_PREPARED ||
      txn_db_options.write_policy == WRITE_UNPREPARED;
  const bool use_batch_per_txn =
      txn_db_options.write_policy == WRITE_COMMITTED ||
      txn_db_options.write_policy == WRITE_PREPARED;
  st = DBImpl::Open(db_options_2pc, local_dbname, column_families_copy, handles, &db,
                   use_seq_per_batch, use_batch_per_txn);
  printf("DB::Open, read_only:%d , st.ok(): %d\n", (int)read_only, (int)st.ok());
  // now that the database is opened, all file sizes have been verified and we
  // no longer need to verify file sizes for each file that we open. Note that
  // this might have a data race with background compaction, but it's not a big
  // deal, since it's a boolean and it does not impact correctness in any way.
  if (cenv->GetCloudEnvOptions().validate_filesize) {
    *const_cast<bool*>(&cenv->GetCloudEnvOptions().validate_filesize) = false;
  }

  if (st.ok()) {
    DBCloudImpl* cloud = new DBCloudImpl(db, txn_db_options);
    *dbptr = cloud;
    st = WrapDB(db, txn_db_options, compaction_enabled_cf_indices, *handles,
               dbptr);
    db->GetDbIdentity(dbid);
  }
  Log(InfoLogLevel::INFO_LEVEL, options.info_log,
      "Opened cloud db with local dir %s dbid %s. %s", local_dbname.c_str(),
      dbid.c_str(), st.ToString().c_str());
  return st;
}

void DBCloud::PrepareWrap(
    DBOptions* db_options, std::vector<ColumnFamilyDescriptor>* column_families,
    std::vector<size_t>* compaction_enabled_cf_indices) {
  compaction_enabled_cf_indices->clear();

  // Enable MemTable History if not already enabled
  for (size_t i = 0; i < column_families->size(); i++) {
    ColumnFamilyOptions* cf_options = &(*column_families)[i].options;

    if (cf_options->max_write_buffer_number_to_maintain == 0) {
      // Setting to -1 will set the History size to max_write_buffer_number.
      cf_options->max_write_buffer_number_to_maintain = -1;
    }
    if (!cf_options->disable_auto_compactions) {
      // Disable compactions momentarily to prevent race with DB::Open
      cf_options->disable_auto_compactions = true;
      compaction_enabled_cf_indices->push_back(i);
    }
  }
  db_options->allow_2pc = true;
}

Status DBCloud::WrapDB(
    // make sure this db is already opened with memtable history enabled,
    // auto compaction distabled and 2 phase commit enabled
    DB* db, const TransactionDBOptions& txn_db_options,
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles, DBCloud** dbptr) {
  DBCloudImpl* txn_db = new DBCloudImpl(db, txn_db_options);
  *dbptr = txn_db;
  Status s = txn_db->Initialize(compaction_enabled_cf_indices, handles);
  return s;
}

Status DBCloud::WrapStackableDB(
    // make sure this stackable_db is already opened with memtable history
    // enabled,
    // auto compaction distabled and 2 phase commit enabled
    StackableDB* db, const TransactionDBOptions& txn_db_options,
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles, DBCloud** dbptr) {
  DBCloudImpl* txn_db = new DBCloudImpl(db, txn_db_options);
  *dbptr = txn_db;
  Status s = txn_db->Initialize(compaction_enabled_cf_indices, handles);
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
