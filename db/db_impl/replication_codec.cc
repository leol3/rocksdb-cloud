#include "db/db_impl/replication_codec.h"
#include "db/memtable.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

Status SerializeMemTableSwitchRecord(std::string* dst,
                                     const MemTableSwitchRecord& record) {
  PutFixed16(dst, 1);  // version
  PutVarint64(dst, record.next_log_num);
  return Status::OK();
}
Status DeserializeMemTableSwitchRecord(Slice* src,
                                       MemTableSwitchRecord* record) {
  uint16_t version;
  if (!GetFixed16(src, &version)) {
    return Status::Corruption(
        "Unable to decode memtable switch record version");
  }
  uint64_t next_log_num;
  if (!GetVarint64(src, &next_log_num)) {
    return Status::Corruption("Unable to decode memtable switch next_log_num");
  }

  record->next_log_num = next_log_num;
  return Status::OK();
}

std::string RecordMemTableSwitch(
    const std::shared_ptr<rocksdb::ReplicationLogListener>&
        replication_log_listener,
    const MemTableSwitchRecord& record) {
  ReplicationLogRecord rlr;
  rlr.type = ReplicationLogRecord::kMemtableSwitch;
  SerializeMemTableSwitchRecord(&rlr.contents, record);
  return replication_log_listener->OnReplicationLogRecord(std::move(rlr));
}

Status SerializeCloudManifestDelta(std::string* dst, CloudManifestDelta delta) {
  PutVarint64(dst, delta.file_num);
  PutLengthPrefixedSlice(dst, delta.epoch);
  return Status::OK();
}
Status DeserializeCloudManifestDelta(Slice* src, CloudManifestDelta* delta) {
  uint64_t file_num;
  std::string epoch;
  if (!GetVarint64(src, &file_num)) {
    return Status::Corruption(
        "Unable to decode file_num of cloud manifest delta");
  }

  Slice epoch_slice;
  if (!GetLengthPrefixedSlice(src, &epoch_slice)) {
    return Status::Corruption("Unable to decode epoch of cloud manifest delta");
  }

  delta->file_num = file_num;
  delta->epoch = epoch_slice.ToString();
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
