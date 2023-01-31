#include "duckdb/function/compression/compression.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include <sdsl/vectors.hpp>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct SuccinctAnalyzeState : public AnalyzeState {
	SuccinctAnalyzeState() : count(0) {
	}

	idx_t count;
};

unique_ptr<AnalyzeState> SuccinctInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<SuccinctAnalyzeState>();
}

bool SuccinctAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = (SuccinctAnalyzeState &)state_p;
	state.count += count;
	return true;
}

template <class T>
idx_t SuccinctFinalAnalyze(AnalyzeState &state_p) {
	auto &state = (SuccinctAnalyzeState &)state_p;
	return sizeof(T) * state.count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct SuccinctCompressState : public CompressionState {
	explicit SuccinctCompressState(ColumnDataCheckpointer &checkpointer);

	ColumnDataCheckpointer &checkpointer;
	unique_ptr<ColumnSegment> current_segment;
	ColumnAppendState append_state;

	virtual void CreateEmptySegment(idx_t row_start);
	void FlushSegment(idx_t segment_size);
	void Finalize(idx_t segment_size);
};

SuccinctCompressState::SuccinctCompressState(ColumnDataCheckpointer &checkpointer)
    : checkpointer(checkpointer) {
	CreateEmptySegment(checkpointer.GetRowGroup().start);
}

void SuccinctCompressState::CreateEmptySegment(idx_t row_start) {
	auto &db = checkpointer.GetDatabase();
	auto &type = checkpointer.GetType();
	auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
	if (type.InternalType() == PhysicalType::VARCHAR) {
		auto &state = (UncompressedStringSegmentState &)*compressed_segment->GetSegmentState();
		state.overflow_writer = make_unique<WriteOverflowStringsToDisk>(checkpointer.GetColumnData().block_manager);
	}
	current_segment = move(compressed_segment);
	current_segment->InitializeAppend(append_state);
}

void SuccinctCompressState::FlushSegment(idx_t segment_size) {
	auto &state = checkpointer.GetCheckpointState();
	state.FlushSegment(move(current_segment), segment_size);
}

void SuccinctCompressState::Finalize(idx_t segment_size) {
	FlushSegment(segment_size);
	current_segment.reset();
}

unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
                                                                    unique_ptr<AnalyzeState> state) {
	return make_unique<SuccinctCompressState>(checkpointer);
}

void Compress(CompressionState &state_p, Vector &data, idx_t count) {
	//std::cout << "Compress" << std::endl;
	auto &state = (SuccinctCompressState &)state_p;
	UnifiedVectorFormat vdata;
	data.ToUnifiedFormat(count, vdata);

	idx_t offset = 0;
	while (count > 0) {
		idx_t appended = state.current_segment->Append(state.append_state, vdata, offset, count);
		if (appended == count) {
			// appended everything: finished
			return;
		}
		auto next_start = state.current_segment->start + state.current_segment->count;
		// the segment is full: flush it to disk
		state.FlushSegment(state.current_segment->FinalizeAppend(state.append_state));

		// now create a new segment and continue appending
		state.CreateEmptySegment(next_start);
		offset += appended;
		count -= appended;
	}
}

void FinalizeCompress(CompressionState &state_p) {
	auto &state = (SuccinctCompressState &)state_p;
	state.Finalize(state.current_segment->FinalizeAppend(state.append_state));
}
//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct SuccinctScanState : public SegmentScanState {
	BufferHandle handle;
};

unique_ptr<SegmentScanState> SuccinctScanState(ColumnSegment &segment) {
	auto result = make_unique<FixedSizeScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return move(result);
}

template <class T>
void SuccinctScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                         idx_t result_offset) {

	auto &scan_state = (FixedSizeScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	// copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);
	memcpy(FlatVector::GetData(result) + result_offset * sizeof(T), source_data, scan_count * sizeof(T));
}

template <class T>
void SuccinctScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	auto &scan_state = (FixedSizeScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	if (std::is_same<T, list_entry_t>()) {
		// list columns are modified in-place during the scans to correct the offsets
		// so we can't do a zero-copy there
		memcpy(FlatVector::GetData(result), source_data, scan_count * sizeof(T));
	} else {
		FlatVector::SetData(result, source_data);
	}
}
//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void SuccinctFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                      idx_t result_idx) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);

	// first fetch the data from the base table
	auto data_ptr = handle.Ptr() + segment.GetBlockOffset() + row_id * sizeof(T);

	memcpy(FlatVector::GetData(result) + result_idx * sizeof(T), data_ptr, sizeof(T));
}
//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static unique_ptr<CompressionAppendState> SuccinctInitAppend(ColumnSegment &segment) {
	//std::cout << "Init append" << std::endl;
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	return make_unique<CompressionAppendState>(move(handle));
}

template <class T>
void SuccinctAppendLoop(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, UnifiedVectorFormat &adata,
                       idx_t offset, idx_t count) {
	auto sdata = (T *)adata.data;
	auto tdata = (T *)target;
	if (!adata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			bool is_null = !adata.validity.RowIsValid(source_idx);
			if (!is_null) {
				NumericStatistics::Update<T>(stats, sdata[source_idx]);
				tdata[target_idx] = sdata[source_idx];
			} else {
				// we insert a NullValue<T> in the null gap for debuggability
				// this value should never be used or read anywhere
				tdata[target_idx] = NullValue<T>();
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			NumericStatistics::Update<T>(stats, sdata[source_idx]);
			tdata[target_idx] = sdata[source_idx];
		}
	}
}

template <class T>
idx_t SuccinctAppend(CompressionAppendState &append_state, ColumnSegment &segment, SegmentStatistics &stats,
                      UnifiedVectorFormat &data, idx_t offset, idx_t count) {
	D_ASSERT(segment.GetBlockOffset() == 0);
	auto target_ptr = append_state.handle.Ptr();
	idx_t max_tuple_count = segment.SegmentSize() / sizeof(T);
	idx_t copy_count = MinValue<idx_t>(count, max_tuple_count - segment.count);

	SuccinctAppendLoop<T>(stats, target_ptr, segment.count, data, offset, copy_count);
	segment.count += copy_count;
	return copy_count;
}

template <class T>
idx_t SuccinctFinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats) {
	//std::cout << "Compact succinct vector" << std::endl;
	//sdsl::util::bit_compress(segment.succinct_vec);
	//std::cout << "Finalize succinct append" << std::endl;
	return segment.count * sizeof(T);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T>
CompressionFunction SuccinctGetFunction(PhysicalType type) {
	return CompressionFunction(CompressionType::COMPRESSION_SUCCINCT, type, FixedSizeInitAnalyze,
	                           FixedSizeAnalyze, FixedSizeFinalAnalyze<T>, InitCompression,
	                           Compress, FinalizeCompress,
	                           FixedSizeInitScan, SuccinctScan<T>, SuccinctScanPartial<T>, SuccinctFetchRow<T>,
	                           UncompressedFunctions::EmptySkip, nullptr, SuccinctInitAppend, SuccinctAppend<T>,
	                           SuccinctFinalizeAppend<T>, nullptr);
}

CompressionFunction SuccinctFun::GetFunction(PhysicalType data_type) {
	switch (data_type) {
	case PhysicalType::INT8:
	case PhysicalType::UINT8:
		return SuccinctGetFunction<uint8_t>(data_type);
	case PhysicalType::INT16:
	case PhysicalType::UINT16:
		return SuccinctGetFunction<uint16_t>(data_type);
	case PhysicalType::INT32:
	case PhysicalType::UINT32:
		return SuccinctGetFunction<uint32_t>(data_type);
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
		return SuccinctGetFunction<uint64_t>(data_type);
	default:
		throw InternalException("Unsupported type for FixedSizeSuccinct::GetFunction");
	}
}

bool SuccinctFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
		return true;
	default:
		return false;
	}
}

}