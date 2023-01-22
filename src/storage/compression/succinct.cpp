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

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
template <class T>
void SuccinctScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                         idx_t result_offset) {
	auto start = segment.GetRelativeIndex(state.row_index);
	auto source = segment.succinct_vec;

	std::cout << "Succinct scan: " << scan_count << ", " << result_offset << ", " << start << std::endl;
	std::cout << "Scan size: " << source.size() << std::endl;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	data_ptr_t target_ptr = FlatVector::GetData(result) + result_offset * sizeof(T);
	for (idx_t i = 0; i < scan_count; ++i) {
		target_ptr[i * sizeof(T)] = source[start + i];
	}
	std::cout << "Scanned element " << target_ptr[0] << " from " << source[0] << std::endl;
}

template <class T>
void SuccinctScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	SuccinctScanPartial<T>(segment, state, scan_count, result, /* result_offset= */ 0);
}
//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void SuccinctFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                      idx_t result_idx) {
	std::cout << "Succinct fetch row" << std::endl;
	auto source = segment.succinct_vec;

	data_ptr_t target_ptr = FlatVector::GetData(result) + result_idx * sizeof(T);
	for (idx_t i = 0; i < source.size(); ++i) {
		target_ptr[i * sizeof(T)] = source[i];
	}
}
//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static unique_ptr<CompressionAppendState> SuccinctInitAppend(ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	return make_unique<CompressionAppendState>(move(handle));
}

template <class T>
void SuccinctAppendLoop(SegmentStatistics &stats, sdsl::int_vector<> &target, idx_t target_offset, UnifiedVectorFormat &adata,
                       idx_t offset, idx_t count) {

	auto sdata = (T *)adata.data;
	//uint64_t* target_ptr = target->data();
	//std::cout << "Succinct size " << target.size() << std::endl;
	if (!adata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			bool is_null = !adata.validity.RowIsValid(source_idx);
			if (!is_null) {
				NumericStatistics::Update<T>(stats, sdata[source_idx]);
				//std::cout << "Target idx: " << target_idx << std::endl;
				std::cout << "Source: " << sdata[source_idx] << std::endl;
				target[target_idx] = sdata[source_idx];
				std::cout << "Stored " << target[target_idx] << " at " << target_idx << std::endl;
				//std::cout << "Post seg fault" << std::endl;
			} else {
				// we insert a NullValue<T> in the null gap for debuggability
				// this value should never be used or read anywhere
				target[target_idx] = NullValue<T>();
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			NumericStatistics::Update<T>(stats, sdata[source_idx]);
			std::cout << "Source: " << sdata[source_idx] << std::endl;
			target[target_idx] = sdata[source_idx];
		}
	}
}

template <class T>
idx_t SuccinctAppend(CompressionAppendState &append_state, ColumnSegment &segment, SegmentStatistics &stats,
                      UnifiedVectorFormat &data, idx_t offset, idx_t count) {
	D_ASSERT(segment.GetBlockOffset() == 0);
	//std::cout << "Start succinct append" << std::endl;

	idx_t max_tuple_count = segment.SegmentSize() / sizeof(T);
	idx_t copy_count = MinValue<idx_t>(count, max_tuple_count - segment.count);

	SuccinctAppendLoop<T>(stats, segment.succinct_vec, segment.count, data, offset, copy_count);
	segment.count += copy_count;
	return copy_count;
}

template <class T>
idx_t SuccinctFinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats) {
	sdsl::util::bit_compress(segment.succinct_vec);
	std::cout << "Finalize succinct append" << std::endl;
	return segment.count * sizeof(T);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T>
CompressionFunction SuccinctGetFunction(PhysicalType type) {
	return CompressionFunction(CompressionType::COMPRESSION_SUCCINCT, type, FixedSizeInitAnalyze,
	                           FixedSizeAnalyze, FixedSizeFinalAnalyze<T>, UncompressedFunctions::InitCompression,
	                           UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress,
	                           FixedSizeInitScan, SuccinctScan<T>, SuccinctScanPartial<T>, SuccinctFetchRow<T>,
	                           UncompressedFunctions::EmptySkip, nullptr, SuccinctInitAppend, SuccinctAppend<T>,
	                           SuccinctFinalizeAppend<T>, nullptr);
}

CompressionFunction SuccinctFun::GetFunction(PhysicalType data_type) {
	switch (data_type) {
	case PhysicalType::UINT8:
		return SuccinctGetFunction<uint8_t>(data_type);
	case PhysicalType::UINT16:
		return SuccinctGetFunction<uint16_t>(data_type);
	case PhysicalType::UINT32:
		return SuccinctGetFunction<uint32_t>(data_type);
	case PhysicalType::UINT64:
		return SuccinctGetFunction<uint64_t>(data_type);
	default:
		throw InternalException("Unsupported type for FixedSizeUncompressed::GetFunction");
	}
}

bool SuccinctFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
		/*
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
		 */
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