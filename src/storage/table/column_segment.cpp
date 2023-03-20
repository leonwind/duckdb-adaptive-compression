#include "duckdb/storage/table/column_segment.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"

#include <algorithm>
#include <cstring>
#include <utility>

namespace duckdb {

unique_ptr<ColumnSegment> ColumnSegment::CreatePersistentSegment(DatabaseInstance &db, BlockManager &block_manager,
                                                                 block_id_t block_id, idx_t offset,
                                                                 const LogicalType &type, idx_t start, idx_t count,
                                                                 CompressionType compression_type,
                                                                 unique_ptr<BaseStatistics> statistics) {
	auto &config = DBConfig::GetConfig(db);
	CompressionFunction *function;
	shared_ptr<BlockHandle> block;
	if (block_id == INVALID_BLOCK) {
		// constant segment, no need to allocate an actual block
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, type.InternalType());
	} else {
		function = config.GetCompressionFunction(compression_type, type.InternalType());
		block = block_manager.RegisterBlock(block_id);
	}
	auto segment_size = Storage::BLOCK_SIZE;
	return make_unique<ColumnSegment>(db, move(block), type, ColumnSegmentType::PERSISTENT, start, count, function,
	                                  move(statistics), block_id, offset, segment_size,
	                                  /* succinct_possible= */ false, config.adaptive_succinct_compression_enabled);
}

unique_ptr<ColumnSegment> ColumnSegment::CreateTransientSegment(DatabaseInstance &db, const LogicalType &type,
                                                                idx_t start, idx_t segment_size) {

	auto &config = DBConfig::GetConfig(db);

	CompressionFunction* function;
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	shared_ptr<BlockHandle> block;
	bool succinct_possible = false;

	if (TypeIsInteger(type.InternalType()) && config.succinct_enabled && !config.adaptive_succinct_compression_enabled) {
		//std::cout << "Create SUCCINCT transient segment with size "<< segment_size << std::endl;
		succinct_possible = true;
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_SUCCINCT, type.InternalType());
		block = buffer_manager.RegisterSmallMemory(0);
	} else {
		if (TypeIsInteger(type.InternalType()) && config.succinct_enabled) {
			succinct_possible = true;
		}

		// std::cout << "Create UNCOMPRESSED transient segment with size "<< segment_size << std::endl;
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, type.InternalType());

		// transient: allocate a buffer for the uncompressed segment
		if (segment_size < Storage::BLOCK_SIZE) {
			block = buffer_manager.RegisterSmallMemory(segment_size);
		} else {
			buffer_manager.Allocate(segment_size, false, &block);
		}
		buffer_manager.AddOnlyToDataSize(segment_size);
	}

	return make_unique<ColumnSegment>(db, move(block), type, ColumnSegmentType::TRANSIENT, start, 0, function, nullptr,
	                                  INVALID_BLOCK, 0, segment_size, succinct_possible,
	                                  config.adaptive_succinct_compression_enabled);
}

unique_ptr<ColumnSegment> ColumnSegment::CreateSegment(ColumnSegment &other, idx_t start) {
	return make_unique<ColumnSegment>(other, start);
}

ColumnSegment::ColumnSegment(DatabaseInstance &db, shared_ptr<BlockHandle> block, LogicalType type_p,
                             ColumnSegmentType segment_type, idx_t start, idx_t count, CompressionFunction *function_p,
                             unique_ptr<BaseStatistics> statistics, block_id_t block_id_p, idx_t offset_p,
                             idx_t segment_size_p, bool succinct_possible, bool background_compaction_enabled)
    : SegmentBase(start, count), db(db), type(move(type_p)), type_size(GetTypeIdSize(type.InternalType())),
      segment_type(segment_type), function(function_p), stats(type, move(statistics)), block(move(block)),
      block_id(block_id_p), offset(offset_p), segment_size(segment_size_p),
      num_elements(0), min_factor(UINT64_MAX), max_factor(0), compacted(false),
      succinct_possible(succinct_possible), background_compaction_enabled(background_compaction_enabled),
      column_segment_catalog(Catalog::GetSystemCatalog(db).GetColumnSegmentCatalog()) {
	D_ASSERT(function);

	if (function->type == CompressionType::COMPRESSION_SUCCINCT) {
		//std::cout << "Create SUCCINCT transient segment at " << this << std::endl;
		succinct_vec.width(type_size * 8);
		succinct_vec.resize(segment_size / type_size);
		BufferManager::GetBufferManager(db).AddToDataSize(sdsl::size_in_bytes(succinct_vec));
	}

	if (function->init_segment) {
		segment_state = function->init_segment(*this, block_id);
	}
}

ColumnSegment::ColumnSegment(ColumnSegment &other, idx_t start)
    : SegmentBase(start, other.count), db(other.db), type(move(other.type)), type_size(other.type_size),
      segment_type(other.segment_type), function(other.function), stats(move(other.stats)), block(move(other.block)),
      block_id(other.block_id), offset(other.offset), segment_size(other.segment_size), num_elements(other.num_elements),
      segment_state(move(other.segment_state)), compacted(other.compacted),
      min_factor(other.min_factor), max_factor(other.max_factor),
      column_segment_catalog(other.column_segment_catalog),
      succinct_possible(other.succinct_possible) {

	succinct_vec = std::move(other.succinct_vec);
	column_segment_catalog->AddColumnSegment(this);
}

ColumnSegment::~ColumnSegment() {
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void ColumnSegment::InitializeScan(ColumnScanState &state) {
	state.scan_state = function->init_scan(*this);
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset,
                         bool entire_vector) {
	if (entire_vector) {
		D_ASSERT(result_offset == 0);
		Scan(state, scan_count, result);
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		ScanPartial(state, scan_count, result, result_offset);
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	}
}

void ColumnSegment::Skip(ColumnScanState &state) {
	function->skip(*this, state, state.row_index - state.internal_index);
	state.internal_index = state.row_index;
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t scan_count, Vector &result) {
	if (succinct_possible) {
		column_segment_catalog->AddReadAccess(this);
	}
	//std::cout << "Scan at " << this << std::endl;
	bit_compression_lock.lock();
	function->scan_vector(*this, state, scan_count, result);
	bit_compression_lock.unlock();
	//column_segment_catalog.Print();
}

void ColumnSegment::ScanPartial(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset) {
	if (succinct_possible) {
		column_segment_catalog->AddReadAccess(this);
	}
	//std::cout << "Scan at " << this << std::endl;
	bit_compression_lock.lock();
	function->scan_partial(*this, state, scan_count, result, result_offset);
	bit_compression_lock.unlock();
	//column_segment_catalog.Print();
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ColumnSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	function->fetch_row(*this, state, row_id - this->start, result, result_idx);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
idx_t ColumnSegment::SegmentSize() const {
	return segment_size;
}

void ColumnSegment::Resize(idx_t new_size) {
	// Succinct is read only
	D_ASSERT(function->type != CompressionType::COMPRESSION_SUCCINCT);
	D_ASSERT(new_size > this->segment_size);
	D_ASSERT(offset == 0);
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto old_handle = buffer_manager.Pin(block);
	shared_ptr<BlockHandle> new_block;
	auto new_handle = buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &new_block);
	memcpy(new_handle.Ptr(), old_handle.Ptr(), segment_size);
	this->block_id = new_block->BlockId();
	this->block = move(new_block);
	this->segment_size = new_size;
}

void ColumnSegment::InitializeAppend(ColumnAppendState &state) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function->init_append) {
		throw InternalException("Attempting to init append to a segment without init_append method");
	}
	state.append_state = function->init_append(*this);
}

idx_t ColumnSegment::Append(ColumnAppendState &state, UnifiedVectorFormat &append_data, idx_t offset, idx_t count) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function->append) {
		throw InternalException("Attempting to append to a segment without append method");
	}

	idx_t copy_count = function->append(*state.append_state, *this, stats, append_data, offset, count);
	num_elements += count;

	if (function->type == CompressionType::COMPRESSION_SUCCINCT) {
		if (num_elements >= succinct_vec.size()) {
			Compact();
		}
	}
	return copy_count;
}

void ColumnSegment::Compact() {
	if (compacted || num_elements == 0 || !succinct_possible) {
		return;
	}

	if (function->type == CompressionType::COMPRESSION_SUCCINCT) {
		size_t size_before_compress = sdsl::size_in_bytes(succinct_vec);
		BitCompressFromSuccinct();
		int64_t diff_size = size_before_compress - sdsl::size_in_bytes(succinct_vec);
		BufferManager::GetBufferManager(db).AddToDataSize(-diff_size);
		return;
	}

	//! Segment was uncompressed transient and now gets compacted using a succinct representation.

	succinct_vec.width(type_size * 8);
	succinct_vec.resize(segment_size / type_size);

	size_t size_before_compress = sdsl::size_in_bytes(succinct_vec);

	bit_compression_lock.lock();
	BitCompressFromUncompressed();
	bit_compression_lock.unlock();

	BufferManager::GetBufferManager(db).Unpin(block);

	int64_t diff_size = size_before_compress - sdsl::size_in_bytes(succinct_vec);
	BufferManager::GetBufferManager(db).AddToDataSize(-diff_size);
}

void ColumnSegment::BitCompressFromSuccinct() {
	//std::cout << "Updated min: " << GetMinFactor() << ", Updated max: " << GetMax() << std::endl;
	uint64_t min = GetMinFactor();
	uint64_t max = GetMax() - GetMinFactor();

    uint8_t min_width = sdsl::bits::hi(max) + 1;
	if (DBConfig::GetConfig(db).succinct_padded_to_next_byte_enabled) {
		min_width = ((min_width + 7) & (-8));
		//std::cout << "Padded min width: " << (unsigned) min_width << std::endl;
	}
    uint8_t old_width = succinct_vec.width();

    if (old_width > min_width) {
        const uint64_t* read_data = succinct_vec.data();
        uint64_t* write_data = succinct_vec.data();
        uint8_t read_offset = 0;
        uint8_t write_offset = 0;

        for (size_t i = 0; i < count; ++i) {
            uint64_t x = sdsl::bits::read_int_and_move(read_data, read_offset, old_width);
			if (min != UINT64_MAX) {
				x -= min;
			}
            sdsl::bits::write_int_and_move(write_data, x, write_offset, min_width);
        }

        succinct_vec.bit_resize(count * min_width);
        succinct_vec.width(min_width);
    }
}

void ColumnSegment::BitCompressFromUncompressed() {
	//std::cout << "Before lock" << std::endl;

	auto old_handle = BufferManager::GetBufferManager(db).Pin(block);
	uint8_t* old_data = old_handle.Ptr();
	//std::cout << "After lock" << std::endl;
	uint32_t min = UINT32_MAX; //GetMinFactor();
	uint32_t max = 0; //GetMax() - GetMinFactor();
	for (size_t i = 0; i < count; ++i) {
		uint32_t curr;
		memcpy(/* dest= */ &curr,
		       /* src= */ old_data + i * sizeof(curr),
		       /* n= */ sizeof(curr));
		min = std::min(min, curr);
		max = std::max(max, curr);
	}

    uint8_t min_width = sdsl::bits::hi(max) + 1;
	if (DBConfig::GetConfig(db).succinct_padded_to_next_byte_enabled) {
		min_width = ((min_width + 7) & (-8));
		//std::cout << "Padded min width: " << (unsigned) min_width << std::endl;
	}
    uint8_t old_width = succinct_vec.width();
	//std::cout << "Compact with min " << min << std::endl;
    if (old_width > min_width) {

        //const uint64_t* read_data = succinct_vec.data();
        uint64_t* write_data = succinct_vec.data();
        //uint8_t read_offset = 0;
        uint8_t write_offset = 0;

        for (size_t i = 0; i < count; ++i) {
			int32_t x_flat;
			memcpy(/* dest= */ &x_flat,
			       /* src= */ old_data + i * sizeof(x_flat),
			       /* n= */ sizeof(x_flat));

			//std::cout << "Curr element at " << i << ": " << (int32_t) x_flat << std::endl;
			//uint64_t x = sdsl::bits::read_int_and_move(read_data, read_offset, old_width);
			//std::cout << "Compact entry at " << i << " is " << x << std::endl;

			if (min != UINT32_MAX) {
				x_flat -= min;
			}

            sdsl::bits::write_int_and_move(write_data, (uint64_t) x_flat, write_offset, min_width);
        }

        succinct_vec.bit_resize(count * min_width);
        succinct_vec.width(min_width);
    }

	function = DBConfig::GetConfig(db).GetCompressionFunction(CompressionType::COMPRESSION_SUCCINCT, type.InternalType());
	SetBitCompressed();
	//std::cout << "Finished compression, after unlock" << std::endl;
}

idx_t ColumnSegment::FinalizeAppend(ColumnAppendState &state) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function->finalize_append) {
		throw InternalException("Attempting to call FinalizeAppend on a segment without a finalize_append method");
	}
	//std::cout << "Finalize append" << std::endl;
	auto result_count = function->finalize_append(*this, stats);
	state.append_state.reset();
	return result_count;
}

void ColumnSegment::RevertAppend(idx_t start_row) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (function->revert_append) {
		function->revert_append(*this, start_row);
	}
	this->count = start_row - this->start;
}

//===--------------------------------------------------------------------===//
// Convert To Persistent
//===--------------------------------------------------------------------===//
void ColumnSegment::ConvertToPersistent(BlockManager *block_manager, block_id_t block_id_p) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (function->type == CompressionType::COMPRESSION_SUCCINCT) {
		return;
	}
	segment_type = ColumnSegmentType::PERSISTENT;

	block_id = block_id_p;
	offset = 0;

	D_ASSERT(stats.statistics);
	if (block_id == INVALID_BLOCK) {
		// constant block: reset the block buffer
		D_ASSERT(stats.statistics->IsConstant());
		block.reset();
	} else {
		D_ASSERT(!stats.statistics->IsConstant());
		// non-constant block: write the block to disk
		// the data for the block already exists in-memory of our block
		// instead of copying the data we alter some metadata so the buffer points to an on-disk block
		block = block_manager->ConvertToPersistent(block_id, move(block));
	}

	segment_state.reset();
	if (function->init_segment) {
		segment_state = function->init_segment(*this, block_id);
	}
}

void ColumnSegment::MarkAsPersistent(shared_ptr<BlockHandle> block_p, uint32_t offset_p) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	segment_type = ColumnSegmentType::PERSISTENT;

	block_id = block_p->BlockId();
	offset = offset_p;
	block = move(block_p);

	segment_state.reset();
	if (function->init_segment) {
		segment_state = function->init_segment(*this, block_id);
	}
}

//===--------------------------------------------------------------------===//
// Filter Selection
//===--------------------------------------------------------------------===//
template <class T, class OP, bool HAS_NULL>
static idx_t TemplatedFilterSelection(T *vec, T *predicate, SelectionVector &sel, idx_t approved_tuple_count,
                                      ValidityMask &mask, SelectionVector &result_sel) {
	idx_t result_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		auto idx = sel.get_index(i);
		if ((!HAS_NULL || mask.RowIsValid(idx)) && OP::Operation(vec[idx], *predicate)) {
			result_sel.set_index(result_count++, idx);
		}
	}
	return result_count;
}

template <class T>
static void FilterSelectionSwitch(T *vec, T *predicate, SelectionVector &sel, idx_t &approved_tuple_count,
                                  ExpressionType comparison_type, ValidityMask &mask) {
	SelectionVector new_sel(approved_tuple_count);
	// the inplace loops take the result as the last parameter
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, false>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, true>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_NOTEQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, false>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, true>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, false>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, true>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThan, false>(vec, predicate, sel,
			                                                                       approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThan, true>(vec, predicate, sel,
			                                                                      approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, LessThanEquals, false>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, LessThanEquals, true>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, false>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, true>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	default:
		throw NotImplementedException("Unknown comparison type for filter pushed down to table!");
	}
	sel.Initialize(new_sel);
}

template <bool IS_NULL>
static idx_t TemplatedNullSelection(SelectionVector &sel, idx_t &approved_tuple_count, ValidityMask &mask) {
	if (mask.AllValid()) {
		// no NULL values
		if (IS_NULL) {
			approved_tuple_count = 0;
			return 0;
		} else {
			return approved_tuple_count;
		}
	} else {
		SelectionVector result_sel(approved_tuple_count);
		idx_t result_count = 0;
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			auto idx = sel.get_index(i);
			if (mask.RowIsValid(idx) != IS_NULL) {
				result_sel.set_index(result_count++, idx);
			}
		}
		sel.Initialize(result_sel);
		approved_tuple_count = result_count;
		return result_count;
	}
}

idx_t ColumnSegment::FilterSelection(SelectionVector &sel, Vector &result, const TableFilter &filter,
                                     idx_t &approved_tuple_count, ValidityMask &mask) {
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_OR: {
		// similar to the CONJUNCTION_AND, but we need to take care of the SelectionVectors (OR all of them)
		idx_t count_total = 0;
		SelectionVector result_sel(approved_tuple_count);
		auto &conjunction_or = (ConjunctionOrFilter &)filter;
		for (auto &child_filter : conjunction_or.child_filters) {
			SelectionVector temp_sel;
			temp_sel.Initialize(sel);
			idx_t temp_tuple_count = approved_tuple_count;
			idx_t temp_count = FilterSelection(temp_sel, result, *child_filter, temp_tuple_count, mask);
			// tuples passed, move them into the actual result vector
			for (idx_t i = 0; i < temp_count; i++) {
				auto new_idx = temp_sel.get_index(i);
				bool is_new_idx = true;
				for (idx_t res_idx = 0; res_idx < count_total; res_idx++) {
					if (result_sel.get_index(res_idx) == new_idx) {
						is_new_idx = false;
						break;
					}
				}
				if (is_new_idx) {
					result_sel.set_index(count_total++, new_idx);
				}
			}
		}
		sel.Initialize(result_sel);
		approved_tuple_count = count_total;
		return approved_tuple_count;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and = (ConjunctionAndFilter &)filter;
		for (auto &child_filter : conjunction_and.child_filters) {
			FilterSelection(sel, result, *child_filter, approved_tuple_count, mask);
		}
		return approved_tuple_count;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = (ConstantFilter &)filter;
		// the inplace loops take the result as the last parameter
		switch (result.GetType().InternalType()) {
		case PhysicalType::UINT8: {
			auto result_flat = FlatVector::GetData<uint8_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<uint8_t>(predicate_vector);
			FilterSelectionSwitch<uint8_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::UINT16: {
			auto result_flat = FlatVector::GetData<uint16_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<uint16_t>(predicate_vector);
			FilterSelectionSwitch<uint16_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::UINT32: {
			auto result_flat = FlatVector::GetData<uint32_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<uint32_t>(predicate_vector);
			FilterSelectionSwitch<uint32_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::UINT64: {
			auto result_flat = FlatVector::GetData<uint64_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<uint64_t>(predicate_vector);
			FilterSelectionSwitch<uint64_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT8: {
			auto result_flat = FlatVector::GetData<int8_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<int8_t>(predicate_vector);
			FilterSelectionSwitch<int8_t>(result_flat, predicate, sel, approved_tuple_count,
			                              constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT16: {
			auto result_flat = FlatVector::GetData<int16_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<int16_t>(predicate_vector);
			FilterSelectionSwitch<int16_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT32: {
			auto result_flat = FlatVector::GetData<int32_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<int32_t>(predicate_vector);
			FilterSelectionSwitch<int32_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT64: {
			auto result_flat = FlatVector::GetData<int64_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<int64_t>(predicate_vector);
			FilterSelectionSwitch<int64_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT128: {
			auto result_flat = FlatVector::GetData<hugeint_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<hugeint_t>(predicate_vector);
			FilterSelectionSwitch<hugeint_t>(result_flat, predicate, sel, approved_tuple_count,
			                                 constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::FLOAT: {
			auto result_flat = FlatVector::GetData<float>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<float>(predicate_vector);
			FilterSelectionSwitch<float>(result_flat, predicate, sel, approved_tuple_count,
			                             constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::DOUBLE: {
			auto result_flat = FlatVector::GetData<double>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<double>(predicate_vector);
			FilterSelectionSwitch<double>(result_flat, predicate, sel, approved_tuple_count,
			                              constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::VARCHAR: {
			auto result_flat = FlatVector::GetData<string_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<string_t>(predicate_vector);
			FilterSelectionSwitch<string_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::BOOL: {
			auto result_flat = FlatVector::GetData<bool>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<bool>(predicate_vector);
			FilterSelectionSwitch<bool>(result_flat, predicate, sel, approved_tuple_count,
			                            constant_filter.comparison_type, mask);
			break;
		}
		default:
			throw InvalidTypeException(result.GetType(), "Invalid type for filter pushed down to table comparison");
		}
		return approved_tuple_count;
	}
	case TableFilterType::IS_NULL:
		return TemplatedNullSelection<true>(sel, approved_tuple_count, mask);
	case TableFilterType::IS_NOT_NULL:
		return TemplatedNullSelection<false>(sel, approved_tuple_count, mask);
	default:
		throw InternalException("FIXME: unsupported type for filter selection");
	}
}

} // namespace duckdb
