#pragma once

#include "duckdb.h"
#include "duckdb/storage/storage_info.hpp"
#include <unordered_map>
namespace duckdb {

struct AccessStatistics {
	idx_t num_reads;

	inline bool operator<(AccessStatistics x) {
      	return x.num_reads < num_reads;
    }
};

class ColumnSegmentCatalog {

public:
	ColumnSegmentCatalog();

	void AddColumnSegment(uintptr_t block_id);
	void AddReadAccess(uintptr_t block_id);
	void Print();

private:
	std::unordered_map<block_id_t, AccessStatistics> statistics;
};

} // namespace duckdb
