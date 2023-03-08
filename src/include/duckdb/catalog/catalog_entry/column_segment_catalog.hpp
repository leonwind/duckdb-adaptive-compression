#pragma once

#include "duckdb.h"
#include "duckdb/storage/storage_info.hpp"
#include <unordered_map>
namespace duckdb {

struct AccessStatistics {
	idx_t num_reads;

	inline bool operator<(AccessStatistics x) {
      	return num_reads < x.num_reads;
    }
};

class ColumnSegmentCatalog {

public:
	ColumnSegmentCatalog();

	void AddColumnSegment(block_id_t block_id);
	void AddReadAccess(block_id_t block_id);
	void Print();

private:
	std::unordered_map<block_id_t, AccessStatistics> statistics;
};

} // namespace duckdb
