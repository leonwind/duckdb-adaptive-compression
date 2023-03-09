#include "duckdb/catalog/catalog_entry/column_segment_catalog.hpp"

#include <algorithm>
#include <iostream>

namespace duckdb {

ColumnSegmentCatalog::ColumnSegmentCatalog(): statistics(), event_counter(0) {}

void ColumnSegmentCatalog::AddColumnSegment(uintptr_t block_id) {
	statistics[block_id] = AccessStatistics{/* num_reads= */ 0};
}

void ColumnSegmentCatalog::AddReadAccess(uintptr_t block_id) {
	statistics[block_id].num_reads++;
	event_counter++;
}

void ColumnSegmentCatalog::Print() {
	std::vector<std::pair<block_id_t, AccessStatistics>> v(statistics.begin(), statistics.end());

	std::sort(v.begin(), v.end(),
	          [](std::pair<block_id_t, AccessStatistics>& left,
	             std::pair<block_id_t, AccessStatistics>&right) {
		          return left.second < right.second;
	          });

	for (auto& curr : v) {
		std::cout << curr.first << ": " << curr.second.num_reads << std::endl;
	}
}

} // namespace duckdb
