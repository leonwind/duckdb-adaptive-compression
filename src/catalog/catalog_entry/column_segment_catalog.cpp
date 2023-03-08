#include "duckdb/catalog/catalog_entry/column_segment_catalog.hpp"

#include <algorithm>
#include <iostream>

namespace duckdb {

ColumnSegmentCatalog::ColumnSegmentCatalog() {}

void ColumnSegmentCatalog::AddColumnSegment(block_id_t block_id) {
	std::cout << "Add new block id: " << block_id << std::endl;
	statistics[block_id] = AccessStatistics{/* num_reads= */ 0};
}

void ColumnSegmentCatalog::AddReadAccess(block_id_t block_id) {
	std::cout << "Add read access for block id: " << block_id << std::endl;
	statistics[block_id].num_reads++;
}

void ColumnSegmentCatalog::Print() {
	std::vector<std::pair<block_id_t, AccessStatistics>> elems(statistics.begin(), statistics.end());

	std::sort(elems.begin(), elems.end(),
	          [](std::pair<block_id_t , AccessStatistics>& left,
	             std::pair<block_id_t , AccessStatistics>  &right) {
		          return left.second < right.second;
	          });

	for (auto& curr : elems) {
		std::cout << curr.first << ": " << curr.second.num_reads << std::endl;
	}
}

} // namespace duckdb
