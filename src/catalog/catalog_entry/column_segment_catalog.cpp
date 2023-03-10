#include "duckdb/catalog/catalog_entry/column_segment_catalog.hpp"

#include <algorithm>
#include <iostream>

namespace duckdb {

ColumnSegmentCatalog::ColumnSegmentCatalog(): statistics(), event_counter(0) {}

void ColumnSegmentCatalog::AddColumnSegment(ColumnSegment* segment) {
	statistics[segment] = AccessStatistics{/* num_reads= */ 0};
}

void ColumnSegmentCatalog::AddReadAccess(ColumnSegment* segment) {
	statistics[segment].num_reads++;
	event_counter++;
}

void ColumnSegmentCatalog::CompressLowestKSegments() {
	std::vector<std::pair<ColumnSegment*, AccessStatistics>> v(statistics.begin(), statistics.end());

	std::sort(v.begin(), v.end(),
	          [](std::pair<ColumnSegment*, AccessStatistics>& left,
	             std::pair<ColumnSegment*, AccessStatistics>&right) {
		          return left.second < right.second;
	          });

	float cum_sum = 0;
	for (auto iter = v.begin(); iter != v.end(); iter++) {
		cum_sum += iter->second.num_reads;
		if (cum_sum / event_counter > 0.7 && !iter->first->IsBitCompressed()) {
			iter->first->Compact();
		}
	}
}


void ColumnSegmentCatalog::Print() {
	std::vector<std::pair<ColumnSegment*, AccessStatistics>> v(statistics.begin(), statistics.end());

	std::sort(v.begin(), v.end(),
	          [](std::pair<ColumnSegment*, AccessStatistics>& left,
	             std::pair<ColumnSegment*, AccessStatistics>&right) {
		          return left.second < right.second;
	          });

	for (auto& curr : v) {
		std::cout << curr.first << ": " << curr.second.num_reads << std::endl;
	}
}


} // namespace duckdb
