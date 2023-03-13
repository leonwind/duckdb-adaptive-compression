#include "duckdb/catalog/catalog_entry/column_segment_catalog.hpp"
#include <algorithm>
#include <iostream>
#include <thread>

namespace duckdb {

ColumnSegmentCatalog::ColumnSegmentCatalog():
      statistics(), event_counter(0), background_thread_started(false) {
}

void ColumnSegmentCatalog::AddColumnSegment(ColumnSegment* segment) {
	statistics[segment] = AccessStatistics{/* num_reads= */ 0};
	//std::cout << "Add segment " << &segment << " to access statistics" << std::endl;
	//std::cout << "This pointer in AddColumnSegment: " << this << std::endl;
}

void ColumnSegmentCatalog::AddReadAccess(ColumnSegment* segment) {
	//std::cout << "This pointer in AddReadAccess: " << this << std::endl;
	if (!background_thread_started) {
		std::thread t(&ColumnSegmentCatalog::CompressLowestKSegments, this);
		t.detach();
		background_thread_started = true;
	}

	if (statistics.find(segment) == statistics.end()) {
		statistics[segment] = AccessStatistics{/* num_reads= */ 1};
	} else {
		statistics[segment].num_reads++;
	}
	event_counter++;
}

void ColumnSegmentCatalog::CompressLowestKSegments() {
	while (true) {
		//std::cout << "Event counter: " << event_counter << std::endl;
		if (event_counter < 10000) {
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			continue;
		}

		std::vector<std::pair<ColumnSegment*, AccessStatistics>> v(statistics.begin(), statistics.end());
		std::sort(v.begin(), v.end(),
				  [](std::pair<ColumnSegment*, AccessStatistics>& left,
					 std::pair<ColumnSegment*, AccessStatistics>& right) {
					  return left.second < right.second;
				  });

		float cum_sum = 0;
		for (auto iter = v.begin(); iter != v.end(); iter++) {
			// Bit Compress lowest 70% of the columns
			if (cum_sum / event_counter > 0.7) {
				return;
			}

			cum_sum += iter->second.num_reads;
			iter->first->Compact();
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}
}


void ColumnSegmentCatalog::Print() {
	std::vector<std::pair<ColumnSegment*, AccessStatistics>> v(statistics.begin(), statistics.end());

	std::sort(v.begin(), v.end(),
	          [](std::pair<ColumnSegment*, AccessStatistics>& left,
	             std::pair<ColumnSegment*, AccessStatistics>&right) {
		          // Sort descending
		          return !(left.second < right.second);
	          });

	for (auto& curr : v) {
		std::cout << curr.first << ": " << curr.second.num_reads << std::endl;
	}
}

} // namespace duckdb
