#include "duckdb/catalog/catalog_entry/column_segment_catalog.hpp"
#include <algorithm>
#include <iostream>
#include <thread>

namespace duckdb {

ColumnSegmentCatalog::ColumnSegmentCatalog():
      statistics(), event_counter(0), background_thread_started(false),
      background_compaction_enabled(false) {
}

void ColumnSegmentCatalog::AddColumnSegment(ColumnSegment* segment) {
	if (segment->succinct_possible) {
		statistics[segment] = AccessStatistics{/* num_reads= */ 0};
		//std::cout << "Add segment " << &segment << " to access statistics" << std::endl;
		//std::cout << "This pointer in AddColumnSegment: " << this << std::endl;
	}
}

void ColumnSegmentCatalog::AddReadAccess(ColumnSegment* segment) {
	//std::cout << "This pointer in AddReadAccess: " << this << std::endl;
	if (background_compaction_enabled && !background_thread_started) {
		background_thread_started = true;
		std::thread t(&ColumnSegmentCatalog::CompressLowestKSegments, this);
		t.detach();
	}

	if (statistics.find(segment) == statistics.end() || segment == nullptr) {
		//statistics[segment] = AccessStatistics{/* num_reads= */ 1};
		if (segment->succinct_possible) {
			std::cout << "SHOULD NEVER HAPPEN NOW???" << std::endl;
		}
	} else {
		statistics[segment].num_reads++;
		event_counter++;
	}
}

void ColumnSegmentCatalog::CompressLowestKSegments() {
	bool finished = false;
	while (!finished) {
		std::this_thread::sleep_for(std::chrono::seconds(10));

		//std::cout << "Event counter: " << event_counter << std::endl;
		if (event_counter < 50000) {
			continue;
		}

		idx_t curr_counter{event_counter};
		std::vector<std::pair<ColumnSegment*, AccessStatistics>> v(statistics.begin(), statistics.end());
		std::sort(v.begin(), v.end(),
				  [](std::pair<ColumnSegment*, AccessStatistics>& left,
					 std::pair<ColumnSegment*, AccessStatistics>& right) {
					  return left.second < right.second;
				  });

		float cum_sum = 0;
		for (auto iter = v.begin(); iter != v.end(); iter++) {
			if (iter->first->IsBitCompressed()) {
				continue;
			}

			cum_sum += iter->second.num_reads;
			if (cum_sum / curr_counter < 0.7) {
				iter->first->Compact();
			} else {
				finished = true;
				break;
			}
		}
	}

	while (true && false) {
		Print();
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	}
}


void ColumnSegmentCatalog::Print() {
	std::vector<std::pair<ColumnSegment*, AccessStatistics>> v(statistics.begin(), statistics.end());

	std::sort(v.begin(), v.end(),
	          [](std::pair<ColumnSegment*, AccessStatistics>& left,
	             std::pair<ColumnSegment*, AccessStatistics>& right) {
		          std::cout << "Left: " << left.second.num_reads << ", Right: " << right.second.num_reads << std::endl;
		          // Sort descending
		          return !(left.second < right.second);
	          });

	idx_t curr_counter{event_counter};
	for (auto& curr : v) {
		std::cout << curr.first << ": "
		          << curr.second.num_reads << "/" << curr_counter
		          << ", compacted: "  << curr.first->IsBitCompressed() << std::endl;
	}
}

} // namespace duckdb
