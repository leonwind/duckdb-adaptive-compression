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
		/*
		if (segment->succinct_possible) {
			std::cout << "SHOULD NEVER HAPPEN NOW???" << std::endl;
		}
		 */
	} else {
		statistics[segment].num_reads++;
		event_counter++;
	}
}

void ColumnSegmentCatalog::CompressLowestKSegments() {
	double compression_rate = 0.9;
	//bool finished = false;
	//size_t i = 0;
	std::cout << "Start background compaction" << std::endl;

	while (true) {
		std::this_thread::sleep_for(std::chrono::seconds(10));

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

		//Print();

		float cum_sum = 0;
		for (auto iter = v.begin(); iter != v.end(); iter++) {
			cum_sum += iter->second.num_reads;

			if (cum_sum / curr_counter < compression_rate) {
				// Compact all the least accessed segments with a ratio of #compression_rate.
				iter->first->Compact();
			} else {
				// Uncompact / leave uncompressed all frequently accessed segments.
				iter->first->Uncompact();
			}

			// Reset statistics to store only access patterns since last compacting iteration.
			statistics[iter->first].num_reads = 0;
			//iter->second.num_reads = 0;
		}

		event_counter = 0;
	}

	//Print();

	while (true) {
		//break;
		Print();
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	}
}


void ColumnSegmentCatalog::Print() {
	idx_t curr_counter{event_counter};
	std::vector<std::pair<ColumnSegment*, AccessStatistics>> v(statistics.begin(), statistics.end());
	std::sort(v.begin(), v.end(),
			  [](std::pair<ColumnSegment*, AccessStatistics>& left,
				 std::pair<ColumnSegment*, AccessStatistics>& right) {
				  return left.second < right.second;
			  });

	for (auto& curr : v) {
		std::cout << curr.first << ": "
		          << curr.second.num_reads << "/" << curr_counter
		          << ", compacted: "  << curr.first->IsBitCompressed()
		          << ", size: " << curr.first->SegmentSize()
		          << ", stats: " << curr.first->stats.statistics->ToString()
		          << std::endl;
	}

	std::cout << "######" << std::endl;
}

} // namespace duckdb
