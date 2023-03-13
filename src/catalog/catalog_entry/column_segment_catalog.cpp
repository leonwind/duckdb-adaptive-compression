#include "duckdb/catalog/catalog_entry/column_segment_catalog.hpp"
#include <algorithm>
#include <iostream>
#include <thread>

namespace duckdb {

ColumnSegmentCatalog::ColumnSegmentCatalog():
      statistics(), event_counter(0), background_thread_started(false) {
}

void ColumnSegmentCatalog::AddColumnSegment(ColumnSegment* segment) {
	if (segment->function->type == CompressionType::COMPRESSION_SUCCINCT) {
		statistics[segment] = AccessStatistics{/* num_reads= */ 0};
		//std::cout << "Add segment " << &segment << " to access statistics" << std::endl;
		//std::cout << "This pointer in AddColumnSegment: " << this << std::endl;
	}
}

void ColumnSegmentCatalog::AddReadAccess(ColumnSegment* segment) {
	//std::cout << "This pointer in AddReadAccess: " << this << std::endl;
	if (!background_thread_started) {
		std::thread t(&ColumnSegmentCatalog::CompressLowestKSegments, this);
		t.detach();
		background_thread_started = true;
	}

	if (statistics.find(segment) == statistics.end()) {
		//statistics[segment] = AccessStatistics{/* num_reads= */ 1};
		if (segment->function->type == CompressionType::COMPRESSION_SUCCINCT) {
			std::cout << "SHOULD NEVER HAPPEN NOW???" << std::endl;
		}
	} else {
		statistics[segment].num_reads++;
		event_counter++;
	}
}

void ColumnSegmentCatalog::CompressLowestKSegments() {
	while (true) {
		//std::cout << "Event counter: " << event_counter << std::endl;
		if (event_counter < 50000) {
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
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
				Print();
				return;
			}
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

	idx_t curr_counter{event_counter};
	for (auto& curr : v) {
		std::cout << curr.first << ": " << curr.second.num_reads
		          << " / " << curr_counter
		          <<", compacted: "  << curr.first->IsBitCompressed() << std::endl;
	}
}

} // namespace duckdb
