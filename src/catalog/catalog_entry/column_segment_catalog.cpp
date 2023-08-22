#include "duckdb/catalog/catalog_entry/column_segment_catalog.hpp"
#include <algorithm>
#include <iostream>
#include <thread>

namespace duckdb {

ColumnSegmentCatalog::ColumnSegmentCatalog():
      statistics(), event_counter(0), background_thread_started(false),
      background_compaction_enabled(false) {
}

void ColumnSegmentCatalog::EnableBackgroundThreadCompaction() {
	if (!background_compaction_enabled) {
		background_compaction_enabled = true;
		std::cout << "START BACKGROUND COMPACTION at " << this << std::endl;

		background_thread_started = true;
		std::thread t(&ColumnSegmentCatalog::CompressLowestKSegments, this);
		t.detach();
	}
}

void ColumnSegmentCatalog::AddColumnSegment(ColumnSegment* segment) {
	if (segment->is_data_segment) {
		statistics[segment] = AccessStatistics{/* num_reads= */ 0};
	}

	//std::cout << "Add segment " << &segment << " to access statistics" << std::endl;
	//std::cout << "This pointer in AddColumnSegment: " << this << std::endl;
}

void ColumnSegmentCatalog::RemoveColumnSegment(ColumnSegment* segment) {
	statistics.erase(segment);
}

void ColumnSegmentCatalog::AddReadAccess(ColumnSegment* segment) {
	if (segment == nullptr || !segment->is_data_segment) {
		//std::cout << "Add read access but early return" << std::endl;
		return;
	}

	if (statistics.find(segment) == statistics.end()) {
		statistics[segment] = AccessStatistics{/* num_reads= */ 1};
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

void ColumnSegmentCatalog::CompactAllSegments() {
	for (auto iter = statistics.begin(); iter != statistics.end(); ++iter) {
		iter->first->Compact();
	}
	//Print();
}

void ColumnSegmentCatalog::CompressLowestKSegments() {
	double compression_rate = 0.90;
	//bool finished = false;
	//size_t i = 0;
	//std::cout << "Start background compaction" << std::endl;

	while (true) {
		std::this_thread::sleep_for(std::chrono::seconds(10));
		std::cout << "COMPRESS " << statistics.size() << " segments" << std::endl;

		/*
		if (event_counter < 50000) {
			continue;
		}
		*/

		idx_t curr_counter{event_counter};
		std::vector<std::pair<ColumnSegment*, AccessStatistics>> v(statistics.begin(), statistics.end());
		std::sort(v.begin(), v.end(),
				  [](std::pair<ColumnSegment*, AccessStatistics>& left,
					 std::pair<ColumnSegment*, AccessStatistics>& right) {
					  return left.second < right.second;
				  });

		//Print();

		float cum_sum = 0;
		curr_counter = v.size();
		for (auto iter = v.begin(); iter != v.end(); iter++) {
			cum_sum += 1;
			//cum_sum += iter->second.num_reads;

			if (cum_sum / curr_counter < compression_rate) {
				//! Compact all the least accessed segments with a ratio of #compression_rate.
				//std::cout << "Before compact" << std::endl;
				iter->first->Compact();
				//std::cout << "After compact" << std::endl;
			} else {
				//! Uncompact / leave uncompressed all frequently accessed segments.
				//std::cout << "Before uncompact" << std::endl;
				iter->first->Uncompact();
				//std::cout << "After uncompact" << std::endl;
			}

			// Reset statistics to store only access patterns since last compacting iteration.
			statistics[iter->first].num_reads = 0;
			//iter->second.num_reads = 0;
		}
		event_counter = 0;

		//std::cout << "\nFINISHED COMPACTION ROUND\n" << std::endl;
		//std::cout << "Num segments: " << v.size() << std::endl;
	}

	std::cout << "\n\n\n#####AFTER WHILE TRUE LOOP??######\n\n\n" << std::endl;

	//Print();

	while (true && false) {
		//break;
		Print();
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	}
}

size_t ColumnSegmentCatalog::GetTotalDataSize() {
	size_t data_size = 0;
	for (auto iter = statistics.begin(); iter != statistics.end(); ++iter) {
		data_size += iter->first->GetDataSize();
	}
	return data_size;
}


void ColumnSegmentCatalog::Print() {
	idx_t curr_counter{event_counter};
	std::vector<std::pair<ColumnSegment*, AccessStatistics>> v(statistics.begin(), statistics.end());
	std::sort(v.begin(), v.end(),
			  [](std::pair<ColumnSegment*, AccessStatistics>& left,
				 std::pair<ColumnSegment*, AccessStatistics>& right) {
				  return left.second < right.second;
			  });

	size_t segment_sizes = 0;
	size_t compressed_size = 0;
	size_t succinct_size = 0;

	float cum_sum = 0.0;
	for (auto& curr : v) {
		cum_sum += curr.second.num_reads;
		if (cum_sum == 0.0) {
			continue;
		}

		std::cout << curr.first << ": "
		          << curr.second.num_reads << "/" << curr_counter
		          << ", compacted: "  << curr.first->IsBitCompressed()
		          << ", cum ratio: " << cum_sum / curr_counter
		          << ", size: " << curr.first->GetDataSize()
		          << ", ratio: " << double(curr.first->GetDataSize()) / curr.first->SegmentSize()
		          << ", stats: " << curr.first->stats.statistics->ToString();

		/*
		if (curr.first->stats.statistics) {
			std::cout << ", stats: " << curr.first->stats.statistics->ToString();
		}
		 */

		std::cout << std::endl;
		          //<< ", stats: " << curr.first->stats.statistics->ToString()
		          //<< std::endl;

		segment_sizes += curr.first->SegmentSize();
		compressed_size += curr.first->GetDataSize();
		succinct_size += curr.first->SuccinctSize();
	}

	std::cout << "Segment size: " << segment_sizes << std::endl;
	std::cout << "Compressed size: " << compressed_size << std::endl;
	std::cout << "Succinct size: " << succinct_size << std::endl;

	std::cout << "######" << std::endl;
}

} // namespace duckdb
