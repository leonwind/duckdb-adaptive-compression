#pragma once

#include "duckdb.h"

#include <atomic>
#include <unordered_map>

namespace duckdb {
class ColumnSegment;
class ColumnSegmentCatalog;

struct AccessStatistics {
	idx_t num_reads;

	inline bool operator<(AccessStatistics x) {
      	return num_reads < x.num_reads;
    }
};

class ColumnSegmentCatalog {
public:
	ColumnSegmentCatalog();

	void AddColumnSegment(ColumnSegment* segment);
	void AddReadAccess(ColumnSegment* segment);
	void Print();

	[[noreturn]] void CompressLowestKSegments();

	inline void EnableBackgroundThreadCompaction() {
		background_compaction_enabled = true;
	}

	inline bool BackgroundCompactionEnabled() {
		return background_compaction_enabled;
	}

	void CompactAllSegments();

	size_t GetTotalDataSize();

private:
	std::unordered_map<ColumnSegment*, AccessStatistics> statistics;
	idx_t event_counter;
	bool background_thread_started;
	bool background_compaction_enabled;
	idx_t skip_length_mask = 8 - 1;
};

} // namespace duckdb
