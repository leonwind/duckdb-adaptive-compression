#pragma once

#include <unordered_map>

namespace duckdb {
class ColumnSegment;
class ColumnSegmentCatalog;

struct AccessStatistics {
	idx_t num_reads;

	inline bool operator<(AccessStatistics x) {
      	return x.num_reads < num_reads;
    }
};

class ColumnSegmentCatalog {
public:
	ColumnSegmentCatalog();

	void AddColumnSegment(ColumnSegment* segment);
	void AddReadAccess(ColumnSegment* segment);
	void Print();

	void CompressLowestKSegments();

private:
	std::unordered_map<ColumnSegment*, AccessStatistics> statistics;
	idx_t event_counter;
};

} // namespace duckdb
