#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"

#include <iostream>

using namespace duckdb;

#define NUM_INSERTS 300000

DUCKDB_BENCHMARK(SuccinctBuggyWorking, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE t1(i UINTEGER);");

	Appender appender(state->conn, "t1");
	for (size_t i = 1; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<uint32_t>(i);
		appender.EndRow();
	}

	std::cout << "Used after insert memory: "
	          << state->db.instance->GetBufferManager().GetUsedMemory()
	          << std::endl;
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	state->result = state->conn.Query("SELECT * FROM t1 where i < 10");
	state->conn.Query("COMMIT");

	std::cout << "Used memory: "
	          << state->db.instance->GetBufferManager().GetUsedMemory()
	          << std::endl;
}

string VerifyResult(QueryResult *result) override {
    return result->ToString();
}

string BenchmarkInfo() override {
	return "Run a bulk update using succinct integers";
}

bool InMemory() override {
	return true;
}
FINISH_BENCHMARK(SuccinctBuggyWorking)
