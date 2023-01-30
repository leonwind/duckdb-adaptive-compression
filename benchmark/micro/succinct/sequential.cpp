#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"

#include <iostream>

using namespace duckdb;

#define NUM_INSERTS 10

DUCKDB_BENCHMARK(SuccinctSequentialInsert, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE t1(i INTEGER);");

	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.EndRow();
	}

	std::cout << "Used after insert memory: "
	          << state->db.instance->GetBufferManager().GetUsedMemory()
	          << std::endl;
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	auto query_string = "SELECT * FROM t1 where i < 5";
	std::cout << "Query string: " << query_string << std::endl;
	state->conn.Query("BEGIN TRANSACTION");
	state->result = state->conn.Query(query_string);
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
FINISH_BENCHMARK(SuccinctSequentialInsert)

DUCKDB_BENCHMARK(NonSuccinctSequentialInsert, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->GetBufferManager().DisableSuccinct();

	state->conn.Query("CREATE TABLE t1(i INTEGER);");
	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.EndRow();
	}

	std::cout << "Used after insert memory: "
	          << state->db.instance->GetBufferManager().GetUsedMemory()
	          << std::endl;
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	auto query_string = "SELECT * FROM t1 where i < 5";
	std::cout << "Query string: " << query_string << std::endl;
	state->conn.Query("BEGIN TRANSACTION");
	state->result = state->conn.Query(query_string);
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
FINISH_BENCHMARK(NonSuccinctSequentialInsert)