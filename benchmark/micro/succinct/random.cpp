#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"

#include <iostream>

using namespace duckdb;

#define NUM_INSERTS 1000000

DUCKDB_BENCHMARK(SuccinctRandomInsert, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	srand((unsigned) time(nullptr));

	state->conn.Query("CREATE TABLE t1(i INTEGER);");
	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(rand() % NUM_INSERTS);
		appender.EndRow();
	}

	std::cout << "Used after insert memory: "
	          << state->db.instance->GetBufferManager().GetUsedMemory()
	          << std::endl;
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	state->result = state->conn.Query("SELECT * FROM t1");
	state->conn.Query("COMMIT");
	std::cout << "Used memory: "
	          << state->db.instance->GetBufferManager().GetUsedMemory()
	          << ", Data allocated: "
	          << state->db.instance->GetBufferManager().GetDataSize()
	          << std::endl;
}

string VerifyResult(QueryResult *result) override {
    return string();
}

string BenchmarkInfo() override {
	return "Run a bulk update using succinct integers";
}

bool InMemory() override {
	return true;
}
FINISH_BENCHMARK(SuccinctRandomInsert)

DUCKDB_BENCHMARK(NonSuccinctRandomInsert, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	srand((unsigned) time(nullptr));
	state->db.instance->GetBufferManager().DisableSuccinct();

	state->conn.Query("CREATE TABLE t1(i INTEGER);");
	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(random() % NUM_INSERTS);
		appender.EndRow();
	}

	std::cout << "Used after insert memory: "
	          << state->db.instance->GetBufferManager().GetUsedMemory()
	          << std::endl;
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	state->result = state->conn.Query("SELECT * FROM t1");
	state->conn.Query("COMMIT");
	std::cout << "Used memory: "
	          << state->db.instance->GetBufferManager().GetUsedMemory()
	          << ", Data allocated: "
	          << state->db.instance->GetBufferManager().GetDataSize()
	          << std::endl;
}

string VerifyResult(QueryResult *result) override {
    return string();
}

string BenchmarkInfo() override {
	return "Run a bulk update using succinct integers";
}

bool InMemory() override {
	return true;
}
FINISH_BENCHMARK(NonSuccinctRandomInsert)