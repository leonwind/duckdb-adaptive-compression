#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "zipf.cpp"
#include <iostream>
#include <chrono>

using namespace duckdb;

#define NUM_INSERTS 10000000
#define NUM_LOOKUPS 1000

DUCKDB_BENCHMARK(DistributionChanging, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.adaptive_succinct_compression_enabled = true;
	state->conn.Query("SET threads TO 8;");
	state->conn.Query("CREATE TABLE t1(i UINTEGER);");

	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<uint32_t>(i);
		appender.EndRow();
	}
	appender.Close();
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	auto start = std::chrono::steady_clock::now();
	auto first_duration = std::chrono::seconds(13);
	auto second_duration = std::chrono::seconds(15);

	while (std::chrono::steady_clock::now() - start < first_duration) {
		state->conn.Query("BEGIN TRANSACTION");
		auto query_string = "SELECT t1.i FROM t1 where i == 0";
		state->result = state->conn.Query(query_string);
		state->conn.Query("COMMIT");
	}

	start = std::chrono::steady_clock::now();
	while (std::chrono::steady_clock::now() - start < second_duration) {
		state->conn.Query("BEGIN TRANSACTION");
		auto query_string = "SELECT t1.i FROM t1 where i == " +
		                    std::to_string(NUM_INSERTS - 1);
		state->result = state->conn.Query(query_string);
		state->conn.Query("COMMIT");
	}

	start = std::chrono::steady_clock::now();
	while (std::chrono::steady_clock::now() - start < second_duration) {
		state->conn.Query("BEGIN TRANSACTION");
		auto query_string = "SELECT t1.i FROM t1 where i == " +
		                    std::to_string(NUM_INSERTS - 1);
		state->result = state->conn.Query(query_string);
		state->conn.Query("COMMIT");
	}
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
FINISH_BENCHMARK(DistributionChanging)
