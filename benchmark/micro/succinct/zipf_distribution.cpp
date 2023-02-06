#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "zipf.cpp"

#include <iostream>

using namespace duckdb;

#define NUM_INSERTS 1000000
#define NUM_LOOKUPS 10000
#define ZIPF_K 3

DUCKDB_BENCHMARK(SuccinctZipfDistribution, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
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
	std::random_device rd{};
    std::mt19937 gen{rd()};
	Zipf<uint32_t, double> zipf(NUM_INSERTS, ZIPF_K);

	state->conn.Query("BEGIN TRANSACTION");
	for (int i = 0; i < NUM_LOOKUPS; ++i) {
		auto val = uint32_t(std::round(zipf(gen)));
		auto query_string = "SELECT i FROM t1 where i == " + std::to_string(val);
		state->result = state->conn.Query(query_string);
	}
	state->conn.Query("COMMIT");
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
FINISH_BENCHMARK(SuccinctZipfDistribution)

DUCKDB_BENCHMARK(NonSuccinctZipfDistribution, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.succinct_enabled = false;

	state->conn.Query("CREATE TABLE t1(i UINTEGER);");
	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<uint32_t>(i);
		appender.EndRow();
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	std::random_device rd{};
    std::mt19937 gen{rd()};
	Zipf<uint32_t, double> zipf(NUM_INSERTS, ZIPF_K);

	state->conn.Query("BEGIN TRANSACTION");
	for (int i = 0; i < NUM_LOOKUPS; ++i) {
		auto val = uint32_t(std::round(zipf(gen)));
		auto query_string = "SELECT t1.i FROM t1 where i == " + std::to_string(val);
		state->result = state->conn.Query(query_string);
	}
	state->conn.Query("COMMIT");
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
FINISH_BENCHMARK(NonSuccinctZipfDistribution)
