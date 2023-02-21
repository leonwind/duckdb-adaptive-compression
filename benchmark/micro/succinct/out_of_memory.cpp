#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "zipf.cpp"

#include <iostream>

using namespace duckdb;

#define NUM_INSERTS 150000000
#define NUM_LOOKUPS 100
#define NUM_ZIPF_LOOKUPS 10000
#define ZIPF_K 2

DUCKDB_BENCHMARK(SuccinctScanOOM, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("PRAGMA memory_limit='1GB'");
	state->conn.Query("CREATE TABLE t1(i INTEGER);");

	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.EndRow();
	}
	appender.Close();
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	for (size_t i = 0; i < NUM_LOOKUPS; ++i) {
		state->result = state->conn.Query("SELECT * FROM t1");
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
FINISH_BENCHMARK(SuccinctScanOOM)

DUCKDB_BENCHMARK(NonSuccinctScanOOM, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.succinct_enabled = false;
	state->conn.Query("PRAGMA memory_limit='1GB'");
	state->conn.Query("CREATE TABLE t1(i INTEGER);");

	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.EndRow();
	}
	appender.Close();
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	for (size_t i = 0; i < NUM_LOOKUPS; ++i) {
		state->result = state->conn.Query("SELECT * FROM t1");
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
	return false;
}
FINISH_BENCHMARK(NonSuccinctScanOOM)

DUCKDB_BENCHMARK(SuccinctZipfScanOOM, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("PRAGMA memory_limit='1GB'");
	state->conn.Query("CREATE TABLE t1(i UINTEGER);");

	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<uint32_t>(i);
		appender.EndRow();
	}
	appender.Close();

	std::random_device rd{};
    std::mt19937 gen{rd()};
	Zipf<uint32_t, double> zipf(NUM_INSERTS, ZIPF_K);
	for (int i = 0; i < NUM_ZIPF_LOOKUPS; ++i) {
		state->data.push_back(uint32_t(std::round(zipf(gen))));
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	for (int i = 0; i < NUM_ZIPF_LOOKUPS; ++i) {
		auto val = state->data[i];
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
FINISH_BENCHMARK(SuccinctZipfScanOOM)

DUCKDB_BENCHMARK(NonSuccinctZipfScanOOM, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.succinct_enabled = false;
	state->conn.Query("PRAGMA memory_limit='1GB'");
	state->conn.Query("CREATE TABLE t1(i UINTEGER);");

	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<uint32_t>(i);
		appender.EndRow();
	}
	appender.Close();

	std::random_device rd{};
    std::mt19937 gen{rd()};
	Zipf<uint32_t, double> zipf(NUM_INSERTS, ZIPF_K);
	for (int i = 0; i < NUM_ZIPF_LOOKUPS; ++i) {
		state->data.push_back(uint32_t(std::round(zipf(gen))));
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	for (int i = 0; i < NUM_ZIPF_LOOKUPS; ++i) {
		auto val = state->data[i];
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
	return false;
}
FINISH_BENCHMARK(NonSuccinctZipfScanOOM)
