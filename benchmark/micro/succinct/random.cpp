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
		appender.Append<uint32_t>(rand() % NUM_INSERTS);
		appender.EndRow();
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	state->result = state->conn.Query("SELECT * FROM t1");
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
FINISH_BENCHMARK(SuccinctRandomInsert)

DUCKDB_BENCHMARK(SuccinctPaddedRandomInsert, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	srand((unsigned) time(nullptr));
	state->db.instance->config.succinct_padded_to_next_byte_enabled = true;
	state->conn.Query("CREATE TABLE t1(i INTEGER);");

	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<uint32_t>(rand() % NUM_INSERTS);
		appender.EndRow();
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	state->result = state->conn.Query("SELECT * FROM t1");
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
FINISH_BENCHMARK(SuccinctPaddedRandomInsert)

DUCKDB_BENCHMARK(NonSuccinctRandomInsert, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	srand((unsigned) time(nullptr));
	state->db.instance->config.succinct_enabled = false;

	state->conn.Query("CREATE TABLE t1(i INTEGER);");
	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<uint32_t>(random() % NUM_INSERTS);
		appender.EndRow();
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	state->result = state->conn.Query("SELECT * FROM t1");
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
FINISH_BENCHMARK(NonSuccinctRandomInsert)
