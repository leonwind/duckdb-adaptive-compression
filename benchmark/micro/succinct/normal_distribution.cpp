#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"

#include <iostream>

using namespace duckdb;

#define NUM_INSERTS 1000000
#define NUM_LOOKUPS 10000

DUCKDB_BENCHMARK(SuccinctNormalDistribution, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
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
	std::normal_distribution<> normal{
	    /* mean= */ std::round(NUM_INSERTS / 2),
	    /* stddev= */ std::round(NUM_INSERTS) / 4};
	for (int i = 0; i < NUM_LOOKUPS; ++i) {
		state->data.push_back(uint32_t(std::round(normal(gen))));
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	for (int i = 0; i < NUM_LOOKUPS; i++) {
		auto query_string = "SELECT i FROM t1 where i == " +
		                    to_string(state->data[i]);
		state->conn.Query(query_string);
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
FINISH_BENCHMARK(SuccinctNormalDistribution)

DUCKDB_BENCHMARK(SuccinctPaddedNormalDistribution, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.succinct_padded_to_next_byte_enabled = true;
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
	std::normal_distribution<> normal{
	    /* mean= */ std::round(NUM_INSERTS / 2),
	    /* stddev= */ std::round(NUM_INSERTS) / 4};
	for (int i = 0; i < NUM_LOOKUPS; ++i) {
		state->data.push_back(uint32_t(std::round(normal(gen))));
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	for (int i = 0; i < NUM_LOOKUPS; i++) {
		auto query_string = "SELECT i FROM t1 where i == " +
		                    to_string(state->data[i]);
		state->conn.Query(query_string);
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
FINISH_BENCHMARK(SuccinctPaddedNormalDistribution)

DUCKDB_BENCHMARK(NonSuccinctNormalDistribution, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.succinct_enabled = false;

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
	std::normal_distribution<> normal{
	    /* mean= */ std::round(NUM_INSERTS / 2),
	    /* stddev= */ std::round(NUM_INSERTS) / 4};
	for (int i = 0; i < NUM_LOOKUPS; ++i) {
		state->data.push_back(uint32_t(std::round(normal(gen))));
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	for (int i = 0; i < NUM_LOOKUPS; i++) {
		auto query_string = "SELECT i FROM t1 where i == " +
		                    to_string(state->data[i]);
		state->conn.Query(query_string);
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
FINISH_BENCHMARK(NonSuccinctNormalDistribution)