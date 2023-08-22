#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "zipf.cpp"
#include <iostream>
#include <chrono>

using namespace duckdb;

//#define NUM_INSERTS 10000000
#define NUM_LOOKUPS 1000

DUCKDB_BENCHMARK(DistributionChanging, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.adaptive_succinct_compression_enabled = false;
	state->db.instance->config.succinct_enabled = true;
	state->conn.Query("CREATE TABLE t1(i UINTEGER);");

	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < 10; i++) {
		appender.BeginRow();
		appender.Append<uint32_t>(i);
		appender.EndRow();
	}

	auto& db_manager = state->db.instance->GetDatabaseManager();
	db_manager.GetSystemCatalog().GetColumnSegmentCatalog()->CompactAllSegments();
	appender.Close();
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	/*
	auto start = std::chrono::steady_clock::now();
	auto first_duration = std::chrono::seconds(30);
	auto second_duration = std::chrono::seconds(30);

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
		                    std::to_string(100000);
		state->result = state->conn.Query(query_string);
		state->conn.Query("COMMIT");
	}
	*/
	auto& db_manager = state->db.instance->GetDatabaseManager();

	state->conn.Query("BEGIN TRANSACTION");
	auto query_string = "SELECT * FROM t1";
	state->result = state->conn.Query(query_string);
	state->conn.Query("COMMIT");

	std::cout << state->result->ToString() << std::endl;

	std::cout << "APPEND NOW:" << std::endl;

	state->conn.Query("BEGIN TRANSACTION");
	query_string = "INSERT INTO t1 values (999)";
	state->result = state->conn.Query(query_string);
	state->conn.Query("COMMIT");

	db_manager.GetSystemCatalog().GetColumnSegmentCatalog()->CompactAllSegments();
	std::cout << "COMPACTED ALL SEGMENTS AGAIN" << std::endl;

	state->conn.Query("BEGIN TRANSACTION");
	query_string = "SELECT * FROM t1";
	state->result = state->conn.Query(query_string);
	state->conn.Query("COMMIT");
	std::cout << state->result->ToString() << std::endl;
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
