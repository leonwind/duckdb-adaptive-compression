#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "zipf.cpp"
#include <iostream>
#include <chrono>

using namespace duckdb;

#define NUM_INSERTS 10000000 // 10 Million
#define NUM_LOOKUPS 1000000 // 1 Million
#define ZIPF_K 1
#define DURATION std::chrono::seconds(10)

DUCKDB_BENCHMARK(SuccinctZipfDistributionOverTime, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.adaptive_succinct_compression_enabled = true;
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
	for (int i = 0; i < NUM_LOOKUPS; ++i) {
		state->data.push_back(uint32_t(std::round(zipf(gen))));
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
	size_t i = 0;

	std::chrono::steady_clock::time_point qps_start = std::chrono::steady_clock::now();
	std::vector<std::pair<size_t, size_t>> qps;
	size_t transaction_count = 0;

	auto& buffer_manager = state->db.instance->GetBufferManager();

	while (std::chrono::steady_clock::now() - start < DURATION) {
		state->conn.Query("BEGIN TRANSACTION");
		auto val = state->data[i];
		auto query_string = "SELECT t1.i FROM t1 where i == " + std::to_string(val);
		state->result = state->conn.Query(query_string);
		state->conn.Query("COMMIT");
		i++;
		transaction_count++;

		if (std::chrono::steady_clock::now() - qps_start >= std::chrono::seconds(1)) {
			size_t memory = buffer_manager.GetUsedMemory();
			qps.emplace_back(transaction_count, memory);
			transaction_count = 0;
			qps_start = std::chrono::steady_clock::now();
		}

		if (i >= state->data.size()) {
			i = 0;
		}
	}

	for (auto curr: qps) {
		std::cout << curr.first << ", " << curr.second << std::endl;
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
FINISH_BENCHMARK(SuccinctZipfDistributionOverTime)

DUCKDB_BENCHMARK(SuccinctNotAdaptiveZipfDistributionOverTime, "[succinct]")
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
	Zipf<uint32_t, double> zipf(NUM_INSERTS, ZIPF_K);
	for (int i = 0; i < NUM_LOOKUPS; ++i) {
		state->data.push_back(uint32_t(std::round(zipf(gen))));
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
	size_t i = 0;

	std::chrono::steady_clock::time_point qps_start = std::chrono::steady_clock::now();
	std::vector<std::pair<size_t, size_t>> qps;
	size_t transaction_count = 0;

	auto& buffer_manager = state->db.instance->GetBufferManager();

	while (std::chrono::steady_clock::now() - start < DURATION) {
		state->conn.Query("BEGIN TRANSACTION");
		auto val = state->data[i];
		auto query_string = "SELECT t1.i FROM t1 where i == " + std::to_string(val);
		state->result = state->conn.Query(query_string);
		state->conn.Query("COMMIT");
		i++;
		transaction_count++;

		if (std::chrono::steady_clock::now() - qps_start >= std::chrono::seconds(1)) {
			size_t memory = buffer_manager.GetUsedMemory();
			qps.emplace_back(transaction_count, memory);
			transaction_count = 0;
			qps_start = std::chrono::steady_clock::now();
		}

		if (i >= state->data.size()) {
			i = 0;
		}
	}

	for (auto curr: qps) {
		std::cout << curr.first << ", " << curr.second << std::endl;
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
FINISH_BENCHMARK(SuccinctNotAdaptiveZipfDistributionOverTime)


DUCKDB_BENCHMARK(NonSuccinctZipfDistributionOverTime, "[succinct]")
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
	Zipf<uint32_t, double> zipf(NUM_INSERTS, ZIPF_K);
	for (int i = 0; i < NUM_LOOKUPS; ++i) {
		state->data.push_back(uint32_t(std::round(zipf(gen))));
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
	size_t i = 0;

	std::chrono::steady_clock::time_point qps_start = std::chrono::steady_clock::now();
	std::vector<std::pair<size_t, size_t>> qps;
	size_t transaction_count = 0;

	auto& buffer_manager = state->db.instance->GetBufferManager();

	while (std::chrono::steady_clock::now() - start < DURATION) {
		state->conn.Query("BEGIN TRANSACTION");
		auto val = state->data[i];
		auto query_string = "SELECT t1.i FROM t1 where i == " + std::to_string(val);
		state->result = state->conn.Query(query_string);
		state->conn.Query("COMMIT");
		i++;
		transaction_count++;

		if (std::chrono::steady_clock::now() - qps_start >= std::chrono::seconds(1)) {
			size_t memory = buffer_manager.GetUsedMemory();
			qps.emplace_back(transaction_count, memory);
			transaction_count = 0;
			qps_start = std::chrono::steady_clock::now();
		}

		if (i >= state->data.size()) {
			i = 0;
		}
	}

	for (auto curr: qps) {
		std::cout << curr.first << ", " << curr.second << std::endl;
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
FINISH_BENCHMARK(NonSuccinctZipfDistributionOverTime)

