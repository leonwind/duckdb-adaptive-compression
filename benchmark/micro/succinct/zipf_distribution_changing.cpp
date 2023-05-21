#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "zipf.cpp"
#include <iostream>
#include <chrono>

using namespace duckdb;

#define NUM_INSERTS 1000000 // 1 Billion
#define NUM_LOOKUPS 1000000 // 1 Million
#define ZIPF_K 3
#define DURATION std::chrono::seconds(50)
#define DISTRIBUTION_CHANGE std::chrono::seconds(25)

DUCKDB_BENCHMARK(SuccinctZipfChangingOverTime, "[succinct]")
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
		uint32_t curr = uint32_t(std::round(zipf(gen))) + NUM_INSERTS / 4;
		state->data.push_back(curr);
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
		auto val = state->data[i] + NUM_INSERTS / 2;

		if (std::chrono::steady_clock::now() - start >= DISTRIBUTION_CHANGE) {
			val = state->data[i];
		}

		state->conn.Query("BEGIN TRANSACTION");
		auto query_string = "SELECT t1.i FROM t1 where i == " + std::to_string(val);
		state->result = state->conn.Query(query_string);
		state->conn.Query("COMMIT");
		i++;
		transaction_count++;

		if (std::chrono::steady_clock::now() - qps_start >= std::chrono::seconds(1)) {
			size_t memory = buffer_manager.GetDataSize();
			qps.emplace_back(transaction_count, memory);
			//std::cout << "Curr qps: " << transaction_count << ", memory: " << memory << std::endl;
			transaction_count = 0;
			qps_start = std::chrono::steady_clock::now();
		}

		if (i >= state->data.size()) {
			i = 0;
		}
	}

	for (auto curr: qps) {
		std::cout << /* qps= */ curr.first << ", "
		          << /* memory= */ curr.second << std::endl;
	}
	//exit(0);
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
FINISH_BENCHMARK(SuccinctZipfChangingOverTime)

DUCKDB_BENCHMARK(NonSuccinctZipfChangingOverTime, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.succinct_enabled = false;
	state->db.instance->config.adaptive_succinct_compression_enabled = false;
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
		uint32_t curr = uint32_t(std::round(zipf(gen)));
		state->data.push_back(curr);
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
		auto val = state->data[i] + NUM_INSERTS / 2;

		if (std::chrono::steady_clock::now() - start >= DISTRIBUTION_CHANGE) {
			val = state->data[i];
		}

		state->conn.Query("BEGIN TRANSACTION");
		auto query_string = "SELECT t1.i FROM t1 where i == " + std::to_string(val);
		state->result = state->conn.Query(query_string);
		state->conn.Query("COMMIT");
		i++;
		transaction_count++;

		if (std::chrono::steady_clock::now() - qps_start >= std::chrono::seconds(1)) {
			size_t memory = buffer_manager.GetDataSize();
			qps.emplace_back(transaction_count, memory);
			//std::cout << "Curr qps: " << transaction_count << ", memory: " << memory << std::endl;
			transaction_count = 0;
			qps_start = std::chrono::steady_clock::now();
		}

		if (i >= state->data.size()) {
			i = 0;
		}
	}

	for (auto curr: qps) {
		std::cout << /* qps= */ curr.first << ", "
		          << /* memory= */ curr.second << std::endl;
	}
	//exit(0);
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
FINISH_BENCHMARK(NonSuccinctZipfChangingOverTime)

DUCKDB_BENCHMARK(SuccinctNotAdaptiveZipfChangingOverTime, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.succinct_enabled = true;
	state->db.instance->config.adaptive_succinct_compression_enabled = false;
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
		uint32_t curr = uint32_t(std::round(zipf(gen)));
		state->data.push_back(curr);
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
		auto val = state->data[i] + NUM_INSERTS / 2;

		if (std::chrono::steady_clock::now() - start >= DISTRIBUTION_CHANGE) {
			val = state->data[i];
		}

		state->conn.Query("BEGIN TRANSACTION");
		auto query_string = "SELECT t1.i FROM t1 where i == " + std::to_string(val);
		state->result = state->conn.Query(query_string);
		state->conn.Query("COMMIT");
		i++;
		transaction_count++;

		if (std::chrono::steady_clock::now() - qps_start >= std::chrono::seconds(1)) {
			size_t memory = buffer_manager.GetDataSize();
			qps.emplace_back(transaction_count, memory);
			//std::cout << "Curr qps: " << transaction_count << ", memory: " << memory << std::endl;
			transaction_count = 0;
			qps_start = std::chrono::steady_clock::now();
		}

		if (i >= state->data.size()) {
			i = 0;
		}
	}

	for (auto curr: qps) {
		std::cout << /* qps= */ curr.first << ", "
		          << /* memory= */ curr.second << std::endl;
	}
	//exit(0);
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
FINISH_BENCHMARK(SuccinctNotAdaptiveZipfChangingOverTime)