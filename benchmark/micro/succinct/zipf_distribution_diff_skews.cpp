#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "zipf.cpp"

#include <iostream>

using namespace duckdb;

#define NUM_INSERTS 1000000 /// 1 Billion
#define NUM_LOOKUPS 100000 /// 100K
#define ZIPF_SKEW 0.0
#define DURATION std::chrono::seconds(10)

DUCKDB_BENCHMARK(SuccinctZipfDifferentSkews, "[succinct]")
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
	std::cout << "Finished insertion" << std::endl;

	std::random_device rd{};
    std::mt19937 gen{rd()};
	Zipf<uint32_t, double> zipf(NUM_INSERTS / 2, ZIPF_SKEW);
	std::unordered_map<uint32_t, uint32_t> frequencies;
	for (int i = 0; i < NUM_LOOKUPS; ++i) {
		uint32_t curr = uint32_t(std::round(zipf(gen)));
		state->data.push_back(curr);
		if (frequencies.find(curr) == frequencies.end()) {
			frequencies[curr] = 1;
		} else {
			frequencies[curr] += 1;
		}
	}

	/*
	std::vector<std::pair<uint32_t, uint32_t>> v(frequencies.begin(), frequencies.end());
	std::sort(v.begin(), v.end(),
				  [](std::pair<uint32_t, uint32_t>& left,
					 std::pair<uint32_t, uint32_t>& right) {
					  return left.second < right.second;
				  });

	for (auto c: v) {
		std::cout << c.first << ", " << c.second << std::endl;
	}
	*/
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
	size_t i = 0;

	std::chrono::steady_clock::time_point qps_start = std::chrono::steady_clock::now();
	std::vector<std::pair<size_t, size_t>> qps;
	size_t transaction_count = 0;

	auto& buffer_manager = state->db.instance->GetBufferManager();
	std::cout << "Start queries" << std::endl;
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
			std::cout << transaction_count << ", " << memory << std::endl;
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

FINISH_BENCHMARK(SuccinctZipfDifferentSkews)

DUCKDB_BENCHMARK(SuccinctZipfNonAdaptiveDifferentSkews, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.succinct_enabled = true;
	state->conn.Query("CREATE TABLE t1(i UINTEGER);");

	Appender appender(state->conn, "t1");
	for (size_t i = 0; i < NUM_INSERTS; i++) {
		appender.BeginRow();
		appender.Append<uint32_t>(i);
		appender.EndRow();
	}
	appender.Close();
	std::cout << "Finished insertion" << std::endl;

	std::random_device rd{};
    std::mt19937 gen{rd()};
	Zipf<uint32_t, double> zipf(NUM_INSERTS / 2, ZIPF_SKEW);
	std::unordered_map<uint32_t, uint32_t> frequencies;
	for (int i = 0; i < NUM_LOOKUPS; ++i) {
		uint32_t curr = uint32_t(std::round(zipf(gen)));
		state->data.push_back(curr);
		if (frequencies.find(curr) == frequencies.end()) {
			frequencies[curr] = 1;
		} else {
			frequencies[curr] += 1;
		}
	}

	auto& db_manager = state->db.instance->GetDatabaseManager();
	db_manager.GetSystemCatalog().GetColumnSegmentCatalog()->CompactAllSegments();
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
	size_t i = 0;

	std::chrono::steady_clock::time_point qps_start = std::chrono::steady_clock::now();
	std::vector<std::pair<size_t, size_t>> qps;
	size_t transaction_count = 0;

	auto& buffer_manager = state->db.instance->GetBufferManager();
	std::cout << "Start queries" << std::endl;
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
			std::cout << transaction_count << ", " << memory << std::endl;
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

FINISH_BENCHMARK(SuccinctZipfNonAdaptiveDifferentSkews)

DUCKDB_BENCHMARK(NonSuccinctZipfDifferentSkews, "[succinct]")
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
	std::cout << "Finished insertion" << std::endl;

	std::random_device rd{};
    std::mt19937 gen{rd()};
	Zipf<uint32_t, double> zipf(NUM_INSERTS / 2, ZIPF_SKEW);
	std::unordered_map<uint32_t, uint32_t> frequencies;
	for (int i = 0; i < NUM_LOOKUPS; ++i) {
		uint32_t curr = uint32_t(std::round(zipf(gen)));
		state->data.push_back(curr);
		if (frequencies.find(curr) == frequencies.end()) {
			frequencies[curr] = 1;
		} else {
			frequencies[curr] += 1;
		}
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
	size_t i = 0;

	std::chrono::steady_clock::time_point qps_start = std::chrono::steady_clock::now();
	std::vector<std::pair<size_t, size_t>> qps;
	size_t transaction_count = 0;

	auto& buffer_manager = state->db.instance->GetBufferManager();
	std::cout << "Start queries" << std::endl;
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
			std::cout << transaction_count << ", " << memory << std::endl;
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

FINISH_BENCHMARK(NonSuccinctZipfDifferentSkews)