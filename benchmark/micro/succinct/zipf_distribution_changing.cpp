#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "zipf.cpp"
#include <iostream>
#include <chrono>

using namespace duckdb;

#define NUM_INSERTS 100000000 // 10 Million
#define NUM_LOOKUPS 1000000 // 1 Million
#define ZIPF_K 1
#define DURATION std::chrono::seconds(30)
#define DISTRIBUTION_CHANGE std::chrono::seconds(15)

DUCKDB_BENCHMARK(SuccinctZipfChangingOverTime, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.adaptive_succinct_compression_enabled = true;
	state->conn.Query("SET threads TO 4;");
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

	for (auto& it: v) {
		std::cout << it.first << ": " << it.second << std::endl;
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

	while (std::chrono::steady_clock::now() - start < DURATION) {
		state->conn.Query("BEGIN TRANSACTION");
		auto val = state->data[i];

		if (std::chrono::steady_clock::now() - start >= DISTRIBUTION_CHANGE) {
			val = NUM_INSERTS - 10;
		} else {
			val = 3;
		}

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
FINISH_BENCHMARK(SuccinctZipfChangingOverTime)
