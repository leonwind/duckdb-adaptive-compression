#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "fb_binary_data_loader.cpp"
#include <iostream>
#include <chrono>

using namespace duckdb;

enum QueryType { LOOKUP, INSERT, UPDATE, DELETE, RANGELOOKUP, UNDEFINED };

template<class KEY_TYPE = uint64_t>
struct Query {
  QueryType type;
  KEY_TYPE key;
  KEY_TYPE key_range_upper;
  uint64_t value;
};

const string user_ids_filename = "data/fb_workloads/prefix-random-53M_uint64";
const string workload_0_path =   "data/fb_workloads/workload0";
const string workload_1_path =   "data/fb_workloads/workload1";
const string workload_2_path =   "data/fb_workloads/workload2";

std::vector<std::vector<Query<uint64_t>>> workloads;

DUCKDB_BENCHMARK(FBWorkload, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	auto user_ids = ParallelLoadData<uint64_t>(user_ids_filename);

	state->db.instance->config.adaptive_succinct_compression_enabled = true;
	state->conn.Query("CREATE TABLE t1(i UBIGINT);");

	Appender appender(state->conn, "t1");
	for (auto& id: user_ids) {
		appender.BeginRow();
		appender.Append<uint64_t>(id);
		appender.EndRow();
	}
	appender.Close();
	std::cout << "Finished insertion" << std::endl;

	for (const auto &workload: {workload_0_path, workload_1_path, workload_2_path}) {
		workloads.push_back(ParallelLoadData<Query<uint64_t>>(workload));
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	size_t i = 0;
	size_t progress_step = 10000;

	for (const auto& workload: workloads) {
		std::cout << "Start workload " << i++ << std::endl;

		size_t j = 0;
		for (const auto& query: workload) {
			state->conn.Query("BEGIN TRANSACTION");

			const auto query_string = "SELECT i FROM t1 where i == " + std::to_string(query.key);
			state->result = state->conn.Query(query_string);
			state->conn.Query("COMMIT");

			if (j % progress_step == 0) {
				std::cout << "QUERIED 10K: j = " << j << std::endl;
			}

			j++;
		}
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

FINISH_BENCHMARK(FBWorkload)