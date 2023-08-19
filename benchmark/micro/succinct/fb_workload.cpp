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

/*
enum QueryType { LOOKUP, INSERT, UPDATE, DELETE, RANGELOOKUP, UNDEFINED };

template<class KEY_TYPE = uint64_t>
struct Query {
  QueryType type;
  KEY_TYPE key;
  KEY_TYPE key_range_upper;
  uint64_t value;
};
*/

const string user_ids_filename = "data/fb_workloads/prefix-random-53M_uint64";
const string workload_0_path =   "data/fb_workloads/workload0";
const string workload_1_path =   "data/fb_workloads/workload1";
const string workload_2_path =   "data/fb_workloads/workload2";

//std::vector<std::vector<Query<uint64_t>>> workloads;

DUCKDB_BENCHMARK(FBWorkload, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.adaptive_succinct_compression_enabled = true;
	state->conn.Query("CREATE TABLE t1(i UBIGINT);");

	auto user_ids = ParallelLoadData<uint64_t>(user_ids_filename);
	Appender appender(state->conn, "t1");

	size_t c = 0;
	for (auto id: user_ids) {
		appender.BeginRow();
		appender.Append<uint64_t>(id);
		appender.EndRow();

		if (c++ >= 40000000) {
			break;
		}
	}
	appender.Close();

	std::cout << "Finished insertion of " << user_ids.size() << " entries" << std::endl;

	for (const auto &workload: {workload_0_path, /* workload_1_path, workload_2_path */}) {
		state->workloads.push_back(ParallelLoadData<Query<uint64_t>>(workload));
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	size_t progress_step = 10000;
	auto& buffer_manager = state->db.instance->GetBufferManager();

	for (const auto& workload: state->workloads) {
		//std::cout << "Start workload " << i++ << std::endl;

		size_t j = 0;
		for (const auto& query: workload) {
			state->conn.Query("BEGIN TRANSACTION");

			const auto select_query =
			    "SELECT i FROM t1 where i == " + std::to_string(query.key);
			auto result = state->conn.Query(select_query);

			state->conn.Query("COMMIT");

			if (!result->FetchRaw()) {
				state->conn.Query("BEGIN TRANSACTION");

				const auto insert_query =
				    "INSERT INTO t1 values (" + std::to_string(query.key) + ");";
				state->conn.Query(insert_query);

				state->conn.Query("COMMIT");

				std::cout << "Inserted new value " << query.key << std::endl;

				result = state->conn.Query(select_query);
				if (!result->FetchRaw()) {
					std::cout << "Value still does not exist???" << std::endl;
				}

			}

			if (j % progress_step == 0) {
				std::cout << "Queried: " << j
				          << ", Curr memory: " << buffer_manager.GetUsedMemory()
				          << std::endl;
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