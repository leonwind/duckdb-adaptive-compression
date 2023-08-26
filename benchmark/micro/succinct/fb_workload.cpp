#include "benchmark_runner.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "fb_binary_data_loader.cpp"
#include <random>
#include <iostream>
#include <chrono>

using namespace duckdb;

const string user_ids_filename = "data/fb_workloads/prefix-random-53M_uint64";
const string workload_0_path =   "data/fb_workloads/workload0";
const string workload_1_path =   "data/fb_workloads/workload1";
const string workload_2_path =   "data/fb_workloads/workload2";

const size_t workload_step_size = 100;


DUCKDB_BENCHMARK(FBWorkloadAdaptive, "[succinct]")
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

		/*
		if (c++ >= 40000000) {
			break;
		}
		 */
	}
	appender.Close();

	std::cout << "Finished insertion of " << user_ids.size() << " entries" << std::endl;

	auto rng = std::default_random_engine{};
	for (const auto &workload: {workload_0_path, workload_1_path, workload_2_path}) {
		auto loaded_workload = ParallelLoadData<Query<uint64_t>>(workload);
		std::shuffle(std::begin(loaded_workload), std::end(loaded_workload), rng);
		state->workloads.push_back(loaded_workload);
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	auto& buffer_manager = state->db.instance->GetBufferManager();

	std::chrono::steady_clock::time_point qps_start = std::chrono::steady_clock::now();
	//std::vector<std::pair<size_t, size_t>> qps;
	size_t transaction_count = 0;

	for (const auto& workload: state->workloads) {
		//std::cout << "Start workload " << i++ << std::endl;
		for (size_t i = 0; i < workload.size(); i += workload_step_size) {
			auto query = workload[i];
			state->conn.Query("BEGIN TRANSACTION");
			const auto select_query =
			    "SELECT i FROM t1 where i == " + std::to_string(query.key);
			auto result = state->conn.Query(select_query);
			state->conn.Query("COMMIT");
			transaction_count++;

			if (!result->FetchRaw()) {
				state->conn.Query("BEGIN TRANSACTION");
				const auto insert_query =
				    "INSERT INTO t1 values (" + std::to_string(query.key) + ");";
				state->conn.Query(insert_query);
				state->conn.Query("COMMIT");
				transaction_count++;

				//std::cout << "Inserted new value " << query.key << std::endl;

				/*
				result = state->conn.Query(select_query);
				if (!result->FetchRaw()) {
					std::cout << "Value still does not exist???" << std::endl;
				}
				 */
			}

			if (std::chrono::steady_clock::now() - qps_start >= std::chrono::seconds(1)) {
				size_t memory = buffer_manager.GetUsedMemory();
				//qps.emplace_back(transaction_count, memory);
				std::cout << transaction_count << ", " << memory << std::endl;
				transaction_count = 0;
				qps_start = std::chrono::steady_clock::now();
			}
		}
	}

	exit(0);
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

FINISH_BENCHMARK(FBWorkloadAdaptive)

DUCKDB_BENCHMARK(FBWorkloadSuccinct, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE t1(i UBIGINT);");

	auto user_ids = ParallelLoadData<uint64_t>(user_ids_filename);
	Appender appender(state->conn, "t1");

	size_t c = 0;
	for (auto id: user_ids) {
		appender.BeginRow();
		appender.Append<uint64_t>(id);
		appender.EndRow();
	}
	appender.Close();

	std::cout << "Finished insertion of " << user_ids.size() << " entries" << std::endl;

	auto rng = std::default_random_engine{};
	for (const auto &workload: {workload_0_path, workload_1_path, workload_2_path}) {
		auto loaded_workload = ParallelLoadData<Query<uint64_t>>(workload);
		std::shuffle(std::begin(loaded_workload), std::end(loaded_workload), rng);
		state->workloads.push_back(loaded_workload);
	}

	auto& buffer_manager = state->db.instance->GetBufferManager();

	std::cout << "Size before compacting: " << buffer_manager.GetUsedMemory() << std::endl;
	auto& db_manager = state->db.instance->GetDatabaseManager();
	db_manager.GetSystemCatalog().GetColumnSegmentCatalog()->CompactAllSegments();

	std::cout << "Size before queries: " << buffer_manager.GetUsedMemory() << std::endl;
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	auto& buffer_manager = state->db.instance->GetBufferManager();

	std::chrono::steady_clock::time_point qps_start = std::chrono::steady_clock::now();
	//std::vector<std::pair<size_t, size_t>> qps;
	size_t transaction_count = 0;

	for (const auto& workload: state->workloads) {
		//std::cout << "Start workload " << i++ << std::endl;
		for (size_t i = 0; i < workload.size(); i += workload_step_size) {
			auto query = workload[i];
			state->conn.Query("BEGIN TRANSACTION");
			const auto select_query =
			    "SELECT i FROM t1 where i == " + std::to_string(query.key);
			auto result = state->conn.Query(select_query);
			state->conn.Query("COMMIT");
			transaction_count++;

			if (!result->FetchRaw()) {
				state->conn.Query("BEGIN TRANSACTION");
				const auto insert_query =
				    "INSERT INTO t1 values (" + std::to_string(query.key) + ");";
				state->conn.Query(insert_query);
				state->conn.Query("COMMIT");
				transaction_count++;

				//std::cout << "Inserted new value " << query.key << std::endl;
			}

			if (std::chrono::steady_clock::now() - qps_start >= std::chrono::seconds(1)) {
				size_t memory = buffer_manager.GetUsedMemory();
				std::cout << transaction_count << ", " << memory << std::endl;
				transaction_count = 0;
				qps_start = std::chrono::steady_clock::now();
			}
		}
	}

	exit(0);
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

FINISH_BENCHMARK(FBWorkloadSuccinct)

DUCKDB_BENCHMARK(FBWorkload, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->db.instance->config.succinct_enabled = false;
	state->conn.Query("CREATE TABLE t1(i UBIGINT);");

	auto user_ids = ParallelLoadData<uint64_t>(user_ids_filename);
	Appender appender(state->conn, "t1");

	for (auto id: user_ids) {
		appender.BeginRow();
		appender.Append<uint64_t>(id);
		appender.EndRow();
	}
	appender.Close();

	std::cout << "Finished insertion of " << user_ids.size() << " entries" << std::endl;

	auto rng = std::default_random_engine{};
	for (const auto &workload: {workload_0_path, workload_1_path, workload_2_path}) {
		auto loaded_workload = ParallelLoadData<Query<uint64_t>>(workload);
		std::shuffle(std::begin(loaded_workload), std::end(loaded_workload), rng);
		state->workloads.push_back(loaded_workload);
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	auto& buffer_manager = state->db.instance->GetBufferManager();

	std::chrono::steady_clock::time_point qps_start = std::chrono::steady_clock::now();
	//std::vector<std::pair<size_t, size_t>> qps;
	size_t transaction_count = 0;

	for (const auto& workload: state->workloads) {
		//std::cout << "Start workload " << i++ << std::endl;
		for (size_t i = 0; i < workload.size(); i += workload_step_size) {
			auto query = workload[i];
			state->conn.Query("BEGIN TRANSACTION");
			const auto select_query =
			    "SELECT i FROM t1 where i == " + std::to_string(query.key);
			auto result = state->conn.Query(select_query);
			state->conn.Query("COMMIT");
			transaction_count++;

			if (!result->FetchRaw()) {
				state->conn.Query("BEGIN TRANSACTION");
				const auto insert_query =
				    "INSERT INTO t1 values (" + std::to_string(query.key) + ");";
				state->conn.Query(insert_query);
				state->conn.Query("COMMIT");
				transaction_count++;
			}

			if (std::chrono::steady_clock::now() - qps_start >= std::chrono::seconds(1)) {
				size_t memory = buffer_manager.GetUsedMemory();
				//qps.emplace_back(transaction_count, memory);
				std::cout << transaction_count << ", " << memory << std::endl;
				transaction_count = 0;
				qps_start = std::chrono::steady_clock::now();
			}
		}
	}

	exit(0);
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