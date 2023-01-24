#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

#define ROW_COUNT 1000000

DUCKDB_BENCHMARK(SuccinctBulkInsert, "[succinct]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE t1(i INTEGER);");
	Appender appender(state->conn, "t1");
	// insert the elements into the database
	for (size_t i = 0; i < 2048; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(200000);
		appender.EndRow();
	}
}

void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	state->result = state->conn.Query("SELECT * FROM t1");
	state->conn.Query("COMMIT");
}

string VerifyResult(QueryResult *result) override {
	//return result->ToString();
    return string();
}

string BenchmarkInfo() override {
	return "Run a bulk update using succinct integers";
}

bool InMemory() override {
	return true;
}

FINISH_BENCHMARK(SuccinctBulkInsert)