rm -rf duckdb_benchmark_data
cd build
benchmark/benchmark_runner benchmark/tpch/sf1/q01.benchmark > ../log.tmp
