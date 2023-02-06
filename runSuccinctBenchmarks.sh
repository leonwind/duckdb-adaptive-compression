touch benchmarks.csv
echo $'name\trun\ttiming\tsizeInBytes\tMemoryAllocationInBytes' > benchmarks.csv

build/benchmark/benchmark_runner SuccinctSequentialInsert 2>> benchmarks.csv
build/benchmark/benchmark_runner SuccinctPaddedSequentialInsert 2>> benchmarks.csv
build/benchmark/benchmark_runner NonSuccinctSequentialInsert 2>> benchmarks.csv

build/benchmark/benchmark_runner SuccinctRandomInsert 2>> benchmarks.csv
build/benchmark/benchmark_runner SuccinctPaddedRandomInsert 2>> benchmarks.csv
build/benchmark/benchmark_runner NonSuccinctRandomInsert 2>> benchmarks.csv

build/benchmark/benchmark_runner SuccinctNormalDistribution 2>> benchmarks.csv
build/benchmark/benchmark_runner SuccinctPaddedNormalDistribution 2>> benchmarks.csv
build/benchmark/benchmark_runner NonSuccinctNormalDistribution 2>> benchmarks.csv

build/benchmark/benchmark_runner SuccinctZipfDistribution 2>> benchmarks.csv
build/benchmark/benchmark_runner SuccinctPaddedZipfDistribution 2>> benchmarks.csv
build/benchmark/benchmark_runner NonSuccinctZipfDistribution 2>> benchmarks.csv

