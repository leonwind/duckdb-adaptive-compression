touch benchmarks.csv
echo $'name\trun\ttiming\tsizeInBytes\tMemoryAllocationInBytes' > benchmarks.csv

build/benchmark/benchmark_runner SuccinctSequentialInsert 2>> benchmarks.csv
build/benchmark/benchmark_runner SuccinctPaddedSequentialInsert 2>> benchmarks.csv
build/benchmark/benchmark_runner NonSuccinctSequentialInsert 2>> benchmarks.csv
echo "Finished Succinct Sequential Insert Benchmarks"

build/benchmark/benchmark_runner SuccinctRandomInsert 2>> benchmarks.csv
build/benchmark/benchmark_runner SuccinctPaddedRandomInsert 2>> benchmarks.csv
build/benchmark/benchmark_runner NonSuccinctRandomInsert 2>> benchmarks.csv
echo "Finished Succinct Random Insert Benchmarks"

build/benchmark/benchmark_runner SuccinctNormalDistribution 2>> benchmarks.csv
build/benchmark/benchmark_runner SuccinctPaddedNormalDistribution 2>> benchmarks.csv
build/benchmark/benchmark_runner NonSuccinctNormalDistribution 2>> benchmarks.csv
echo "Finished Succinct Normal Distribution Benchmarks"

build/benchmark/benchmark_runner SuccinctZipfDistribution 2>> benchmarks.csv
build/benchmark/benchmark_runner SuccinctPaddedZipfDistribution 2>> benchmarks.csv
build/benchmark/benchmark_runner NonSuccinctZipfDistribution 2>> benchmarks.csv
echo "Finished Succinct Zipf Distribution Benchmarks"

build/benchmark/benchmark_runner SuccinctScanOOM 2>> benchmarks.csv
build/benchmark/benchmark_runner NonSuccinctScanOOM 2>> benchmarks.csv
echo "Finished Succinct Sequential Scan OOM Benchmarks"

build/benchmark/benchmark_runner SuccinctZipfScanOOM 2>> benchmarks.csv
build/benchmark/benchmark_runner NonSuccinctZipfScanOOM 2>> benchmarks.csv
echo "Finished Succinct Sequential Zipf Scan OOM Benchmarks"

