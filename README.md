## AdaCom: Adaptive Compression For Databases
This is an exemplary implementation of AdaCom in [DuckDB](https://github.com/duckdb/duckdb).

## Installation
Our implementation uses the [Succinct Data Structure Library (SDSL)](https://github.com/simongog/sdsl-lite/), and it can be installed with

```shell
./installSDSL.sh
```

Afterwards, DuckDB with our extension can be compiled with:

```shell
mkdir build
cd build
cmake -DBUILD_BENCHMARKS=1 -DBUILD_TPCH_EXTENSION=1 -GNinja -DCMAKE_BUILD_TYPE=Release ..
ninja
```

## Benchmarks
Our benchmarks are in `/benchmark/micro/succinct` and can be run from the build directory with e.g.

```shell
benchmark/benchmark_runner SuccinctZipfDistribution
```