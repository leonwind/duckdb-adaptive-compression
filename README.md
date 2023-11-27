# AdaCom: Adaptive Compression For Databases
This is our implementation of our paper "Adaptive Compression For Databases" (EDBT '24), where we adaptively compress infrequently accessed column segments using succinct encoding, in [DuckDB](https://github.com/duckdb/duckdb).

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

### Build issue: Linking SDSL with DuckDB
Some systems may experience issues linking the SDSL with DuckDB due to the error

```
can not be used when making a shared object; recompile with -fPIC
```

One fix is by adding the flag `-DCMAKE_POSITION_INDEPENDENT_CODE=ON` in `third_party/sdsl-lite/install.sh` in the line

```
cmake -DCMAKE_VERBOSE_MAKEFILE=ON -DCMAKE_INSTALL_PREFIX="${SDSL_INSTALL_PREFIX}" .. # run cmake
```

resulting in 

```
cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_VERBOSE_MAKEFILE=ON -DCMAKE_INSTALL_PREFIX="${SDSL_INSTALL_PREFIX}" .. # run cmake 
```

After adding the flag, re-running the `install.sh` script is necessary:

```
cd third_party/sdsl-lite
./install.sh ../sdsl
```
