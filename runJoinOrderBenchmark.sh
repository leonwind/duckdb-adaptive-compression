parquet_count=$(find benchmark/imdb/init/parquet -name '*.parquet' | wc -l)

if [[ "$parquet_count" -ne 21 ]]; then
	cd benchmark/imdb/init/parquet
	./download.sh
	cd ../../../../
fi

cd build
benchmark/benchmark_runner benchmark/imdb/.*
