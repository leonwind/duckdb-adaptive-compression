include_directories(../../third_party/sqlite/include)
add_library(
  duckdb_benchmark_micro OBJECT append.cpp append_mix.cpp bulkupdate.cpp
                                cast.cpp in.cpp storage.cpp succinct/sequential.cpp succinct/random.cpp succinct/normal_distribution.cpp succinct/zipf_distribution.cpp succinct/zipf.cpp succinct/out_of_memory.cpp succinct/zipf_over_time.cpp succinct/zipf_distribution_changing.cpp succinct/distribution_changing.cpp)
set(BENCHMARK_OBJECT_FILES
    ${BENCHMARK_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_benchmark_micro>
    PARENT_SCOPE)
