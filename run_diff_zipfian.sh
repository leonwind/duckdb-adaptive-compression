cd build

for i in `seq 0.0 .1 2.0`; 
do 
	#sed -i "/#define ZIPF_SKEW*/c\#define ZIPF_SKEW $i" ../benchmark/micro/succinct/zipf_distribution_diff_skews.cpp
	#ninja
	dir_name="k_${i//,/}"
	echo "$dir_name"
	mkdir "$dir_name"

	./benchmark/benchmark_runner SuccinctZipfDifferentSkews | tee "k_$i/adaptive.txt"
	./benchmark/benchmark_runner SuccinctZipfNonAdaptiveDifferentSkews | tee "k_$i/non_adaptive.txt"
	./benchmark/benchmark_runner NonSuccinctZipfDifferentSkews | tee "k_$i/non_succinct.txt"
done;


