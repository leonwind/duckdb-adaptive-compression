#include <iostream>
#include <sdsl/vectors.hpp>
#include <chrono>

using namespace std;
using namespace sdsl;

#define NUM_ELEMENTS 10000000 // 10 Million
#define NUM_ITERATIONS 100000 // 100K


void sdsl_test() {
	int_vector<> v(NUM_ELEMENTS);
	for (size_t i = 0; i < NUM_ELEMENTS; i++) {
		v[i] = 0; 
	}
	cout << "Before compression width: " << (unsigned) v.width() << ", size: " <<  size_in_bytes(v) << endl;
	util::bit_compress(v);
	cout << "After compression width: " << (unsigned) v.width() << ", size: " <<  size_in_bytes(v) << endl;

	srand((unsigned) time(nullptr));
	size_t sum = 0;

	std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
	for (size_t i = 0; i < NUM_ITERATIONS; i++) {
		sum += v[rand() % NUM_ELEMENTS];
	}

	std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
	std::cout << "Sum is: " << sum << std::endl;
	std::cout << "Elapsed time: "
	          << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count()
	          << "[ns]" << std::endl;
}


void std_test() {
	std::vector<int32_t> v;
	for (size_t i = 0; i < NUM_ELEMENTS; i++) {
		v.push_back(i);
	}
	
	srand((unsigned) time(nullptr));
	size_t sum = 0;

	std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
	for (size_t i = 0; i < NUM_ITERATIONS; i++) {
		sum += v[rand() % NUM_ELEMENTS];
	}

	std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
	std::cout << "Sum is: " << sum << std::endl;
	std::cout << "Elapsed time: "
	          << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count()
	          << "[ns]" << std::endl;
}

int main(){
	//sdsl_test();
	std_test();
}
