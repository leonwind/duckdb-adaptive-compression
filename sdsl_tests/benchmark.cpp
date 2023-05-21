#include <chrono>
#include <iostream>
#include <random>
#include <sdsl/vectors.hpp>
#include "perfEvent.hpp"
//---------------------------------------------------------------------------
using namespace std;
using namespace sdsl;
//---------------------------------------------------------------------------
// Align bits to full bytes
uint8_t byte_align(uint8_t width) {
  if ((width & 0b00000111) == 0) return width;
  return (width & ~(0b111ull)) + 8ull;
}
//---------------------------------------------------------------------------
// Create one cyclic permutation using Sattolo's algorithm.
template<typename Vec>
void one_cyclic_permutation(Vec &vec, size_t size) {
	srand((unsigned)time(nullptr));
	size_t i = size;

	while (i > 1) {
		i--;
		size_t j = rand() % i;
		size_t tmp_i = vec[i];
		vec[i] = vec[j];
		vec[j] = tmp_i;
	}
}
//---------------------------------------------------------------------------
template<typename Vec>
size_t run_benchmark(const Vec &vec, size_t num_iterations) {
  size_t i = 0;
  size_t iterations = 0;

  size_t sum = 0;
  while (iterations < num_iterations) {
	  sum += vec[i];
	  i = vec[i];
	  iterations++;
  }

  return sum;
}
//---------------------------------------------------------------------------
std::vector<uint32_t> get_random_ints(size_t num_elements, size_t num_lookups) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distrib(0, num_elements - 1ull);

  std::vector<uint32_t> numbers;
  numbers.reserve(num_lookups);

  for (auto i = 0; i < num_lookups; i++)
    numbers.emplace_back(distrib(gen));

  return numbers;
}
//---------------------------------------------------------------------------
void sdsl_test(size_t num_elements, size_t num_iterations, bool align_to_bytes, bool isFirst) {
  int_vector<> v(num_elements);

  for (size_t i = 0; i < num_elements; i++) {
    v[i] = i;
  }
  one_cyclic_permutation(v, num_elements);

  auto bit_width = static_cast<uint8_t>(std::log2(num_elements - 1) + 1);
  if (align_to_bytes) bit_width = byte_align(bit_width);

  std::string prefix = "";
  if (align_to_bytes) {
	  prefix = "SDSL-Byte-Aligned";
  } else {
	  prefix = "SDSL";
  }
  util::bit_compress(v);
  util::expand_width(v, bit_width);

  BenchmarkParameters params;
  params.setParam("name", prefix);
  params.setParam("size", std::to_string(size_in_bytes(v)));
  PerfEventBlock e(num_iterations, params, isFirst);

  size_t sum = run_benchmark(v, num_iterations);
  e.e.stopCounters();

  std::cout << "SUM " << sum << std::endl;
}
//---------------------------------------------------------------------------
void std_test(size_t num_elements, size_t num_iterations, bool isFirst) {
  std::vector<int32_t> v(num_elements);
  for (size_t i = 0; i < num_elements; i++) {
    v[i] = i;
  }
  one_cyclic_permutation(v, num_elements);

  BenchmarkParameters params;
  params.setParam("name", "STD");
  params.setParam("size", v.size() * sizeof(size_t));
  PerfEventBlock e(num_iterations, params, isFirst);

  size_t sum = run_benchmark(v, num_iterations);
  e.e.stopCounters();

  std::cout << "SUM " << sum << std::endl;
}
//---------------------------------------------------------------------------
int main() {
	size_t num_iterations = 10000000; // 10 Million

	for (size_t i = 0; i < 20; ++i) {
		unsigned long long num_elements = 2ULL << i;

		sdsl_test(num_elements, num_iterations, /* align_to_bytes= */ false, /* isFirst= */ i == 0);
		sdsl_test(num_elements, num_iterations, /* align_to_bytes= */ true, /* isFirst= */ false);
		std_test(num_elements, num_iterations, /* isFirst= */ false);
	}

	//size_t num_elements = 10000000;
	//std_test(num_elements, num_iterations);
	//sdsl_test(num_elements, num_iterations, false);
	//sdsl_test(num_elements, num_iterations, true);

	//auto lookups = get_random_ints(num_elements, num_lookups);

}
//---------------------------------------------------------------------------
