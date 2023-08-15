#include <fstream>
#include <thread>
#include <vector>
#include <iostream>

// Loads values from binary file into vector using multiple threads.
template<typename T>
static std::vector<T> ParallelLoadData(const std::string &filename, size_t max_index = UINT64_MAX, bool print = true) {
  std::vector<T> data;
  std::ifstream in(filename, std::ios::binary);
  if (!in.is_open()) {
    std::cerr << "unable to open " << filename << std::endl;
    exit(EXIT_FAILURE);
  }
  // Read size.
  uint64_t size;
  in.read(reinterpret_cast<char *>(&size), sizeof(uint64_t));
  in.close();

  size = std::min(size, max_index);

  data.resize(size);
  static uint32_t nthreads = std::thread::hardware_concurrency();

  auto read_in_thread = [&](uint32_t thread_id) {
    size_t begin = (size / nthreads) * thread_id;
    size_t end = (size / nthreads) * (thread_id + 1);
    if (thread_id == nthreads - 1) end = size;

    std::ifstream in_file(filename, std::ios::binary);
    in_file.seekg(sizeof(uint64_t) + sizeof(T) * begin);
    in_file.read(reinterpret_cast<char *>(data.data() + begin), (end - begin) * sizeof(T));
  };

  std::vector<std::thread> threads;
  for (auto thread_id = 0; thread_id < nthreads; thread_id++)
    threads.push_back(std::thread(read_in_thread, thread_id));

  for (auto &thread: threads)
    thread.join();

  std::cout << "read " << data.size() << " values from " << filename << std::endl;
  return data;
}

// Writes values from vector into binary file.
template<typename T>
static void WriteData(const std::vector<T> &data, const std::string &filename, const bool print = true) {
    std::ofstream out(filename, std::ios_base::trunc | std::ios::binary);
    if (!out.is_open()) {
      std::cerr << "unable to open " << filename << std::endl;
      exit(EXIT_FAILURE);
    }
    // Write size.
    const uint64_t size = data.size();
    out.write(reinterpret_cast<const char *>(&size), sizeof(uint64_t));
    // Write values.
    out.write(reinterpret_cast<const char *>(data.data()), size * sizeof(T));
    out.close();
    std::cout << "wrote " << data.size() << " values to " << filename << std::endl;
}
