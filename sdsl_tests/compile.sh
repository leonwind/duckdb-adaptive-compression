if [ "$#" -ne 1 ] ; then
  echo "Usage: ./$(basename "$0") <file.cpp>"
	exit 1
fi

g++ -std=c++11 -DNDEBUG -O3 -I/home/leon/Uni/IMLAB/duckdb-adaptive-compression/third_party/sdsl/include -L/home/leon/Uni/IMLAB/duckdb-adaptive-compression/third_party/sdsl/lib $1 -lsdsl -ldivsufsort -ldivsufsort64
