#include <iostream>
#include <sdsl/vectors.hpp>

using namespace std;
using namespace sdsl;

void test1() {
	int_vector<> v(100, 0);
	v[0] = 2;
	cout << size_in_bytes(v) << endl;
	util::bit_compress(v);
    cout << v << endl;
    cout << size_in_bytes(v) << endl;
	v[0] = 9;
	util::bit_compress(v);
	cout << v << endl;
}

void test2() {
	int_vector<> v(100, 0);
	v[0] = 5;
	util::bit_compress(v);
	cout << size_in_mega_bytes(v) << endl;
	cout << v << endl;
	vlc_vector<> vv(v);
	cout << size_in_mega_bytes(vv) << endl;
	v[0] = 7;
	cout << v << endl;
}

void test3() {
	int_vector<> v(10*(1<<20), 0);
	v[0] = 1ULL<<63;
	util::bit_compress(v);
	cout << size_in_mega_bytes(v) << endl;
	vlc_vector<> vv(v);
	cout << size_in_mega_bytes(vv) << endl;
}

int main(){
	//test1();
	//test2();
	test3();
}