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
	cout << size_in_mega_bytes(v) << endl;
	util::bit_compress(v);
	cout << size_in_mega_bytes(v) << endl;
	vlc_vector<> vv(v);
	cout << size_in_mega_bytes(vv) << endl;
}

void test4() {
	int_vector<> v(100, 0);
	v[0] = 10;
	util::bit_compress(v);
	for (size_t i{0}; i < 20; ++i) {
		v[0] = i;
		cout << v[0] << endl;
	}
}

void test5() {
	int_vector<> v(100, 0);
	uint64_t* ptr = v.data();
	for (size_t i = 0; i < 100; ++i) {
		ptr[i] = i;
	}
	std::cout << ptr[10] << std::endl;
	std::cout << size_in_bytes(v) << std::endl;
	util::bit_compress(v);
	std::cout << size_in_bytes(v) << std::endl;
	ptr = v.data();
	std::cout << ptr[10] << std::endl;
	std::cout << v[10] << std::endl;
}

void test6() {
	int_vector<> v(100, 0);
	int_vector<>* ptr = &v;
	(*ptr)[0] = 80;
	util::bit_compress(*ptr);
	std::cout << (*ptr)[0];
}

void test7() {
	int_vector<> v(100);
	for (size_t i = 0; i < 100; ++i) {
		v[i] = i;
	}
	util::bit_compress(v);
	std::cout << v[10] << std::endl;
	uint64_t* ptr = v.data();
	std::cout << *((ptr)+10);
}

void test8() {
	int_vector<> v(100);
	uint64_t* ptr = v.data();
	for (size_t i = 0; i < 100; ++i) {
		ptr[i] = i;
	}
	util::bit_compress(v);
	std::cout << v[10] << std::endl;
}

void test9_sub(int_vector<64>* ptr) {
	for (size_t i = 0; i < ptr->size(); ++i) {
		(*ptr)[i] = -i;
	}
	//util::bit_compress(*ptr);
	std::cout << (*ptr)[10] << std::endl;
	std::cout << ptr->size() << std::endl;
}

void test9() {
	int_vector<64> v(100);
	test9_sub(&v);
}

void test10() {
	int_vector<> v(100);
	//v.width(32);
	v[0] = 300;
	std::cout << v[0] << std::endl;
}

void test11() {
	uint32_t b = 0;
	int a = b;
	std::cout << "a val: " << a << std::endl;
}

int main(){
	//test1();
	//test2();
	//test3();
	//test4();
	//test5();
	//test6();
	//test7();
	//test8();
	//test9();
	//test10();
	test11();
}
