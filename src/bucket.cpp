// Compile with g++ -std=c++17 -fopenmp -O3 -o bucket src/bucket.cpp

#include <vector>
#include <deque>
#include <omp.h>
#include <random>
#include <iostream>
#include <ostream>
#include <chrono>
#include <algorithm>

using namespace std;

void bucket_sort(vector<int32_t>& vector_to_sort);
void insertion_sort(vector<int32_t>& vector_to_sort);
pair<int32_t, int32_t> get_vector_values_range(vector<int32_t>& vector);

// Change this value
// For quicksort you can go up to 30, beyond that you need >16 GB of RAM
// for the process only.
// For insertion sort the maximum is arround 23, beyond that the execution time 
// is too high
static const size_t VALUES = 1 << 30;

int main()
{
	cout << "Creating vector" << endl;

	auto values = vector<int32_t>();
	values.reserve(VALUES);

	random_device dev;
	mt19937 rng(dev());
	uniform_int_distribution<mt19937::result_type> dist(0, VALUES << 1);

	cout << "Generating values" << endl;

	for (auto _i = 0; _i < VALUES; ++_i)
	{
		values.emplace_back(dist(rng));
	}

	cout << "Sorting..." << endl;

	auto start = chrono::high_resolution_clock::now();

	bucket_sort(values);

	auto end = chrono::high_resolution_clock::now();

	chrono::duration<double> diff = end - start;
	auto milis = chrono::duration_cast<chrono::milliseconds>(diff);

	cout << "Finished Sorting!" << endl;
	cout << "Time taken: " << diff.count() << " seconds" << endl;
	cout << "Time taken: " << milis.count() << " milliseconds" << endl;

	bool is_sorted = true;
	auto size = values.size() - 1;
#pragma omp parallel for reduction(&& : is_sorted) schedule(static, static_cast<uint32_t>(sqrt(VALUES)))
	for (auto i = 0; i < size; ++i)
	{
		is_sorted = is_sorted && values[i] <= values[i + 1];
	}

	cout << "Is sorted? " << (is_sorted ? "true" : "false") << endl;

	return EXIT_SUCCESS;
}

pair<int32_t, int32_t> get_vector_values_range(vector<int32_t>& vector)
{
	auto min = INT32_MAX;
	auto max = INT32_MIN;

	for (const auto& value : vector)
	{
		min = value <= min ? value : min;
		max = value >= max ? value : max;
	}

	return make_pair(min, max);
}

void bucket_sort(vector<int32_t> &vector_to_sort) 
{
	// If the sorting algorithm used in each bucket is a fast (n*logn), then is better to use 
	// fewer buckets, given that the performance of the algorithm will not be drastically impacted
	// by the increse in size, and the lower number of buckets will incur in less overhead for task
	// execution. If using QuickSort a good number of buckets is log2 of the number of elements to sort.
	// If the sorting algorithm is a slow one (n^2), then the best option is to have a lot of 
	// buckets, because increasing the size of each bucket will incur in a large time penalty
	// because the sorting is slow. The best option for when using insertion sort is to use
	// the square root of the number of values to sort.
	
	auto number_of_buckets = static_cast<uint32_t>(log2(VALUES));
	auto buckets = vector<vector<int32_t>>(number_of_buckets, vector<int32_t>());

	auto ranges = get_vector_values_range(vector_to_sort);
	auto range = static_cast<int64_t>(ranges.second) - static_cast<int64_t>(ranges.first);
	auto step = static_cast<size_t>(ceil(static_cast<double_t>(range+1) / static_cast<double_t>(number_of_buckets)));

	cout << "Step size is " << step << " the range of values is " << range << endl;
	cout << "Each task will sort aproximadelly " << vector_to_sort.size() / number_of_buckets << " elements, " << number_of_buckets << " buckets will be used!" << endl;

#pragma omp parallel for shared(number_of_buckets, buckets) schedule(static, 1)
	for (auto i = 0; i < number_of_buckets; ++i)
	{
		for (auto const& value : vector_to_sort) 
		{
			auto bucket_index = (value - ranges.first) / step;
			if (bucket_index == i) buckets[bucket_index].emplace_back(value);
		}
	}

	auto start = chrono::high_resolution_clock::now();


#pragma omp parallel for shared(number_of_buckets, buckets) schedule(static, 1)
	for (size_t i = 0; i < number_of_buckets; ++i)
	{
		//insertion_sort(buckets[i]);
		sort(buckets[i].begin(), buckets[i].end()); // Use cpp quicksort
	}

	auto end = chrono::high_resolution_clock::now();

	chrono::duration<double> diff = end - start;
	auto milis = chrono::duration_cast<chrono::milliseconds>(diff);

	cout << "Finished Sorting each bucket!" << endl;
	cout << "Time taken: " << diff.count() << " seconds" << endl;
	cout << "Time taken: " << milis.count() << " milliseconds" << endl;


#pragma omp parallel for shared(number_of_buckets, buckets, vector_to_sort)
	for (size_t i = 0; i < number_of_buckets; ++i)
	{
		size_t start_index = 0;
		for (size_t c = 0; c < i; ++c)
		{
			start_index += buckets[c].size();
		}

		for (size_t c = 0; c < buckets[i].size(); ++c)
		{
			vector_to_sort[c + start_index] = buckets[i][c];
		}
	}
}

void insertion_sort(vector<int32_t> &vector_to_sort)
{
	size_t i = 1;
	size_t j = 0;
	auto size = vector_to_sort.size();
	
	for (i = 1; i < size; ++i)
	{
		for (j = i; j > 0 && vector_to_sort[j - 1] > vector_to_sort[j]; --j)
		{
			swap(vector_to_sort[j - 1], vector_to_sort[j]);
		}
	}
}