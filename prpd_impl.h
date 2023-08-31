#include <vector>
#include <functional>
#include <cassert>
#include <string>

using namespace std;

class Table {
public:
    string partitioned_column_name;
    vector<int> partitioned_column;
    string non_partitioned_column_name;
    vector<int> non_partitioned_column;
};

struct PU {
    // original tables on this node
    Table R;
    Table S;

    Table redis_spool_R;
    Table redis_spool_S;

    Table dup_spool_R;
    Table dup_spool_S;

    Table loc_spool_R;
    Table loc_spool_S;
};

class parallel_redistribute {
public:
    vector<PU> pu_collection;
    std::function<int(int)> hash_function;

    parallel_redistribute(std::function<int(int)> hash_function, int node_number)
    {
        this->hash_function = hash_function;
        pu_collection.resize(node_number);
    }

    void redistribute_initial_data(const Table& R, const Table& S)
    {
        assert(R.non_partitioned_column.size() == R.partitioned_column.size());
        assert(S.non_partitioned_column.size() == S.partitioned_column.size());

        for (int i = 0; i < R.non_partitioned_column.size(); i++) {
            int node = hash_function(R.partitioned_column[i]);
            pu_collection[node].R.non_partitioned_column.push_back(R.non_partitioned_column[i]);
            pu_collection[node].R.partitioned_column.push_back(R.partitioned_column[i]);
        }

        for (int i = 0; i < S.non_partitioned_column.size(); i++) {
            int node = hash_function(S.partitioned_column[i]);
            pu_collection[node].S.non_partitioned_column.push_back(S.non_partitioned_column[i]);
            pu_collection[node].S.partitioned_column.push_back(S.partitioned_column[i]);
        }
    }
};