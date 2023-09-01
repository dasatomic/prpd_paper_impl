#include <vector>
#include <functional>
#include <cassert>
#include <string>
#include <set>
#include <algorithm>

using namespace std;

class Table {
public:
    string partitioned_column_name;
    vector<int> partitioned_column;
    string non_partitioned_column_name;
    vector<int> non_partitioned_column;

    Table() = default;

    Table(string partitioned_column_name, string non_partitioned_column_name,
          vector<int> partitioned_column, vector<int> non_partitioned_column)
        : partitioned_column_name(partitioned_column_name), non_partitioned_column_name(non_partitioned_column_name),
        partitioned_column(partitioned_column), non_partitioned_column(non_partitioned_column)
    {
    }

    const vector<int>& get_column(const string& name) const
    {
        if (name == partitioned_column_name)
            return partitioned_column;
        else if (name == non_partitioned_column_name)
            return non_partitioned_column;
        else
            assert(false);
    }

    const vector<string> get_column_names() const
    {
        return { partitioned_column_name, non_partitioned_column_name };
    }

    bool operator==(const Table& other) const
    {
        auto column1My = this->get_column(this->partitioned_column_name);
        auto column2My = this->get_column(this->non_partitioned_column_name);
        auto column1Other = other.get_column(this->partitioned_column_name);
        auto column2Other = other.get_column(this->non_partitioned_column_name);

        vector<int> column1MySorted(column1My.size());
        vector<int> column2MySorted(column2My.size());
        vector<int> column1OtherSorted(column1Other.size());
        vector<int> column2OtherSorted(column2Other.size());

        partial_sort_copy(column1My.begin(), column1My.end(), column1MySorted.begin(), column1MySorted.end());
        partial_sort_copy(column2My.begin(), column2My.end(), column2MySorted.begin(), column2MySorted.end());
        partial_sort_copy(column1Other.begin(), column1Other.end(), column1OtherSorted.begin(), column1OtherSorted.end());
        partial_sort_copy(column2Other.begin(), column2Other.end(), column2OtherSorted.begin(), column2OtherSorted.end());

        return column1MySorted == column1OtherSorted && column2MySorted == column2OtherSorted;
    }
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

        for (int i = 0; i < pu_collection.size(); i++)
        {
            pu_collection[i].R.partitioned_column_name = R.partitioned_column_name;
            pu_collection[i].R.non_partitioned_column_name = R.non_partitioned_column_name;
            pu_collection[i].S.partitioned_column_name = S.partitioned_column_name;
            pu_collection[i].S.non_partitioned_column_name = S.non_partitioned_column_name;
        }

        for (int i = 0; i < R.non_partitioned_column.size(); i++) {
            int node = hash_function(R.partitioned_column[i]);
            pu_collection[node].R.non_partitioned_column.push_back(R.non_partitioned_column[i]);
            pu_collection[node].R.partitioned_column.push_back(R.partitioned_column[i]);
            pu_collection[node].R.non_partitioned_column_name = R.non_partitioned_column_name;
            pu_collection[node].R.partitioned_column_name = R.partitioned_column_name;
        }

        for (int i = 0; i < S.non_partitioned_column.size(); i++) {
            int node = hash_function(S.partitioned_column[i]);
            pu_collection[node].S.non_partitioned_column.push_back(S.non_partitioned_column[i]);
            pu_collection[node].S.partitioned_column.push_back(S.partitioned_column[i]);
            pu_collection[node].S.non_partitioned_column_name = S.non_partitioned_column_name;
            pu_collection[node].S.partitioned_column_name = S.partitioned_column_name;
        }
    }

    void redistribute_non_partitioned_column()
    {
        for (int i = 0; i < pu_collection.size(); i++) {
            pu_collection[i].redis_spool_R.partitioned_column_name = pu_collection[i].R.non_partitioned_column_name;
            pu_collection[i].redis_spool_R.non_partitioned_column_name = pu_collection[i].R.partitioned_column_name;

            for (int j = 0; j < pu_collection[i].R.non_partitioned_column.size(); j++) {
                int node = hash_function(pu_collection[i].R.non_partitioned_column[j]);

                pu_collection[node].redis_spool_R.partitioned_column.push_back(pu_collection[i].R.non_partitioned_column[j]);
                pu_collection[node].redis_spool_R.non_partitioned_column.push_back(pu_collection[i].R.partitioned_column[j]);

            }

            pu_collection[i].redis_spool_S.partitioned_column_name = pu_collection[i].S.non_partitioned_column_name;
            pu_collection[i].redis_spool_S.non_partitioned_column_name = pu_collection[i].S.partitioned_column_name;

            for (int j = 0; j < pu_collection[i].S.non_partitioned_column.size(); j++) {
                int node = hash_function(pu_collection[i].S.non_partitioned_column[j]);
                pu_collection[node].redis_spool_S.partitioned_column.push_back(pu_collection[i].S.non_partitioned_column[j]);
                pu_collection[node].redis_spool_S.non_partitioned_column.push_back(pu_collection[i].S.partitioned_column[j]);
            }
        }
    }

    void prpd_non_partitioned_column(const set<int>& L1, const set<int>& L2)
    {
        // TODO: refactor this properly...
        set<int> L1L2_intersection;
        set_intersection(L1.begin(), L1.end(), L2.begin(), L2.end(), inserter(L1L2_intersection, L1L2_intersection.begin()));
        assert(L1L2_intersection.size() == 0);

        for (int idx_node = 0; idx_node < pu_collection.size(); idx_node++)
        {
            // for loc keep same distributed column.
            pu_collection[idx_node].loc_spool_R.partitioned_column_name = pu_collection[idx_node].R.partitioned_column_name;
            pu_collection[idx_node].loc_spool_R.non_partitioned_column_name = pu_collection[idx_node].R.non_partitioned_column_name;
            // same for dup
            pu_collection[idx_node].dup_spool_R.partitioned_column_name = pu_collection[idx_node].R.partitioned_column_name;
            pu_collection[idx_node].dup_spool_R.non_partitioned_column_name = pu_collection[idx_node].R.non_partitioned_column_name;
            // swap for redis
            pu_collection[idx_node].redis_spool_R.partitioned_column_name = pu_collection[idx_node].R.non_partitioned_column_name;
            pu_collection[idx_node].redis_spool_R.non_partitioned_column_name = pu_collection[idx_node].R.partitioned_column_name;

            // for loc keep same distributed column.
            pu_collection[idx_node].loc_spool_S.partitioned_column_name = pu_collection[idx_node].S.partitioned_column_name;
            pu_collection[idx_node].loc_spool_S.non_partitioned_column_name = pu_collection[idx_node].S.non_partitioned_column_name;
            // same for dup
            pu_collection[idx_node].dup_spool_S.partitioned_column_name = pu_collection[idx_node].S.partitioned_column_name;
            pu_collection[idx_node].dup_spool_S.non_partitioned_column_name = pu_collection[idx_node].S.non_partitioned_column_name;
            // swap for redis
            pu_collection[idx_node].redis_spool_S.partitioned_column_name = pu_collection[idx_node].S.non_partitioned_column_name;
            pu_collection[idx_node].redis_spool_S.non_partitioned_column_name = pu_collection[idx_node].S.partitioned_column_name;

            for (int idx_row = 0; idx_row < pu_collection[idx_node].R.partitioned_column.size(); idx_row++)
            {
                // build loc out of all skewed rows in R (where R in L1)
                if (L1.find(pu_collection[idx_node].R.non_partitioned_column[idx_row]) != L1.end())
                {
                    pu_collection[idx_node].loc_spool_R.partitioned_column.push_back(pu_collection[idx_node].R.partitioned_column[idx_row]);
                    pu_collection[idx_node].loc_spool_R.non_partitioned_column.push_back(pu_collection[idx_node].R.non_partitioned_column[idx_row]);
                }
                // build dup out of all skewed rows in R (where R in L2). Move to all the other nodes.
                else if (L2.find(pu_collection[idx_node].R.non_partitioned_column[idx_row]) != L2.end())
                {
                    for (int idx_node_dup = 0; idx_node_dup < pu_collection.size(); idx_node_dup++)
                    {
                        // this is not partitioned in any way...
                        pu_collection[idx_node_dup].dup_spool_R.partitioned_column.push_back(pu_collection[idx_node].R.partitioned_column[idx_row]);
                        pu_collection[idx_node_dup].dup_spool_R.non_partitioned_column.push_back(pu_collection[idx_node].R.non_partitioned_column[idx_row]);
                    }
                }
                else
                {
                    // redist normally everything else.
                    int node = hash_function(pu_collection[idx_node].R.non_partitioned_column[idx_row]);
                    pu_collection[node].redis_spool_R.partitioned_column.push_back(pu_collection[idx_node].R.non_partitioned_column[idx_row]);
                    pu_collection[node].redis_spool_R.non_partitioned_column.push_back(pu_collection[idx_node].R.partitioned_column[idx_row]);
                }
            }

            // same thing for S, with L1 and L2 swapped.
            for (int idx_row = 0; idx_row < pu_collection[idx_node].S.partitioned_column.size(); idx_row++)
            {
                // build loc out of all skewed rows in S (where S in L2)
                if (L2.find(pu_collection[idx_node].S.non_partitioned_column[idx_row]) != L2.end())
                {
                    pu_collection[idx_node].loc_spool_S.partitioned_column.push_back(pu_collection[idx_node].S.partitioned_column[idx_row]);
                    pu_collection[idx_node].loc_spool_S.non_partitioned_column.push_back(pu_collection[idx_node].S.non_partitioned_column[idx_row]);
                }
                // build dup out of all skewed rows in S (where S in L1). Move to all the other nodes.
                else if (L1.find(pu_collection[idx_node].S.non_partitioned_column[idx_row]) != L1.end())
                {
                    for (int idx_node_dup = 0; idx_node_dup < pu_collection.size(); idx_node_dup++)
                    {
                        // this is not partitioned in any way...
                        pu_collection[idx_node_dup].dup_spool_S.partitioned_column.push_back(pu_collection[idx_node].S.partitioned_column[idx_row]);
                        pu_collection[idx_node_dup].dup_spool_S.non_partitioned_column.push_back(pu_collection[idx_node].S.non_partitioned_column[idx_row]);
                    }
                }
                else
                {
                    // redist normally everything else.
                    int node = hash_function(pu_collection[idx_node].S.non_partitioned_column[idx_row]);
                    pu_collection[node].redis_spool_S.partitioned_column.push_back(pu_collection[idx_node].S.non_partitioned_column[idx_row]);
                    pu_collection[node].redis_spool_S.non_partitioned_column.push_back(pu_collection[idx_node].S.partitioned_column[idx_row]);
                }
            }
        }
    }
};