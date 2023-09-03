#include <vector>
#include <functional>
#include <cassert>
#include <string>
#include <set>
#include <algorithm>

using namespace std;

class Table {
public:
    // string partitioned_column_name;
    // vector<int> partitioned_column;
    // string non_partitioned_column_name;
    // vector<int> non_partitioned_column;

    vector<string> column_names;
    vector<vector<int>> columns; // column ordering.

    Table(const vector<string>& column_names, const vector<vector<int>>& columns)
    {
        assert(column_names.size() == columns.size());
        this->column_names = column_names;
        this->columns = columns;
    }

    const vector<int>& get_column(const string& name) const
    {
        auto it = find(column_names.begin(), column_names.end(), name);
        assert(it != column_names.end());
        return columns[it - column_names.begin()];
    }

    bool operator==(const Table& other) const
    {
        int num_rows = columns[0].size();

        if (num_rows != other.columns[0].size())
            return false;

        // find matching row for every row in this table.
        // todo: this is not proper eq check...
        for (int idx_row = 0; idx_row < num_rows; idx_row++)
        {
            bool found_row_other = false;
            for (int idx_row_other = 0; idx_row_other < num_rows; idx_row_other++)
            {
                bool row_eq = true;
                for (int idx_col = 0; idx_col < column_names.size(); idx_col++)
                {
                    if (columns[idx_col][idx_row] != other.columns[idx_col][idx_row])
                    {
                        row_eq = false;
                        break;
                    }
                }

                if (row_eq)
                {
                    found_row_other = true;
                    break;
                }
            }

            if (!found_row_other)
                return false;
        }

        return true;
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

    void redistribute_initial_data(const Table& R, const Table& S, string column_r, string column_s)
    {
        // assert(R.get_column(column_r).size() == R.partitioned_column.size());
        // assert(S.non_partitioned_column.size() == S.partitioned_column.size());

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