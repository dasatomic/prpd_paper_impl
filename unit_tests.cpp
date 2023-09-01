#include <gtest/gtest.h>

#include "prpd_impl.h"
#include <tuple>
#include <algorithm>

using namespace std;

tuple<Table, Table> generate_data_paper_example()
{
    Table R;
    R.partitioned_column_name = "x";
    R.non_partitioned_column_name= "a";
    Table S;
    S.partitioned_column_name = "y";
    S.non_partitioned_column_name = "b";
    // x from paper
    R.partitioned_column = { 3, 6, 9, 12, 1, 4, 7, 7, 2, 5, 5, 8};
    // a from paper
    R.non_partitioned_column = { 1, 2, 7, 1, 2, 1, 3, 1, 4, 1, 6, 1};
    // y from paper
    S.partitioned_column = { 0, 0, 3, 6, 1, 4, 4, 10, 2, 8, 8, 11 };
    S.non_partitioned_column = { 5, 1, 2, 2, 1, 2, 8, 2, 2, 2, 3, 6 };

    return make_tuple(R, S);
}

// initial example from the paper.
TEST(tests, verify_initial_placement)
{
    parallel_redistribute  prpd([](int x) { return x % 3; }, 3);
    auto [R, S] = generate_data_paper_example();
    prpd.redistribute_initial_data(R, S);

    ASSERT_EQ(prpd.pu_collection[0].R.partitioned_column, vector({ 3, 6, 9, 12 }));
    ASSERT_EQ(prpd.pu_collection[0].R.non_partitioned_column, vector({ 1, 2, 7, 1 }));
    ASSERT_EQ(prpd.pu_collection[0].S.partitioned_column, vector({ 0, 0, 3, 6 }));
    ASSERT_EQ(prpd.pu_collection[0].S.non_partitioned_column, vector({ 5, 1, 2, 2 }));
    ASSERT_EQ(prpd.pu_collection[0].R.partitioned_column_name, "x");
    ASSERT_EQ(prpd.pu_collection[0].R.non_partitioned_column_name, "a");
    ASSERT_EQ(prpd.pu_collection[0].S.partitioned_column_name, "y");
    ASSERT_EQ(prpd.pu_collection[0].S.non_partitioned_column_name, "b");

    ASSERT_EQ(prpd.pu_collection[1].R.partitioned_column, vector({ 1, 4, 7, 7 }));
    ASSERT_EQ(prpd.pu_collection[1].R.non_partitioned_column, vector({ 2, 1, 3, 1 }));
    ASSERT_EQ(prpd.pu_collection[1].S.partitioned_column, vector({ 1, 4, 4, 10 }));
    ASSERT_EQ(prpd.pu_collection[1].S.non_partitioned_column, vector({ 1, 2, 8, 2 }));

    ASSERT_EQ(prpd.pu_collection[2].R.partitioned_column, vector({ 2, 5, 5, 8 }));
    ASSERT_EQ(prpd.pu_collection[2].R.non_partitioned_column, vector({ 4, 1, 6, 1 }));
    ASSERT_EQ(prpd.pu_collection[2].S.partitioned_column, vector({ 2, 8, 8, 11 }));
    ASSERT_EQ(prpd.pu_collection[2].S.non_partitioned_column, vector({ 2, 2, 3, 6 }));
}

TEST(tests, verify_standard_join_redistribution)
{
    parallel_redistribute  prpd([](int x) { return x % 3; }, 3);
    auto [R, S] = generate_data_paper_example();
    prpd.redistribute_initial_data(R, S);
    prpd.redistribute_non_partitioned_column();

    ASSERT_EQ(prpd.pu_collection[0].redis_spool_R.partitioned_column_name, "a");
    ASSERT_EQ(prpd.pu_collection[0].redis_spool_R.non_partitioned_column_name, "x");
    ASSERT_EQ(prpd.pu_collection[0].redis_spool_S.partitioned_column_name, "b");
    ASSERT_EQ(prpd.pu_collection[0].redis_spool_S.non_partitioned_column_name, "y");

    auto redis_node_0 = prpd.pu_collection[0].redis_spool_R;
    auto redis_node_1 = prpd.pu_collection[1].redis_spool_R;
    ASSERT_EQ(redis_node_0.partitioned_column, vector({ 3, 6 }));
    ASSERT_EQ(redis_node_0.non_partitioned_column, vector({ 7, 5 }));

    // 6 x 1 in R on node 2
    ASSERT_EQ(std::count_if(redis_node_1.partitioned_column.begin(), redis_node_1.partitioned_column.end(), [](int x) { return x == 1; }), 6);

    // 6 x 2 in S on node 2
    auto redis_node_2_S = prpd.pu_collection[2].redis_spool_S;
    ASSERT_EQ(std::count_if(redis_node_2_S.partitioned_column.begin(), redis_node_2_S.partitioned_column.end(), [](int x) { return x == 2; }), 6);
}

TEST(tests, verify_prpd_join_distribution)
{
    parallel_redistribute  prpd([](int x) { return x % 3; }, 3);
    auto [R, S] = generate_data_paper_example();
    prpd.redistribute_initial_data(R, S);

    set<int> L1 {1};
    set<int> L2 {2};
    prpd.prpd_non_partitioned_column(L1, L2);

    auto node1 = prpd.pu_collection[0];
    auto node2 = prpd.pu_collection[1];
    auto node3 = prpd.pu_collection[2];
    ASSERT_EQ(node1.dup_spool_R.get_column("x"), vector({ 6, 1 }));
    ASSERT_EQ(node1.dup_spool_R.get_column("a"), vector({ 2, 2 }));
    ASSERT_EQ(node1.loc_spool_R.get_column("x"), vector({ 3, 12 }));
    ASSERT_EQ(node1.loc_spool_R.get_column("a"), vector({ 1, 1 }));
    ASSERT_EQ(node1.redis_spool_R.get_column("x"), vector({ 7, 5 }));
    ASSERT_EQ(node1.redis_spool_R.get_column("a"), vector({ 3, 6 }));

    ASSERT_EQ(node1.dup_spool_S.get_column("y"), vector({ 0, 1 }));
    ASSERT_EQ(node1.dup_spool_S.get_column("b"), vector({ 1, 1 }));
    ASSERT_EQ(node1.loc_spool_S.get_column("y"), vector({ 3, 6 }));
    ASSERT_EQ(node1.loc_spool_S.get_column("b"), vector({ 2, 2 }));
    ASSERT_EQ(node1.redis_spool_S.get_column("y"), vector({ 8, 11 }));
    ASSERT_EQ(node1.redis_spool_S.get_column("b"), vector({ 3, 6 }));

    // node2
    ASSERT_EQ(node2.dup_spool_R.get_column("x"), vector({ 6, 1 }));
    ASSERT_EQ(node2.dup_spool_R.get_column("a"), vector({ 2, 2 }));
    ASSERT_EQ(node2.loc_spool_R.get_column("x"), vector({ 4, 7 }));
    ASSERT_EQ(node2.loc_spool_R.get_column("a"), vector({ 1, 1 }));
    // order might be different so using table comparisong.
    // fix other examples later.
    ASSERT_EQ(node2.redis_spool_R, Table("x", "a", vector({ 2, 9}), vector({4, 7})));

    ASSERT_EQ(node2.dup_spool_S.get_column("y"), vector({ 0, 1 }));
    ASSERT_EQ(node2.dup_spool_S.get_column("b"), vector({ 1, 1 }));
    ASSERT_EQ(node2.loc_spool_S.get_column("y"), vector({ 4, 10 }));
    ASSERT_EQ(node2.loc_spool_S.get_column("b"), vector({ 2, 2 }));
    ASSERT_EQ(node2.redis_spool_S.get_column("y"), vector<int>());
    ASSERT_EQ(node2.redis_spool_S.get_column("b"), vector<int>());

    // node3
    ASSERT_EQ(node3.redis_spool_R, Table("x", "a", vector<int>(), vector<int>()));
    ASSERT_EQ(node3.loc_spool_R, Table("x", "a", vector<int>({5, 8}), vector<int>({1, 1})));
    ASSERT_EQ(node3.dup_spool_R, Table("x", "a", vector<int>({6, 1}), vector<int>({2, 2})));
    ASSERT_EQ(node3.redis_spool_S, Table("y", "b", vector<int>({0, 4}), vector<int>({5, 8})));
    ASSERT_EQ(node3.dup_spool_S, Table("y", "b", vector<int>({0, 1}), vector<int>({1, 1})));
    ASSERT_EQ(node3.loc_spool_S, Table("y", "b", vector<int>({2, 8}), vector<int>({2, 2})));
}