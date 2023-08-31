#include <gtest/gtest.h>

#include "prpd_impl.h"

// initial example from the paper.
TEST(tests, verify_initial_placement)
{
    parallel_redistribute  prpd([](int x) { return x % 3; }, 3);

    Table R;
    Table S;
    // x from paper
    R.partitioned_column = { 3, 6, 9, 12, 1, 4, 7, 7, 2, 5, 5, 8};
    // a from paper
    R.non_partitioned_column = { 1, 2, 7, 1, 2, 1, 3, 1, 4, 1, 6, 1};
    // y from paper
    S.partitioned_column = { 0, 0, 3, 6, 1, 4, 4, 10, 2, 8, 8, 11 };
    S.non_partitioned_column = { 5, 1, 2, 2, 1, 2, 8, 2, 2, 2, 3, 6 };

    prpd.redistribute_initial_data(R, S);

    ASSERT_EQ(prpd.pu_collection[0].R.partitioned_column, vector({ 3, 6, 9, 12 }));
    ASSERT_EQ(prpd.pu_collection[0].R.non_partitioned_column, vector({ 1, 2, 7, 1 }));
    ASSERT_EQ(prpd.pu_collection[0].S.partitioned_column, vector({ 0, 0, 3, 6 }));
    ASSERT_EQ(prpd.pu_collection[0].S.non_partitioned_column, vector({ 5, 1, 2, 2 }));

    ASSERT_EQ(prpd.pu_collection[1].R.partitioned_column, vector({ 1, 4, 7, 7 }));
    ASSERT_EQ(prpd.pu_collection[1].R.non_partitioned_column, vector({ 2, 1, 3, 1 }));
    ASSERT_EQ(prpd.pu_collection[1].S.partitioned_column, vector({ 1, 4, 4, 10 }));
    ASSERT_EQ(prpd.pu_collection[1].S.non_partitioned_column, vector({ 1, 2, 8, 2 }));

    ASSERT_EQ(prpd.pu_collection[2].R.partitioned_column, vector({ 2, 5, 5, 8 }));
    ASSERT_EQ(prpd.pu_collection[2].R.non_partitioned_column, vector({ 4, 1, 6, 1 }));
    ASSERT_EQ(prpd.pu_collection[2].S.partitioned_column, vector({ 2, 8, 8, 11 }));
    ASSERT_EQ(prpd.pu_collection[2].S.non_partitioned_column, vector({ 2, 2, 3, 6 }));
}