cmake_minimum_required(VERSION 3.14)
project(unit_tests)

set(DOWNLOAD_EXTRACT_TIMESTAMP true)
cmake_policy(SET CMP0135 OLD)

set(CMAKE_CXX_STANDARD 23)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(
  prpd_paper_impl_unit_tests
  prpd_impl.cpp
  unit_tests.cpp
)

target_link_libraries(
  prpd_paper_impl_unit_tests
  GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(prpd_paper_impl_unit_tests)