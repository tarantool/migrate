project(tarantool-xlog C)
cmake_minimum_required(VERSION 2.8 FATAL_ERROR)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

# Find Tarantool
set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)
include_directories(${TARANTOOL_INCLUDEDIR})

# Set CFLAGS
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -Wextra")

include_directories("${PROJECT_SOURCE_DIR}/")
include_directories("${PROJECT_SOURCE_DIR}/third_party/tarantool-c/include")

add_subdirectory(third_party)
add_subdirectory(migrate)

# add_custom_target(test
#     WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
#     COMMAND ${PROJECT_SOURCE_DIR}/test/http.test.lua)
