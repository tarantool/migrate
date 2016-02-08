project(tarantool-xlog C)
cmake_minimum_required(VERSION 2.8 FATAL_ERROR)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

if(POLICY CMP0037)
    cmake_policy(SET CMP0037 OLD) # don't blame `test` target name
endif(POLICY CMP0037)

set_property(GLOBAL PROPERTY ALLOW_DUPLICATE_CUSTOM_TARGETS ON)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/migrate/xlog/")
message("${CMAKE_LIBRARY_OUTPUT_DIRECTORY}")

# Find Tarantool
set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)
include_directories(${TARANTOOL_INCLUDEDIR})

# Set CFLAGS
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -Wextra")

include_directories("${PROJECT_SOURCE_DIR}/")
include_directories("${PROJECT_SOURCE_DIR}/third_party/tarantool-c/include")
include_directories("${PROJECT_SOURCE_DIR}/third_party/small")
include_directories("${PROJECT_SOURCE_DIR}/third_party/")

add_subdirectory(third_party)
add_subdirectory(migrate)

# add_custom_target(test
#     WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
#     COMMAND ${PROJECT_SOURCE_DIR}/test/http.test.lua)
