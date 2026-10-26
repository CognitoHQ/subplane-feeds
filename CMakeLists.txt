cmake_minimum_required(VERSION 3.30)
project(subplane_feeds VERSION 0.1.0 LANGUAGES C CXX)

# Same toolchain floor as the agent: C++20, GCC 11 on Ubuntu 22.04.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG")

option(FEEDS_BUILD_TESTS "Build unit and fixture tests" ON)
option(FEEDS_WERROR "Treat warnings as errors" ON)
option(FEEDS_WITH_AERON "Build the Aeron publisher (fetches the Aeron C client + media driver)" ON)
option(FEEDS_WITH_NETWORK "Build the WebSocket/REST client (needs Boost.Beast + OpenSSL)" ON)
set(SUBPLANE_BUILD_DIR "" CACHE PATH "An agent build directory (subplaned, views_app) for the end-to-end test; empty skips it")

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(YAML_CPP REQUIRED IMPORTED_TARGET yaml-cpp)

add_library(feeds_warnings INTERFACE)
target_compile_options(feeds_warnings INTERFACE -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion)
if(FEEDS_WERROR)
    target_compile_options(feeds_warnings INTERFACE -Werror)
endif()

add_library(doctest INTERFACE)
target_include_directories(doctest INTERFACE third_party/doctest)

include(cmake/simdjson.cmake)
if(FEEDS_WITH_AERON)
    include(cmake/aeron.cmake)
endif()

# Everything that is not the network client: pack, mapping, book strategies, publishers, replay.
# Builds and tests without Boost, so the data path is testable from recorded frames alone.
add_library(feeds_core STATIC
    src/pack.cpp
    src/registry.cpp
    src/json_path.cpp
    src/decimal.cpp
    src/mapper.cpp
    src/instruments.cpp
    src/book/book.cpp
    src/book/binance_diff.cpp
    src/book/l2_inband.cpp
    src/publisher_stdout.cpp
    src/replayer.cpp
    src/recorder.cpp
    src/gateway.cpp
    src/control.cpp
    src/snap/snap.cpp
    src/snap/recap_server.cpp
    src/snap/gateway_link.cpp
    src/obs/prom_writer.cpp
    src/obs/endpoint_key.cpp
    src/obs/observe.cpp
    src/obs/snap_observe.cpp
)
target_include_directories(feeds_core PUBLIC src contract)
target_link_libraries(feeds_core PUBLIC PkgConfig::YAML_CPP feeds_simdjson Threads::Threads PRIVATE feeds_warnings)
if(FEEDS_WITH_AERON)
    target_sources(feeds_core PRIVATE src/publisher_aeron.cpp src/aeron_io.cpp)
    target_link_libraries(feeds_core PUBLIC feeds_aeron)
    target_compile_definitions(feeds_core PUBLIC FEEDS_WITH_AERON=1)
endif()

target_compile_definitions(feeds_core PUBLIC FEEDS_VERSION="${PROJECT_VERSION}")

add_executable(spwsgw src/main.cpp)
target_link_libraries(spwsgw PRIVATE feeds_core feeds_warnings)
# The snapshot service only speaks Aeron (its gateway's streams and the agent's recap channel).
if(FEEDS_WITH_AERON)
    add_executable(spwsgw-snap src/snap_main.cpp)
    target_link_libraries(spwsgw-snap PRIVATE feeds_core feeds_warnings)
endif()
if(FEEDS_WITH_NETWORK)
    find_package(Boost 1.74 REQUIRED)
    find_package(OpenSSL REQUIRED)
    target_sources(spwsgw PRIVATE src/ws_client.cpp src/rest_client.cpp src/http_server.cpp)
    target_include_directories(spwsgw SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
    target_link_libraries(spwsgw PRIVATE OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(spwsgw PRIVATE FEEDS_WITH_NETWORK=1)
    if(FEEDS_WITH_AERON)
        # The snap's --http uses the gateway's HTTP server.
        target_sources(spwsgw-snap PRIVATE src/http_server.cpp)
        target_include_directories(spwsgw-snap SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
        target_compile_definitions(spwsgw-snap PRIVATE FEEDS_WITH_NETWORK=1)
    endif()
endif()
install(TARGETS spwsgw DESTINATION bin)
if(FEEDS_WITH_AERON)
    install(TARGETS spwsgw-snap DESTINATION bin)
endif()

if(FEEDS_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
