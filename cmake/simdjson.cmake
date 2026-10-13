# simdjson (On Demand API) for the per-message JSON parse, the gateway's whole decode cost.
include(FetchContent)
set(FEEDS_SIMDJSON_TAG "v5.0.4" CACHE STRING "simdjson git tag")
set(SIMDJSON_BUILD_STATIC_LIB ON CACHE BOOL "" FORCE)
set(SIMDJSON_JUST_LIBRARY ON CACHE BOOL "" FORCE)
set(SIMDJSON_DEVELOPER_MODE OFF CACHE BOOL "" FORCE)
FetchContent_Declare(simdjson
    GIT_REPOSITORY https://github.com/simdjson/simdjson.git
    GIT_TAG        ${FEEDS_SIMDJSON_TAG}
    GIT_SHALLOW    TRUE
    EXCLUDE_FROM_ALL)
FetchContent_MakeAvailable(simdjson)
add_library(feeds_simdjson INTERFACE)
target_link_libraries(feeds_simdjson INTERFACE simdjson)
get_target_property(_sj_inc simdjson INTERFACE_INCLUDE_DIRECTORIES)
target_include_directories(feeds_simdjson SYSTEM INTERFACE ${_sj_inc})
