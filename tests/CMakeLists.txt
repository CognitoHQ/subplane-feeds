add_executable(feeds_test test_main.cpp test_decimal.cpp test_json_path.cpp test_pack.cpp test_book.cpp test_gateway.cpp test_contract.cpp test_observe.cpp test_control.cpp test_snap.cpp)
target_link_libraries(feeds_test PRIVATE feeds_core feeds_warnings doctest)
target_compile_definitions(feeds_test PRIVATE
    FEEDS_SOURCE_DIR="${CMAKE_SOURCE_DIR}")
add_test(NAME feeds_test COMMAND feeds_test)

# One fixture directory per pack (fixtures/<pack>/frames.jsonl): recorded frames through the pack must
# reproduce expected.txt byte for byte, and the same replay must reach an app through the agent.
file(GLOB FEEDS_FIXTURES LIST_DIRECTORIES true ${CMAKE_SOURCE_DIR}/fixtures/*)
foreach(dir ${FEEDS_FIXTURES})
    if(NOT EXISTS ${dir}/frames.jsonl)
        continue()
    endif()
    get_filename_component(name ${dir} NAME)
    string(REPLACE "-" "_" tname ${name})
    set(pack ${CMAKE_SOURCE_DIR}/packs/${name}.yaml)
    add_test(NAME fixture_${tname}
        COMMAND ${CMAKE_SOURCE_DIR}/tests/fixture_test.sh $<TARGET_FILE:spwsgw> ${pack} ${dir})
    # Gateway (replay) -> Aeron IPC -> subplaned -> ring -> views_app, when an agent build is given.
    add_test(NAME agent_e2e_${tname}
        COMMAND ${CMAKE_SOURCE_DIR}/tests/agent_e2e.sh $<TARGET_FILE:spwsgw> "${SUBPLANE_BUILD_DIR}"
                ${pack} ${dir} ${CMAKE_SOURCE_DIR}/contract/subplane_canonical.xml)
    set_tests_properties(agent_e2e_${tname} PROPERTIES SKIP_RETURN_CODE 77)
endforeach()

# Recap end to end: gateway -> agent recap partition <- spwsgw-snap; a late recap session's book equals the live one.
if(FEEDS_WITH_AERON)
    add_test(NAME snap_e2e_binance_spot
        COMMAND ${CMAKE_SOURCE_DIR}/tests/snap_e2e.sh $<TARGET_FILE:spwsgw> $<TARGET_FILE:spwsgw-snap> "${SUBPLANE_BUILD_DIR}"
                ${CMAKE_SOURCE_DIR}/packs/binance-spot.yaml ${CMAKE_SOURCE_DIR}/fixtures/binance-spot ${CMAKE_SOURCE_DIR}/contract/subplane_canonical.xml 1 $<IF:$<BOOL:${FEEDS_WITH_NETWORK}>,1,0>)
    set_tests_properties(snap_e2e_binance_spot PROPERTIES SKIP_RETURN_CODE 77)
endif()

# The observability HTTP routes (needs the network build for the server).
if(FEEDS_WITH_NETWORK)
    add_test(NAME http_observability
        COMMAND ${CMAKE_SOURCE_DIR}/tests/http_test.sh $<TARGET_FILE:spwsgw>
                ${CMAKE_SOURCE_DIR}/packs/binance-spot.yaml ${CMAKE_SOURCE_DIR}/fixtures/binance-spot)
    set_tests_properties(http_observability PROPERTIES SKIP_RETURN_CODE 77)
endif()
