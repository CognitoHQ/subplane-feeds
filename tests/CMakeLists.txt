add_executable(feeds_test test_main.cpp test_decimal.cpp test_json_path.cpp test_pack.cpp test_book.cpp test_gateway.cpp test_contract.cpp)
target_link_libraries(feeds_test PRIVATE feeds_core feeds_warnings doctest)
target_compile_definitions(feeds_test PRIVATE
    FEEDS_SOURCE_DIR="${CMAKE_SOURCE_DIR}")
add_test(NAME feeds_test COMMAND feeds_test)

# Recorded Binance frames through the pack: output must match the expected text exactly.
if(EXISTS ${CMAKE_SOURCE_DIR}/fixtures/binance-spot/btcusdt.jsonl)
    add_test(NAME fixture_binance_spot
        COMMAND ${CMAKE_SOURCE_DIR}/tests/fixture_test.sh $<TARGET_FILE:subplane-gateway>
                ${CMAKE_SOURCE_DIR}/packs/binance-spot.yaml ${CMAKE_SOURCE_DIR}/fixtures/binance-spot)
endif()

# Gateway (replay) -> Aeron IPC -> subplaned -> ring -> views_app, when an agent build is given.
add_test(NAME agent_e2e
    COMMAND ${CMAKE_SOURCE_DIR}/tests/agent_e2e.sh $<TARGET_FILE:subplane-gateway> "${SUBPLANE_BUILD_DIR}"
            ${CMAKE_SOURCE_DIR}/packs/binance-spot.yaml ${CMAKE_SOURCE_DIR}/fixtures/binance-spot ${CMAKE_SOURCE_DIR}/contract/subplane_canonical.xml)
set_tests_properties(agent_e2e PROPERTIES SKIP_RETURN_CODE 77)
