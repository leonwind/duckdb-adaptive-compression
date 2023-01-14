if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

file(GLOB libFiles "${CMAKE_CURRENT_SOURCE_DIR}/lib/*.a")

add_library(duckdb_sdsl STATIC IMPORTED)

set_target_properties(duckdb_sdsl PROPERTIES IMPORTED_LOCATION ${libFiles})
set_target_properties(duckdb_sdsl PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/include)
#set_target_properties(duckdb_sdsl PROPERTIES EXPORT_NAME duckdb_sdsl)

#install(TARGETS pthread_task DESTINATION ${CMAKE_INSTALL_PREFIX})

#disable_target_warnings(duckdb_sdsl)
