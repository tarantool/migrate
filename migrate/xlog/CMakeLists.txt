# Build
if (APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -undefined suppress -flat_namespace")
endif(APPLE)

set (xlog_sources
        xlog.c
        tuple.c
        table.c
)

add_library(xlog SHARED ${xlog_sources})
set_target_properties(xlog PROPERTIES PREFIX "" OUTPUT_NAME "internal")
target_link_libraries(xlog "tntrpl")
target_link_libraries(xlog "tnt")
target_link_libraries(xlog "msgpuck")
target_link_libraries(xlog "small")

install(TARGETS xlog LIBRARY DESTINATION ${TARANTOOL_LIBDIR}/migrate/xlog/)
install(FILES init.lua DESTINATION ${TARANTOOL_LUADIR}/migrate/xlog/)
