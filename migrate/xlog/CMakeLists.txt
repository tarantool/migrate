# Build
if (APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -undefined suppress -flat_namespace")
endif(APPLE)

set (xlog_sources
        xlog.c
        tuple.c
        table.c
        mpstream.c
)

add_library(xlog SHARED ${xlog_sources})
set_target_properties(xlog PROPERTIES PREFIX "" OUTPUT_NAME "internal")
target_link_libraries(xlog tntrpl)
target_link_libraries(xlog tnt)
target_link_libraries(xlog small)
target_link_libraries(xlog msgpuck)

install(TARGETS xlog LIBRARY DESTINATION ${TARANTOOL_INSTALL_LIBDIR}/${PROJECT_NAME}/xlog/)
install(FILES init.lua       DESTINATION ${TARANTOOL_INSTALL_LUADIR}/${PROJECT_NAME}/xlog/)
