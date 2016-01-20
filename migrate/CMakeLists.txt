# Build
if (APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -undefined suppress -flat_namespace")
endif(APPLE)

add_library(xlog SHARED xlog/xlog.c)
set_target_properties(xlog PROPERTIES PREFIX "" OUTPUT_NAME "xlog")
target_link_libraries(xlog "tntrpl")
target_link_libraries(xlog "tnt")

# Install
install(TARGETS xlog LIBRARY DESTINATION ${TARANTOOL_LIBDIR}/migrate)
install(FILES migrate.lua DESTINATION ${TARANTOOL_LUADIR}/migrate)
install(FILES helpers.lua DESTINATION ${TARANTOOL_LUADIR}/migrate)
install(FILES xdir.lua    DESTINATION ${TARANTOOL_LUADIR}/migrate)
