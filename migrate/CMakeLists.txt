add_subdirectory(xlog)

# Install
install(FILES init.lua DESTINATION ${TARANTOOL_LUADIR}/migrate)
install(FILES helpers.lua DESTINATION ${TARANTOOL_LUADIR}/migrate)
install(FILES xdir.lua    DESTINATION ${TARANTOOL_LUADIR}/migrate)
