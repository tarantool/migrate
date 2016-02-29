add_subdirectory(xlog)

# Install
install(FILES init.lua            DESTINATION ${TARANTOOL_INSTALL_LUADIR}/${PROJECT_NAME})
install(FILES xdir.lua            DESTINATION ${TARANTOOL_INSTALL_LUADIR}/${PROJECT_NAME})
install(FILES utils/init.lua      DESTINATION ${TARANTOOL_INSTALL_LUADIR}/${PROJECT_NAME}/utils)
install(FILES utils/checktype.lua DESTINATION ${TARANTOOL_INSTALL_LUADIR}/${PROJECT_NAME}/utils)
