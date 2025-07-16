cmake_minimum_required(VERSION 3.12)
project(TuGraphClientTest LANGUAGES CXX)

# 1. 设置编译选项
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

# 添加 OpenMP 支持
find_package(OpenMP REQUIRED)

# 2. 定义公共包含路径和链接选项
set(COMMON_INCLUDE_DIRS 
    # ~/tugraph/include  # 头文件在include目录中
    ~/tugraph
)

set(COMMON_LINK_LIBRARIES
    OpenMP::OpenMP_CXX  # OpenMP 支持
    lgraph
    lgraph_client_cpp_rpc
    pthread
    ssl
    crypto
)

set(COMMON_LINK_DIRECTORIES
    ~/tugraph/lib
)

# 2. 配置可执行文件
add_executable(edge_insert_test
    ~/tugraph/src/edge_insert_test.cpp
)
target_include_directories(edge_insert_test PRIVATE ${COMMON_INCLUDE_DIRS})
target_link_libraries(edge_insert_test PRIVATE ${COMMON_LINK_LIBRARIES})
target_link_directories(edge_insert_test PRIVATE ${COMMON_LINK_DIRECTORIES})

# add_executable(api_test
#     ~/tugraph/src/api_test.cpp
# )
# target_include_directories(api_test PRIVATE ${COMMON_INCLUDE_DIRS})
# target_link_libraries(api_test PRIVATE ${COMMON_LINK_LIBRARIES})
# target_link_directories(api_test PRIVATE ${COMMON_LINK_DIRECTORIES})

# 6. 设置运行时库路径 (RPATH)
# 这替代了手动修改 ld.so.conf 的操作
set(CMAKE_INSTALL_RPATH "~/tugraph/lib")  # 相对路径
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)   # 构建时使用安装路径
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE) # 使用链接路径

# 添加自定义目标以便于调试
add_custom_target(debug_build
    COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Debug ${CMAKE_SOURCE_DIR}
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target edge_insert_test
    COMMENT "Building in debug mode..."
)

# 输出构建类型信息
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")