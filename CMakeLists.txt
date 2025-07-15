cmake_minimum_required(VERSION 3.12)
project(TuGraphClientTest LANGUAGES CXX)

# 1. 设置编译选项
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 添加 OpenMP 支持
find_package(OpenMP REQUIRED)

# 2. 配置可执行文件
# add_executable(edge_test
#     ~/tugraph/src/edge_test.cpp
# )
add_executable(edge_insert_test
    ~/tugraph/src/edge_insert_test.cpp
)

# 3. 设置包含路径
# target_include_directories(edge_test PRIVATE
#     ~/tugraph
# )

# 4. 设置链接选项
# target_link_libraries(edge_test PRIVATE
#     OpenMP::OpenMP_CXX  # OpenMP 支持
#     lgraph
#     lgraph_client_cpp_rpc
#     pthread
#     ssl
#     crypto
# )

# 5. 设置库搜索路径
# target_link_directories(edge_test PRIVATE
#     ~/tugraph/lib
# )

target_include_directories(edge_insert_test PRIVATE
    ~/tugraph
)

# 4. 设置链接选项
target_link_libraries(edge_insert_test PRIVATE
    OpenMP::OpenMP_CXX  # OpenMP 支持
    lgraph
    lgraph_client_cpp_rpc
    pthread
    ssl
    crypto
)

# 5. 设置库搜索路径
target_link_directories(edge_insert_test PRIVATE
    ~/tugraph/lib
)
# 6. 设置运行时库路径 (RPATH)
# 这替代了手动修改 ld.so.conf 的操作
set(CMAKE_INSTALL_RPATH "~/tugraph/lib")  # 相对路径
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)   # 构建时使用安装路径
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE) # 使用链接路径
