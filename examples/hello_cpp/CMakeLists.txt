cmake_minimum_required(VERSION 3.20)
project(hello_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow REQUIRED)

add_library(hello_cpp MODULE src/hello_cpp.cpp)
target_link_libraries(hello_cpp PRIVATE Arrow::arrow_shared)

# Python extensions use .so on all UNIX platforms (including macOS).
set_target_properties(hello_cpp PROPERTIES
    PREFIX "lib"
    SUFFIX ".so"
)

# scikit-build-core: install into the Python package directory.
install(TARGETS hello_cpp LIBRARY DESTINATION hello_cpp)
