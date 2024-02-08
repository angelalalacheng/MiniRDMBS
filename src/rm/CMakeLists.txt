add_library(rm rm.cc
        rm_utils.h
        rm_utils.h)
add_dependencies(rm rbfm ix googlelog)
target_link_libraries(rm rbfm ix glog)