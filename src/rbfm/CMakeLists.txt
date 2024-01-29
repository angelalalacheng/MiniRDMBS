add_library(rbfm rbfm.cc
        rbfm_utils.h)
add_dependencies(rbfm pfm googlelog)
target_link_libraries(rbfm pfm glog)