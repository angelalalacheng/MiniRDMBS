add_library(ix ix.cc
        ix_utils.h
)
add_dependencies(ix pfm googlelog)
target_link_libraries(ix pfm glog)