cmake_minimum_required(VERSION 3.23)
project(assign4_b_tree_index C)

set(CMAKE_C_STANDARD 11)

include_directories(.)

add_executable(assign4_b_tree_index
        btree_mgr.c
        btree_mgr.h
        buffer_mgr.c
        buffer_mgr.h
        buffer_mgr_stat.c
        buffer_mgr_stat.h
        dberror.c
        dberror.h
        dt.h
        expr.c
        expr.h
        record_mgr.c
        record_mgr.h
        rm_serializer.c
        storage_mgr.c
        storage_mgr.h
        tables.h
        test_assign4_1.c
#        test_expr.c
        test_helper.h)
