cmake_minimum_required(VERSION 3.23)
project(assign3 C)

set(CMAKE_C_STANDARD 11)

include_directories(.)

add_executable(assign3
        main.c
        test_assign3_1.c
        buffer_mgr.h
        buffer_mgr.c
        buffer_mgr_stat.h
        buffer_mgr_stat.c
        dberror.h
        dberror.c
        dt.h
        expr.h
        expr.c
        record_mgr.h
        record_mgr.c
        rm_serializer.c
        storage_mgr.h
        storage_mgr.c
        tables.h
        test_expr.c
        test_helper.h)
