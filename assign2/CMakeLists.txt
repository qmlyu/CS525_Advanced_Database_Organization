cmake_minimum_required(VERSION 3.23)
project(assign2 C)

set(CMAKE_C_STANDARD 11)

include_directories(.)

add_executable(assign2
        dberror.c
        dberror.h
        storage_mgr.h
        storage_mgr.c
        test_helper.h
        buffer_mgr.h
        test_assign2_1.c
#        test_fifo.c
        buffer_mgr_stat.h
        buffer_mgr_stat.c
        dt.h
        buffer_mgr.c)
