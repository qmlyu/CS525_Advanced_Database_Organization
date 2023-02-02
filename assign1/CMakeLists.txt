cmake_minimum_required(VERSION 3.23)
project(assign1 C)

set(CMAKE_C_STANDARD 11)

include_directories(.)

add_executable(assign1
        dberror.c
        dberror.h
        storage_mgr.h
        storage_mgr.c
        test_assign1_1.c
        test_helper.h)
