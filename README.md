# MPMC Ring Buffer in C

## Overview
Implementation of a Multi-Producer and Multi-Consumer Ring Buffer
using C and POSIX threads.

## Features
- Fixed-size circular buffer
- FIFO operations
- Multiple producers and consumers
- Mutex-based synchronization
- Condition variables
- Configurable overflow policies:
  - BLOCK
  - DROP_NEWEST
  - DROP_OLDEST

## Technologies
- C
- GCC
- MSYS2
- POSIX Threads (pthreads)

## How to Compile

gcc main.c ring_buffer.c -o ring_buffer.exe -pthread

## How to Run

./ring_buffer.exe