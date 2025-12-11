
# Record Manager - Assignment 3

A simple record manager implementation for managing tables with fixed schemas. Supports record insertion, deletion, updates, and conditional scans through a buffer pool interface.

## Features

- **Table Management**: Create, open, close, and delete tables
- **Record Operations**: Insert, delete, update, and retrieve records by RID
- **Conditional Scans**: Scan tables with boolean expression conditions
- **Fixed Schema**: Support for INT, FLOAT, STRING, and BOOL data types
- **Buffer Pool Integration**: All page access through buffer manager
- **Free Space Management**: Efficient linked-list based free space tracking

## Building

```bash
make
```

This will compile all source files and create two test executables:
- `test_expr` - Expression evaluation tests
- `test_assign3_1` - Record manager tests

## Running Tests

```bash
./test_expr
./test_assign3_1
```

## Cleaning

```bash
make clean
```

## Project Structure

- `storage_mgr.c/h` - Storage manager for page file operations
- `buffer_mgr.c/h` - Buffer pool manager with replacement strategies
- `record_mgr.c/h` - Record manager implementation
- `expr.c/h` - Expression evaluation for scan conditions
- `dberror.c/h` - Error handling and return codes
- `tables.h` - Data structures for schemas, records, and values
- `test_assign3_1.c` - Test suite for record manager
- `test_expr.c` - Test suite for expressions

## Page Layout

Each data page contains:
- **Header** (12 bytes): numSlots, freeSlots, nextFreePage pointer
- **Tombstone markers**: 1 byte per slot (marks used/free)
- **Record data**: Fixed-size records stored sequentially

Page 0 is reserved for table metadata (schema information).

## Record IDs

Records are identified by RID (Record ID) consisting of:
- Page number
- Slot number within the page

## Requirements

- GCC compiler
- Make (optional, can compile manually)
- C99 standard

