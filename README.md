# ToyDB

A simple database implementation in C++ with CLI support.

## Features

- B+ Tree index for efficient data storage and retrieval
- CRUD operations (Create, Read, Update, Delete)
- Command-line interface for database operations
- Simple SQL-like query language
- Transaction support with ACID properties

## Building

```bash
mkdir build
cd build
cmake ..
make
```

## Usage

```bash
# Start the database
./toydb

# Command examples (from the interactive CLI)
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
INSERT INTO users VALUES (1, "John Doe", 30);
SELECT * FROM users;
UPDATE users SET age = 31 WHERE id = 1;
DELETE FROM users WHERE id = 1;

# Transaction examples
BEGIN TRANSACTION;         # Returns a transaction ID
INSERT INTO users VALUES (2, "Jane Doe", 25, transaction_id);
UPDATE users SET age = 26 WHERE id = 2, transaction_id);
COMMIT TRANSACTION transaction_id;    # Commit changes
# or
ABORT TRANSACTION transaction_id;    # Abort/rollback changes
```

## Project Structure

- `include/` - Header files
- `src/` - Source files
- `src/storage/` - Storage engine (B+ Tree implementation)
- `src/parser/` - SQL parser
- `src/cli/` - Command-line interface
- `src/db/` - Database engine core functionality
- `src/database/` - Database core components including transaction management 