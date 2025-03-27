#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <variant>
#include <optional>
#include <functional>
#include "../storage/bplustree.h"

namespace toydb {
namespace db {

// Define the types our database can handle
using DBNull = std::monostate;
using DBInt = int64_t;
using DBFloat = double;
using DBText = std::string;
using DBValue = std::variant<DBNull, DBInt, DBFloat, DBText>;

// Column types
enum class ColumnType {
    Null,
    Int,
    Float,
    Text
};

// Column definition
struct ColumnDef {
    std::string name;
    ColumnType type;
    bool primary_key = false;
    bool not_null = false;
};

// Helper functions to work with DBValue
ColumnType value_type(const DBValue& value);
std::string type_to_string(ColumnType type);
std::string value_to_string(const DBValue& value);
bool values_equal(const DBValue& a, const DBValue& b);
bool values_less(const DBValue& a, const DBValue& b);

// Row is a vector of values
using Row = std::vector<DBValue>;

// Condition for filtering rows
struct Condition {
    std::string column_name;
    std::string op; // =, >, <, >=, <=, !=
    DBValue value;
    
    bool evaluate(const Row& row, const std::vector<ColumnDef>& columns) const;
};

// Table class representing a single database table
class Table {
public:
    Table(const std::string& name, const std::vector<ColumnDef>& columns);

    const std::string& name() const { return name_; }
    const std::vector<ColumnDef>& columns() const { return columns_; }

    // Insert a row
    bool insert_row(const Row& row);
    
    // Select rows matching the conditions
    std::vector<Row> select(const std::vector<Condition>& conditions = {}) const;
    
    // Update rows matching the conditions
    size_t update(const std::unordered_map<std::string, DBValue>& updates, 
                  const std::vector<Condition>& conditions = {});
    
    // Delete rows matching the conditions
    size_t remove(const std::vector<Condition>& conditions = {});
    
    // Get the index of a column by name
    std::optional<size_t> column_index(const std::string& name) const;

private:
    std::string name_;
    std::vector<ColumnDef> columns_;
    std::vector<Row> rows_;
    std::optional<size_t> primary_key_index_;
    
    // B+ tree index for primary key if available
    std::unique_ptr<storage::BPlusTree<DBInt, size_t>> int_index_;
    std::unique_ptr<storage::BPlusTree<DBText, size_t>> text_index_;
    
    bool row_matches(const Row& row, const std::vector<Condition>& conditions) const;
    void update_index(const DBValue& key, size_t row_index);
    bool has_index() const { return primary_key_index_.has_value(); }
};

} // namespace db
} // namespace toydb 