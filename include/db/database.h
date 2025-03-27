#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <optional>
#include "table.h"

namespace toydb {
namespace db {

class Database {
public:
    Database(const std::string& name);
    
    const std::string& name() const { return name_; }
    
    // Create a new table
    bool create_table(const std::string& name, const std::vector<ColumnDef>& columns);
    
    // Drop a table
    bool drop_table(const std::string& name);
    
    // Get a table by name
    std::shared_ptr<Table> get_table(const std::string& name) const;
    
    // List all tables
    std::vector<std::string> list_tables() const;
    
    // Check if a table exists
    bool table_exists(const std::string& name) const;

private:
    std::string name_;
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
};

} // namespace db
} // namespace toydb 