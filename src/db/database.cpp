#include "../../include/db/database.h"
#include <iostream>
#include <algorithm>

namespace toydb {
namespace db {

Database::Database(const std::string& name)
    : name_(name) {
}

bool Database::create_table(const std::string& name, const std::vector<ColumnDef>& columns) {
    if (table_exists(name)) {
        std::cerr << "Table already exists: " << name << std::endl;
        return false;
    }
    
    // Validate column definitions
    bool has_primary_key = false;
    for (const auto& col : columns) {
        if (col.primary_key) {
            if (has_primary_key) {
                std::cerr << "Multiple primary keys are not supported" << std::endl;
                return false;
            }
            has_primary_key = true;
        }
    }
    
    tables_[name] = std::make_shared<Table>(name, columns);
    return true;
}

bool Database::drop_table(const std::string& name) {
    if (!table_exists(name)) {
        std::cerr << "Table doesn't exist: " << name << std::endl;
        return false;
    }
    
    tables_.erase(name);
    return true;
}

std::shared_ptr<Table> Database::get_table(const std::string& name) const {
    auto it = tables_.find(name);
    if (it != tables_.end()) {
        return it->second;
    }
    return nullptr;
}

std::vector<std::string> Database::list_tables() const {
    std::vector<std::string> result;
    for (const auto& [name, _] : tables_) {
        result.push_back(name);
    }
    return result;
}

bool Database::table_exists(const std::string& name) const {
    return tables_.find(name) != tables_.end();
}

} // namespace db
} // namespace toydb 