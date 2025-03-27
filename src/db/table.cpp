#include "../../include/db/table.h"
#include <iostream>
#include <algorithm>
#include <sstream>

namespace toydb {
namespace db {

// Helper functions implementation
ColumnType value_type(const DBValue& value) {
    if (std::holds_alternative<DBNull>(value)) return ColumnType::Null;
    if (std::holds_alternative<DBInt>(value)) return ColumnType::Int;
    if (std::holds_alternative<DBFloat>(value)) return ColumnType::Float;
    if (std::holds_alternative<DBText>(value)) return ColumnType::Text;
    return ColumnType::Null; // Should never happen
}

std::string type_to_string(ColumnType type) {
    switch (type) {
        case ColumnType::Null: return "NULL";
        case ColumnType::Int: return "INT";
        case ColumnType::Float: return "FLOAT";
        case ColumnType::Text: return "TEXT";
        default: return "UNKNOWN";
    }
}

std::string value_to_string(const DBValue& value) {
    if (std::holds_alternative<DBNull>(value)) return "NULL";
    if (std::holds_alternative<DBInt>(value)) return std::to_string(std::get<DBInt>(value));
    if (std::holds_alternative<DBFloat>(value)) return std::to_string(std::get<DBFloat>(value));
    if (std::holds_alternative<DBText>(value)) return std::get<DBText>(value);
    return "UNKNOWN"; // Should never happen
}

bool values_equal(const DBValue& a, const DBValue& b) {
    if (value_type(a) != value_type(b)) return false;
    
    if (std::holds_alternative<DBNull>(a)) return true; // NULL == NULL
    if (std::holds_alternative<DBInt>(a)) return std::get<DBInt>(a) == std::get<DBInt>(b);
    if (std::holds_alternative<DBFloat>(a)) return std::get<DBFloat>(a) == std::get<DBFloat>(b);
    if (std::holds_alternative<DBText>(a)) return std::get<DBText>(a) == std::get<DBText>(b);
    
    return false; // Should never happen
}

bool values_less(const DBValue& a, const DBValue& b) {
    // NULL is considered less than everything
    if (std::holds_alternative<DBNull>(a)) return !std::holds_alternative<DBNull>(b);
    if (std::holds_alternative<DBNull>(b)) return false;
    
    // If types are different, compare based on type precedence
    if (value_type(a) != value_type(b)) {
        return static_cast<int>(value_type(a)) < static_cast<int>(value_type(b));
    }
    
    // Compare values of the same type
    if (std::holds_alternative<DBInt>(a)) return std::get<DBInt>(a) < std::get<DBInt>(b);
    if (std::holds_alternative<DBFloat>(a)) return std::get<DBFloat>(a) < std::get<DBFloat>(b);
    if (std::holds_alternative<DBText>(a)) return std::get<DBText>(a) < std::get<DBText>(b);
    
    return false; // Should never happen
}

// Condition implementation
bool Condition::evaluate(const Row& row, const std::vector<ColumnDef>& columns) const {
    // Find column index
    size_t col_idx = 0;
    bool found = false;
    for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].name == column_name) {
            col_idx = i;
            found = true;
            break;
        }
    }
    
    if (!found || col_idx >= row.size()) return false;
    
    const DBValue& row_value = row[col_idx];
    
    if (op == "=") return values_equal(row_value, value);
    if (op == "!=") return !values_equal(row_value, value);
    if (op == "<") return values_less(row_value, value);
    if (op == ">") return !values_less(row_value, value) && !values_equal(row_value, value);
    if (op == "<=") return values_less(row_value, value) || values_equal(row_value, value);
    if (op == ">=") return !values_less(row_value, value);
    
    return false; // Unknown operator
}

// Table implementation
Table::Table(const std::string& name, const std::vector<ColumnDef>& columns)
    : name_(name), columns_(columns), primary_key_index_(std::nullopt) {
    
    // Find primary key column if any
    for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].primary_key) {
            primary_key_index_ = i;
            
            // Create appropriate index based on primary key type
            if (columns[i].type == ColumnType::Int) {
                int_index_ = std::make_unique<storage::BPlusTree<DBInt, size_t>>();
            } else if (columns[i].type == ColumnType::Text) {
                text_index_ = std::make_unique<storage::BPlusTree<DBText, size_t>>();
            }
            break;
        }
    }
}

std::optional<size_t> Table::column_index(const std::string& name) const {
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i].name == name) {
            return i;
        }
    }
    return std::nullopt;
}

bool Table::insert_row(const Row& row) {
    // Verify row size
    if (row.size() != columns_.size()) {
        std::cerr << "Column count mismatch" << std::endl;
        return false;
    }
    
    // Verify column types and constraints
    for (size_t i = 0; i < columns_.size(); ++i) {
        const auto& col = columns_[i];
        const auto& val = row[i];
        
        // Check NOT NULL constraint
        if (col.not_null && std::holds_alternative<DBNull>(val)) {
            std::cerr << "NULL value in NOT NULL column: " << col.name << std::endl;
            return false;
        }
        
        // Check type compatibility
        if (!std::holds_alternative<DBNull>(val) && 
            value_type(val) != col.type) {
            std::cerr << "Type mismatch in column " << col.name << std::endl;
            return false;
        }
        
        // Check primary key uniqueness
        if (col.primary_key && has_index()) {
            if (primary_key_index_ && i == *primary_key_index_) {
                if (col.type == ColumnType::Int && 
                    int_index_->find(std::get<DBInt>(val)).has_value()) {
                    std::cerr << "Duplicate primary key: " << value_to_string(val) << std::endl;
                    return false;
                } else if (col.type == ColumnType::Text && 
                           text_index_->find(std::get<DBText>(val)).has_value()) {
                    std::cerr << "Duplicate primary key: " << value_to_string(val) << std::endl;
                    return false;
                }
            }
        }
    }
    
    // Add row and update index
    size_t row_idx = rows_.size();
    rows_.push_back(row);
    
    // Update index if we have a primary key
    if (primary_key_index_) {
        update_index(row[*primary_key_index_], row_idx);
    }
    
    return true;
}

void Table::update_index(const DBValue& key, size_t row_index) {
    if (!primary_key_index_) return;
    
    const auto& pk_column = columns_[*primary_key_index_];
    
    if (pk_column.type == ColumnType::Int && int_index_) {
        int_index_->insert(std::get<DBInt>(key), row_index);
    } else if (pk_column.type == ColumnType::Text && text_index_) {
        text_index_->insert(std::get<DBText>(key), row_index);
    }
}

bool Table::row_matches(const Row& row, const std::vector<Condition>& conditions) const {
    if (conditions.empty()) return true;
    
    for (const auto& condition : conditions) {
        if (!condition.evaluate(row, columns_)) {
            return false;
        }
    }
    
    return true;
}

std::vector<Row> Table::select(const std::vector<Condition>& conditions) const {
    std::vector<Row> result;
    
    // If we have a specific primary key condition, use the index
    if (primary_key_index_ && conditions.size() == 1) {
        const auto& condition = conditions[0];
        if (condition.column_name == columns_[*primary_key_index_].name && 
            condition.op == "=") {
            
            const auto& pk_column = columns_[*primary_key_index_];
            
            if (pk_column.type == ColumnType::Int && 
                std::holds_alternative<DBInt>(condition.value)) {
                
                auto row_idx_opt = int_index_->find(std::get<DBInt>(condition.value));
                if (row_idx_opt && *row_idx_opt < rows_.size()) {
                    result.push_back(rows_[*row_idx_opt]);
                }
                return result;
                
            } else if (pk_column.type == ColumnType::Text && 
                       std::holds_alternative<DBText>(condition.value)) {
                
                auto row_idx_opt = text_index_->find(std::get<DBText>(condition.value));
                if (row_idx_opt && *row_idx_opt < rows_.size()) {
                    result.push_back(rows_[*row_idx_opt]);
                }
                return result;
            }
        }
    }
    
    // Otherwise, do a full table scan
    for (const auto& row : rows_) {
        if (row_matches(row, conditions)) {
            result.push_back(row);
        }
    }
    
    return result;
}

size_t Table::update(const std::unordered_map<std::string, DBValue>& updates, 
                     const std::vector<Condition>& conditions) {
    // Resolve column indices for updates
    std::unordered_map<size_t, DBValue> col_idx_to_value;
    for (const auto& [col_name, value] : updates) {
        auto idx = column_index(col_name);
        if (idx) {
            col_idx_to_value[*idx] = value;
        }
    }
    
    size_t count = 0;
    
    for (size_t i = 0; i < rows_.size(); ++i) {
        auto& row = rows_[i];
        
        if (row_matches(row, conditions)) {
            // Check if we're updating the primary key
            bool updating_pk = primary_key_index_ && 
                               col_idx_to_value.find(*primary_key_index_) != col_idx_to_value.end();
            
            // If updating primary key, check uniqueness
            if (updating_pk) {
                const auto& new_pk_value = col_idx_to_value[*primary_key_index_];
                const auto& pk_col = columns_[*primary_key_index_];
                
                if (pk_col.type == ColumnType::Int) {
                    auto existing = int_index_->find(std::get<DBInt>(new_pk_value));
                    if (existing && *existing != i) {
                        continue; // Duplicate key, skip update
                    }
                } else if (pk_col.type == ColumnType::Text) {
                    auto existing = text_index_->find(std::get<DBText>(new_pk_value));
                    if (existing && *existing != i) {
                        continue; // Duplicate key, skip update
                    }
                }
            }
            
            // Update values
            for (const auto& [col_idx, value] : col_idx_to_value) {
                // Check type compatibility
                if (!std::holds_alternative<DBNull>(value) && 
                    value_type(value) != columns_[col_idx].type) {
                    continue; // Type mismatch, skip this field
                }
                
                row[col_idx] = value;
            }
            
            // Update index if primary key was changed
            if (updating_pk) {
                update_index(row[*primary_key_index_], i);
            }
            
            count++;
        }
    }
    
    return count;
}

size_t Table::remove(const std::vector<Condition>& conditions) {
    // This is a simplified implementation that doesn't update indices properly
    size_t initial_size = rows_.size();
    
    rows_.erase(
        std::remove_if(rows_.begin(), rows_.end(), 
                      [this, &conditions](const Row& row) {
                          return row_matches(row, conditions);
                      }),
        rows_.end()
    );
    
    // In a real implementation, we would rebuild the indices
    
    return initial_size - rows_.size();
}

} // namespace db
} // namespace toydb 