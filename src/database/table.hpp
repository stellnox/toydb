#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include "column.hpp"
#include "index.hpp"
#include "transaction.hpp"

class Table {
public:
    Table(const std::string& name, const std::vector<Column>& columns)
        : name_(name), columns_(columns) {}
    
    // Transaction-aware operations
    void insert(const std::vector<Value>& values, uint64_t transaction_id = 0) {
        if (values.size() != columns_.size()) {
            throw std::runtime_error("Number of values does not match number of columns");
        }
        
        // Validate data types
        for (size_t i = 0; i < values.size(); ++i) {
            if (!columns_[i].validate_value(values[i])) {
                throw std::runtime_error("Invalid value type for column " + columns_[i].name());
            }
        }
        
        // If transaction is active, store the original state
        if (transaction_id > 0) {
            auto& transaction = TransactionManager::instance().get_transaction(transaction_id);
            if (transaction.state() == Transaction::State::ACTIVE) {
                // Store the current state before modification
                transaction.add_table_state(name_, get_current_state());
            }
        }
        
        // Insert the row
        rows_.push_back(values);
        
        // Update indexes
        for (auto& index : indexes_) {
            index.second->insert(values[index.second->column_index()], rows_.size() - 1);
        }
    }
    
    void update(const std::string& column_name, const Value& new_value, 
                const std::string& where_column, const Value& where_value,
                uint64_t transaction_id = 0) {
        size_t column_idx = get_column_index(column_name);
        size_t where_idx = get_column_index(where_column);
        
        // If transaction is active, store the original state
        if (transaction_id > 0) {
            auto& transaction = TransactionManager::instance().get_transaction(transaction_id);
            if (transaction.state() == Transaction::State::ACTIVE) {
                transaction.add_table_state(name_, get_current_state());
            }
        }
        
        // Update matching rows
        for (size_t i = 0; i < rows_.size(); ++i) {
            if (rows_[i][where_idx] == where_value) {
                rows_[i][column_idx] = new_value;
                
                // Update indexes
                for (auto& index : indexes_) {
                    if (index.second->column_index() == column_idx) {
                        index.second->update(i, new_value);
                    }
                }
            }
        }
    }
    
    void delete_rows(const std::string& column_name, const Value& value,
                    uint64_t transaction_id = 0) {
        size_t column_idx = get_column_index(column_name);
        
        // If transaction is active, store the original state
        if (transaction_id > 0) {
            auto& transaction = TransactionManager::instance().get_transaction(transaction_id);
            if (transaction.state() == Transaction::State::ACTIVE) {
                transaction.add_table_state(name_, get_current_state());
            }
        }
        
        // Delete matching rows
        for (size_t i = rows_.size(); i > 0; --i) {
            if (rows_[i-1][column_idx] == value) {
                // Update indexes
                for (auto& index : indexes_) {
                    index.second->remove(i-1);
                }
                
                // Remove the row
                rows_.erase(rows_.begin() + i - 1);
            }
        }
    }
    
    // Index management
    void create_index(const std::string& column_name, const std::string& index_name) {
        if (indexes_.find(index_name) != indexes_.end()) {
            throw std::runtime_error("Index " + index_name + " already exists");
        }
        
        size_t column_idx = get_column_index(column_name);
        indexes_[index_name] = std::make_unique<Index>(column_idx);
        
        // Build index from existing data
        for (size_t i = 0; i < rows_.size(); ++i) {
            indexes_[index_name]->insert(rows_[i][column_idx], i);
        }
    }
    
    void drop_index(const std::string& index_name) {
        if (indexes_.find(index_name) == indexes_.end()) {
            throw std::runtime_error("Index " + index_name + " does not exist");
        }
        indexes_.erase(index_name);
    }
    
    // Getters
    const std::string& name() const { return name_; }
    const std::vector<Column>& columns() const { return columns_; }
    const std::vector<std::vector<Value>>& rows() const { return rows_; }
    
private:
    size_t get_column_index(const std::string& column_name) const {
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (columns_[i].name() == column_name) {
                return i;
            }
        }
        throw std::runtime_error("Column " + column_name + " does not exist");
    }
    
    std::vector<std::vector<Value>> get_current_state() const {
        return rows_;
    }
    
    std::string name_;
    std::vector<Column> columns_;
    std::vector<std::vector<Value>> rows_;
    std::unordered_map<std::string, std::unique_ptr<Index>> indexes_;
}; 