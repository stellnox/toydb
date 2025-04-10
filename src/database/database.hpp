#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include "table.hpp"
#include "transaction.hpp"

class Database {
public:
    static Database& instance() {
        static Database instance;
        return instance;
    }
    
    // Transaction management
    uint64_t begin_transaction() {
        return TransactionManager::instance().begin_transaction();
    }
    
    void commit_transaction(uint64_t id) {
        TransactionManager::instance().commit_transaction(id);
    }
    
    void abort_transaction(uint64_t id) {
        TransactionManager::instance().abort_transaction(id);
    }
    
    // Table management
    void create_table(const std::string& name, const std::vector<Column>& columns) {
        if (tables_.find(name) != tables_.end()) {
            throw std::runtime_error("Table " + name + " already exists");
        }
        tables_[name] = std::make_unique<Table>(name, columns);
    }
    
    void drop_table(const std::string& name) {
        if (tables_.find(name) == tables_.end()) {
            throw std::runtime_error("Table " + name + " does not exist");
        }
        tables_.erase(name);
    }
    
    Table* get_table(const std::string& name) {
        auto it = tables_.find(name);
        return it != tables_.end() ? it->second.get() : nullptr;
    }
    
    // Index management
    void create_index(const std::string& table_name, const std::string& column_name, const std::string& index_name) {
        Table* table = get_table(table_name);
        if (!table) {
            throw std::runtime_error("Table " + table_name + " does not exist");
        }
        table->create_index(column_name, index_name);
    }
    
    void drop_index(const std::string& table_name, const std::string& index_name) {
        Table* table = get_table(table_name);
        if (!table) {
            throw std::runtime_error("Table " + table_name + " does not exist");
        }
        table->drop_index(index_name);
    }
    
private:
    Database() = default;
    std::unordered_map<std::string, std::unique_ptr<Table>> tables_;
}; 