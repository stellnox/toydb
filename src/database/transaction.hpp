#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>

class Transaction {
public:
    enum class State {
        ACTIVE,
        COMMITTED,
        ABORTED
    };
    
    Transaction(uint64_t id) : id_(id), state_(State::ACTIVE) {}
    
    void add_table_state(const std::string& table_name, 
                        const std::vector<std::vector<Value>>& state) {
        table_states_[table_name] = state;
    }
    
    const std::vector<std::vector<Value>>& get_table_state(
        const std::string& table_name) const {
        auto it = table_states_.find(table_name);
        if (it == table_states_.end()) {
            throw std::runtime_error("No state found for table " + table_name);
        }
        return it->second;
    }
    
    void set_state(State state) { state_ = state; }
    State state() const { return state_; }
    uint64_t id() const { return id_; }
    
private:
    uint64_t id_;
    State state_;
    std::unordered_map<std::string, std::vector<std::vector<Value>>> table_states_;
};

class TransactionManager {
public:
    static TransactionManager& instance() {
        static TransactionManager instance;
        return instance;
    }
    
    uint64_t begin_transaction() {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t id = next_transaction_id_++;
        transactions_[id] = std::make_unique<Transaction>(id);
        return id;
    }
    
    void commit_transaction(uint64_t id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = transactions_.find(id);
        if (it == transactions_.end()) {
            throw std::runtime_error("Transaction " + std::to_string(id) + " not found");
        }
        it->second->set_state(Transaction::State::COMMITTED);
        transactions_.erase(it);
    }
    
    void abort_transaction(uint64_t id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = transactions_.find(id);
        if (it == transactions_.end()) {
            throw std::runtime_error("Transaction " + std::to_string(id) + " not found");
        }
        it->second->set_state(Transaction::State::ABORTED);
        transactions_.erase(it);
    }
    
    Transaction& get_transaction(uint64_t id) {
        auto it = transactions_.find(id);
        if (it == transactions_.end()) {
            throw std::runtime_error("Transaction " + std::to_string(id) + " not found");
        }
        return *it->second;
    }
    
private:
    TransactionManager() : next_transaction_id_(1) {}
    ~TransactionManager() = default;
    TransactionManager(const TransactionManager&) = delete;
    TransactionManager& operator=(const TransactionManager&) = delete;
    
    std::mutex mutex_;
    uint64_t next_transaction_id_;
    std::unordered_map<uint64_t, std::unique_ptr<Transaction>> transactions_;
}; 