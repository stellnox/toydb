#include "transaction.hpp"
#include <stdexcept>

TransactionManager& TransactionManager::instance() {
    static TransactionManager instance;
    return instance;
}

uint64_t TransactionManager::begin_transaction() {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t id = next_transaction_id_++;
    transactions_[id] = std::make_unique<Transaction>(id);
    return id;
}

void TransactionManager::commit_transaction(uint64_t id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = transactions_.find(id);
    if (it == transactions_.end()) {
        throw std::runtime_error("Transaction " + std::to_string(id) + " not found");
    }
    it->second->set_state(Transaction::State::COMMITTED);
    transactions_.erase(it);
}

void TransactionManager::abort_transaction(uint64_t id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = transactions_.find(id);
    if (it == transactions_.end()) {
        throw std::runtime_error("Transaction " + std::to_string(id) + " not found");
    }
    it->second->set_state(Transaction::State::ABORTED);
    transactions_.erase(it);
}

Transaction& TransactionManager::get_transaction(uint64_t id) {
    auto it = transactions_.find(id);
    if (it == transactions_.end()) {
        throw std::runtime_error("Transaction " + std::to_string(id) + " not found");
    }
    return *it->second;
} 