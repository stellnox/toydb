#pragma once

#include <vector>
#include <memory>
#include <optional>
#include <functional>
#include <algorithm>
#include <stdexcept>

namespace toydb {
namespace storage {

template<typename Key, typename Value, size_t Order = 4>
class BPlusTree {
public:
    BPlusTree() : root_(std::make_shared<LeafNode>()) {}

    // Insert a key-value pair into the tree
    void insert(const Key& key, const Value& value) {
        auto result = root_->insert(key, value);
        if (result.has_value()) {
            // Need to create a new root
            auto new_root = std::make_shared<InternalNode>();
            new_root->keys.push_back(result->key);
            new_root->children.push_back(root_);
            new_root->children.push_back(result->node);
            root_ = new_root;
        }
    }

    // Find a value by key
    std::optional<Value> find(const Key& key) const {
        return root_->find(key);
    }

    // Update a value by key
    bool update(const Key& key, const Value& value) {
        return root_->update(key, value);
    }

    // Remove a key-value pair from the tree
    bool remove(const Key& key) {
        auto result = root_->remove(key);
        if (result && root_->is_internal() && 
            static_cast<InternalNode*>(root_.get())->keys.empty()) {
            // Root is empty, update root
            root_ = static_cast<InternalNode*>(root_.get())->children[0];
        }
        return result;
    }

    // Range scan - execute function on all elements in range [start, end]
    void range_scan(const Key& start, const Key& end, 
                   std::function<void(const Key&, const Value&)> func) const {
        root_->range_scan(start, end, func);
    }

private:
    // Forward declarations
    class Node;
    class LeafNode;
    class InternalNode;

    struct InsertResult {
        Key key;
        std::shared_ptr<Node> node;
    };

    // Base Node class
    class Node {
    public:
        virtual ~Node() = default;
        virtual std::optional<InsertResult> insert(const Key& key, const Value& value) = 0;
        virtual std::optional<Value> find(const Key& key) const = 0;
        virtual bool update(const Key& key, const Value& value) = 0;
        virtual bool remove(const Key& key) = 0;
        virtual void range_scan(const Key& start, const Key& end,
                              std::function<void(const Key&, const Value&)> func) const = 0;
        
        virtual bool is_leaf() const = 0;
        bool is_internal() const { return !is_leaf(); }
    };

    // Leaf Node implementation
    class LeafNode : public Node {
    public:
        std::vector<Key> keys;
        std::vector<Value> values;
        std::shared_ptr<LeafNode> next; // For range scans

        bool is_leaf() const override { return true; }

        std::optional<InsertResult> insert(const Key& key, const Value& value) override {
            auto it = std::lower_bound(keys.begin(), keys.end(), key);
            auto idx = it - keys.begin();

            if (it != keys.end() && *it == key) {
                // Key already exists, update value
                values[idx] = value;
                return std::nullopt;
            }

            keys.insert(it, key);
            values.insert(values.begin() + idx, value);

            // Check if we need to split the node
            if (keys.size() > Order) {
                auto new_leaf = std::make_shared<LeafNode>();
                int mid = keys.size() / 2;
                
                // Copy the second half to the new leaf
                new_leaf->keys.assign(keys.begin() + mid, keys.end());
                new_leaf->values.assign(values.begin() + mid, values.end());
                
                // Update this node to contain only the first half
                keys.resize(mid);
                values.resize(mid);
                
                // Link the nodes
                new_leaf->next = next;
                next = new_leaf;
                
                // Return the split information
                InsertResult result;
                result.key = new_leaf->keys.front();
                result.node = new_leaf;
                return result;
            }
            
            return std::nullopt;
        }

        std::optional<Value> find(const Key& key) const override {
            auto it = std::lower_bound(keys.begin(), keys.end(), key);
            if (it != keys.end() && *it == key) {
                return values[it - keys.begin()];
            }
            return std::nullopt;
        }

        bool update(const Key& key, const Value& value) override {
            auto it = std::lower_bound(keys.begin(), keys.end(), key);
            if (it != keys.end() && *it == key) {
                values[it - keys.begin()] = value;
                return true;
            }
            return false;
        }

        bool remove(const Key& key) override {
            auto it = std::lower_bound(keys.begin(), keys.end(), key);
            if (it != keys.end() && *it == key) {
                auto idx = it - keys.begin();
                keys.erase(it);
                values.erase(values.begin() + idx);
                return true;
            }
            return false;
        }

        void range_scan(const Key& start, const Key& end,
                      std::function<void(const Key&, const Value&)> func) const override {
            auto it = std::lower_bound(keys.begin(), keys.end(), start);
            while (it != keys.end() && *it <= end) {
                func(*it, values[it - keys.begin()]);
                ++it;
            }
            
            // Continue to next leaf if needed
            if (next && end >= next->keys.front()) {
                next->range_scan(start, end, func);
            }
        }
    };

    // Internal Node implementation
    class InternalNode : public Node {
    public:
        std::vector<Key> keys;
        std::vector<std::shared_ptr<Node>> children;

        bool is_leaf() const override { return false; }

        // Find the child index for a given key
        size_t find_child_index(const Key& key) const {
            auto it = std::upper_bound(keys.begin(), keys.end(), key);
            return it - keys.begin();
        }

        std::optional<InsertResult> insert(const Key& key, const Value& value) override {
            auto idx = find_child_index(key);
            auto result = children[idx]->insert(key, value);
            
            if (!result.has_value()) {
                return std::nullopt;
            }
            
            // Insert the new key and child
            keys.insert(keys.begin() + idx, result->key);
            children.insert(children.begin() + idx + 1, result->node);
            
            // Check if we need to split the node
            if (keys.size() > Order) {
                auto new_internal = std::make_shared<InternalNode>();
                int mid = keys.size() / 2;
                
                // Get the middle key that will be moved up
                Key middle_key = keys[mid];
                
                // Copy the second half to the new internal node (excluding the middle key)
                new_internal->keys.assign(keys.begin() + mid + 1, keys.end());
                new_internal->children.assign(children.begin() + mid + 1, children.end());
                
                // Update this node to contain only the first half
                keys.resize(mid);
                children.resize(mid + 1);
                
                // Return the split information
                InsertResult result;
                result.key = middle_key;
                result.node = new_internal;
                return result;
            }
            
            return std::nullopt;
        }

        std::optional<Value> find(const Key& key) const override {
            return children[find_child_index(key)]->find(key);
        }

        bool update(const Key& key, const Value& value) override {
            return children[find_child_index(key)]->update(key, value);
        }

        bool remove(const Key& key) override {
            auto idx = find_child_index(key);
            return children[idx]->remove(key);
            // Note: This simplified implementation doesn't handle node merging
        }

        void range_scan(const Key& start, const Key& end,
                      std::function<void(const Key&, const Value&)> func) const override {
            auto idx = find_child_index(start);
            children[idx]->range_scan(start, end, func);
        }
    };

    std::shared_ptr<Node> root_;
};

} // namespace storage
} // namespace toydb 