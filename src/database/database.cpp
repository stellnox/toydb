void AlterTableStmt::execute(Database& db) {
    auto table_it = db.tables.find(table_name);
    if (table_it == db.tables.end()) {
        throw std::runtime_error("Table " + table_name + " does not exist");
    }
    
    Table& table = table_it->second;
    
    switch (command) {
        case Command::ADD: {
            // Check if column already exists
            if (table.schema.find(column_def.name) != table.schema.end()) {
                throw std::runtime_error("Column " + column_def.name + " already exists");
            }
            
            // Add new column with default value
            table.schema[column_def.name] = column_def;
            for (auto& row : table.rows) {
                row.values[column_def.name] = Value();  // Default value based on type
            }
            break;
        }
        case Command::DROP: {
            // Check if column exists
            if (table.schema.find(column_name) == table.schema.end()) {
                throw std::runtime_error("Column " + column_name + " does not exist");
            }
            
            // Remove column from schema and all rows
            table.schema.erase(column_name);
            for (auto& row : table.rows) {
                row.values.erase(column_name);
            }
            break;
        }
        case Command::RENAME: {
            // Check if new name already exists
            if (db.tables.find(new_name) != db.tables.end()) {
                throw std::runtime_error("Table " + new_name + " already exists");
            }
            
            // Rename table
            auto node = db.tables.extract(table_name);
            node.key() = new_name;
            db.tables.insert(std::move(node));
            break;
        }
    }
}

void CreateIndexStmt::execute(Database& db) {
    auto table_it = db.tables.find(table_name);
    if (table_it == db.tables.end()) {
        throw std::runtime_error("Table " + table_name + " does not exist");
    }
    
    Table& table = table_it->second;
    
    // Check if index already exists
    if (table.indexes.find(index_name) != table.indexes.end()) {
        throw std::runtime_error("Index " + index_name + " already exists");
    }
    
    // Validate columns exist
    for (const auto& col : columns) {
        if (table.schema.find(col) == table.schema.end()) {
            throw std::runtime_error("Column " + col + " does not exist");
        }
    }
    
    // Create B+ tree index
    auto index = std::make_unique<BPlusTree>();
    
    // Build index from existing data
    for (size_t i = 0; i < table.rows.size(); ++i) {
        const auto& row = table.rows[i];
        std::vector<Value> key;
        for (const auto& col : columns) {
            key.push_back(row.values.at(col));
        }
        index->insert(key, i);
    }
    
    // Store index
    table.indexes[index_name] = std::move(index);
}

void DropIndexStmt::execute(Database& db) {
    auto table_it = db.tables.find(table_name);
    if (table_it == db.tables.end()) {
        throw std::runtime_error("Table " + table_name + " does not exist");
    }
    
    Table& table = table_it->second;
    
    // Check if index exists
    if (table.indexes.find(index_name) == table.indexes.end()) {
        throw std::runtime_error("Index " + index_name + " does not exist");
    }
    
    // Remove index
    table.indexes.erase(index_name);
} 