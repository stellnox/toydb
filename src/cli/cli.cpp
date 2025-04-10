#include "../../include/cli/cli.h"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <variant>

namespace toydb {
namespace cli {

CLI::CLI() : db_(std::make_shared<db::Database>("toydb")) {
    std::cout << "ToyDB initialized. Type 'help' for usage information.\n";
}

void CLI::start() {
    std::string command;
    
    while (true) {
        std::cout << "toydb> ";
        
        // Get a line of input
        std::string line;
        if (!std::getline(std::cin, line)) {
            break; // EOF
        }
        
        // Append to command buffer
        command += line;
        
        // Check if command is complete
        bool is_complete = false;
        if (!command.empty()) {
            // Commands are complete if they end with a semicolon
            // or are special commands like "exit" or "help"
            std::string trimmed = command;
            trimmed.erase(0, trimmed.find_first_not_of(" \t\r\n"));
            
            if (trimmed == "exit" || trimmed == "quit") {
                std::cout << "Goodbye!\n";
                break;
            } else if (trimmed == "help") {
                print_help();
                command.clear();
                continue;
            }
            
            is_complete = !command.empty() && command.back() == ';';
        }
        
        if (is_complete) {
            execute_command(command);
            command.clear();
        } else {
            // Continue reading for multi-line commands
            command += " ";
        }
    }
}

void CLI::execute_command(const std::string& command) {
    auto statement = parser_.parse(command);
    
    if (!statement) {
        std::cerr << "Error: " << parser_.last_error() << std::endl;
        return;
    }
    
    try {
        std::visit([this](const auto& stmt) { 
            using T = std::decay_t<decltype(stmt)>;
            
            if constexpr (std::is_same_v<T, parser::CreateTableStmt>) {
                handle_create_table(stmt);
            } else if constexpr (std::is_same_v<T, parser::InsertStmt>) {
                handle_insert(stmt);
            } else if constexpr (std::is_same_v<T, parser::SelectStmt>) {
                handle_select(stmt);
            } else if constexpr (std::is_same_v<T, parser::UpdateStmt>) {
                handle_update(stmt);
            } else if constexpr (std::is_same_v<T, parser::DeleteStmt>) {
                handle_delete(stmt);
            } else if constexpr (std::is_same_v<T, parser::DropTableStmt>) {
                handle_drop_table(stmt);
            } else if constexpr (std::is_same_v<T, parser::ShowTablesStmt>) {
                handle_show_tables(stmt);
            } else if constexpr (std::is_same_v<T, parser::BeginTransactionStmt>) {
                handle_begin_transaction(stmt);
            } else if constexpr (std::is_same_v<T, parser::CommitTransactionStmt>) {
                handle_commit_transaction(stmt);
            } else if constexpr (std::is_same_v<T, parser::AbortTransactionStmt>) {
                handle_abort_transaction(stmt);
            }
        }, *statement);
    } catch (const std::exception& e) {
        std::cerr << "Error executing command: " << e.what() << std::endl;
    }
}

void CLI::handle_create_table(const parser::CreateTableStmt& stmt) {
    // Convert parser column definitions to DB column definitions
    std::vector<db::ColumnDef> columns;
    for (const auto& col : stmt.columns) {
        columns.push_back(parser::convert_column_def(col));
    }
    
    if (db_->create_table(stmt.table_name, columns)) {
        std::cout << "Table created: " << stmt.table_name << std::endl;
    }
}

void CLI::handle_insert(const parser::InsertStmt& stmt) {
    auto table = db_->get_table(stmt.table_name);
    if (!table) {
        std::cerr << "Table not found: " << stmt.table_name << std::endl;
        return;
    }
    
    const auto& columns = table->columns();
    
    // Process each row
    size_t success_count = 0;
    for (const auto& value_strs : stmt.values) {
        auto row = parse_row(value_strs, columns, stmt.columns);
        if (table->insert_row(row)) {
            success_count++;
        }
    }
    
    std::cout << success_count << " row(s) inserted." << std::endl;
}

db::Row CLI::parse_row(const std::vector<std::string>& value_strs, 
                       const std::vector<db::ColumnDef>& columns,
                       const std::vector<std::string>& col_names) {
    db::Row row;
    
    // If column names are specified, map values to the correct columns
    if (!col_names.empty()) {
        // Initialize all columns to NULL
        row.resize(columns.size(), db::DBNull{});
        
        if (value_strs.size() != col_names.size()) {
            throw std::runtime_error("Column count mismatch");
        }
        
        for (size_t i = 0; i < col_names.size(); ++i) {
            // Find column index
            auto col_idx = std::find_if(columns.begin(), columns.end(), 
                                      [&](const db::ColumnDef& col) { 
                                          return col.name == col_names[i]; 
                                      }) - columns.begin();
            
            if (col_idx >= columns.size()) {
                throw std::runtime_error("Column not found: " + col_names[i]);
            }
            
            row[col_idx] = parser::parse_value(value_strs[i], columns[col_idx].type);
        }
    } else {
        // Use values in order
        if (value_strs.size() != columns.size()) {
            throw std::runtime_error("Column count mismatch");
        }
        
        for (size_t i = 0; i < columns.size(); ++i) {
            row.push_back(parser::parse_value(value_strs[i], columns[i].type));
        }
    }
    
    return row;
}

void CLI::handle_select(const parser::SelectStmt& stmt) {
    auto table = db_->get_table(stmt.table_name);
    if (!table) {
        std::cerr << "Table not found: " << stmt.table_name << std::endl;
        return;
    }
    
    const auto& columns = table->columns();
    
    // Convert parser conditions to DB conditions
    std::vector<db::Condition> conditions;
    for (const auto& cond : stmt.conditions) {
        conditions.push_back(parser::convert_condition(cond, columns));
    }
    
    // Execute the query
    auto rows = table->select(conditions);
    
    // Print the results
    print_results(rows, columns);
}

void CLI::print_results(const std::vector<db::Row>& rows, const std::vector<db::ColumnDef>& columns) {
    if (columns.empty()) {
        std::cout << "No columns" << std::endl;
        return;
    }
    
    // Calculate column widths
    std::vector<size_t> widths(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        widths[i] = columns[i].name.length();
    }
    
    for (const auto& row : rows) {
        for (size_t i = 0; i < columns.size() && i < row.size(); ++i) {
            std::string value_str = db::value_to_string(row[i]);
            widths[i] = std::max(widths[i], value_str.length());
        }
    }
    
    // Print header
    for (size_t i = 0; i < columns.size(); ++i) {
        std::cout << "| " << std::left << std::setw(widths[i]) << columns[i].name << " ";
    }
    std::cout << "|" << std::endl;
    
    // Print separator
    for (size_t i = 0; i < columns.size(); ++i) {
        std::cout << "+-" << std::string(widths[i], '-') << "-";
    }
    std::cout << "+" << std::endl;
    
    // Print rows
    for (const auto& row : rows) {
        for (size_t i = 0; i < columns.size(); ++i) {
            std::string value_str = i < row.size() ? db::value_to_string(row[i]) : "NULL";
            std::cout << "| " << std::left << std::setw(widths[i]) << value_str << " ";
        }
        std::cout << "|" << std::endl;
    }
    
    std::cout << rows.size() << " row(s) returned." << std::endl;
}

void CLI::handle_update(const parser::UpdateStmt& stmt) {
    auto table = db_->get_table(stmt.table_name);
    if (!table) {
        std::cerr << "Table not found: " << stmt.table_name << std::endl;
        return;
    }
    
    const auto& columns = table->columns();
    
    // Convert parser conditions to DB conditions
    std::vector<db::Condition> conditions;
    for (const auto& cond : stmt.conditions) {
        conditions.push_back(parser::convert_condition(cond, columns));
    }
    
    // Convert update assignments
    std::unordered_map<std::string, db::DBValue> updates;
    for (const auto& [col_name, value_str] : stmt.updates) {
        // Find column type
        db::ColumnType col_type = db::ColumnType::Text;
        for (const auto& col : columns) {
            if (col.name == col_name) {
                col_type = col.type;
                break;
            }
        }
        
        updates[col_name] = parser::parse_value(value_str, col_type);
    }
    
    // Execute the update
    size_t count = table->update(updates, conditions);
    std::cout << count << " row(s) updated." << std::endl;
}

void CLI::handle_delete(const parser::DeleteStmt& stmt) {
    auto table = db_->get_table(stmt.table_name);
    if (!table) {
        std::cerr << "Table not found: " << stmt.table_name << std::endl;
        return;
    }
    
    const auto& columns = table->columns();
    
    // Convert parser conditions to DB conditions
    std::vector<db::Condition> conditions;
    for (const auto& cond : stmt.conditions) {
        conditions.push_back(parser::convert_condition(cond, columns));
    }
    
    // Execute the delete
    size_t count = table->remove(conditions);
    std::cout << count << " row(s) deleted." << std::endl;
}

void CLI::handle_drop_table(const parser::DropTableStmt& stmt) {
    if (db_->drop_table(stmt.table_name)) {
        std::cout << "Table dropped: " << stmt.table_name << std::endl;
    }
}

void CLI::handle_show_tables(const parser::ShowTablesStmt& stmt) {
    auto table_names = db_->list_tables();
    
    if (table_names.empty()) {
        std::cout << "No tables found." << std::endl;
        return;
    }
    
    // Find the maximum table name length for formatting
    size_t max_width = 0;
    for (const auto& name : table_names) {
        max_width = std::max(max_width, name.length());
    }
    max_width = std::max(max_width, std::string("TABLE_NAME").length());
    
    // Print header
    std::cout << "| " << std::left << std::setw(max_width) << "TABLE_NAME" << " |" << std::endl;
    std::cout << "+-" << std::string(max_width, '-') << "-+" << std::endl;
    
    // Print table names
    for (const auto& name : table_names) {
        std::cout << "| " << std::left << std::setw(max_width) << name << " |" << std::endl;
    }
    
    std::cout << table_names.size() << " table(s) found." << std::endl;
}

void CLI::handle_begin_transaction(const parser::BeginTransactionStmt& stmt) {
    uint64_t transaction_id = db_->begin_transaction();
    std::cout << "Transaction started with ID: " << transaction_id << std::endl;
}

void CLI::handle_commit_transaction(const parser::CommitTransactionStmt& stmt) {
    try {
        db_->commit_transaction(stmt.transaction_id);
        std::cout << "Transaction " << stmt.transaction_id << " committed successfully." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error committing transaction: " << e.what() << std::endl;
    }
}

void CLI::handle_abort_transaction(const parser::AbortTransactionStmt& stmt) {
    try {
        db_->abort_transaction(stmt.transaction_id);
        std::cout << "Transaction " << stmt.transaction_id << " aborted successfully." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error aborting transaction: " << e.what() << std::endl;
    }
}

void CLI::print_help() const {
    std::cout << "ToyDB Help:\n"
              << "----------\n"
              << "Commands end with ';' and are case-insensitive.\n\n"
              << "CREATE TABLE table_name (col1 TYPE [PRIMARY KEY] [NOT NULL], ...);\n"
              << "  - Create a new table with specified columns\n"
              << "  - Supported types: INT, FLOAT, TEXT\n\n"
              << "INSERT INTO table_name [(col1, col2, ...)] VALUES (val1, val2, ...), ...;\n"
              << "  - Insert one or more rows\n\n"
              << "SELECT col1, col2, ... FROM table_name [WHERE conditions];\n"
              << "  - Query data (use * for all columns)\n\n"
              << "UPDATE table_name SET col1 = val1, ... [WHERE conditions];\n"
              << "  - Update rows matching conditions\n\n"
              << "DELETE FROM table_name [WHERE conditions];\n"
              << "  - Delete rows matching conditions\n\n"
              << "DROP TABLE table_name;\n"
              << "  - Remove a table\n\n"
              << "SHOW TABLES;\n"
              << "  - List all tables in the database\n\n"
              << "Transaction commands:\n"
              << "BEGIN TRANSACTION;\n"
              << "  - Start a new transaction and get a transaction ID\n\n"
              << "COMMIT TRANSACTION transaction_id;\n"
              << "  - Commit a transaction by ID\n\n"
              << "ABORT TRANSACTION transaction_id; (or ROLLBACK TRANSACTION transaction_id;)\n"
              << "  - Abort/rollback a transaction by ID\n\n"
              << "Special commands (without semicolon):\n"
              << "  help - Display this help\n"
              << "  exit/quit - Exit ToyDB\n";
}

} // namespace cli
} // namespace toydb 