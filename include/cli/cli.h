#pragma once

#include <string>
#include <memory>
#include <vector>
#include "../db/database.h"
#include "../parser/parser.h"

namespace toydb {
namespace cli {

// Class to manage the command-line interface
class CLI {
public:
    CLI();
    
    // Start the CLI
    void start();
    
    // Execute a single SQL command
    void execute_command(const std::string& command);
    
    // Print the result of a SELECT query
    void print_results(const std::vector<db::Row>& rows, const std::vector<db::ColumnDef>& columns);

private:
    std::shared_ptr<db::Database> db_;
    parser::Parser parser_;
    
    // Handle specific statement types
    void handle_create_table(const parser::CreateTableStmt& stmt);
    void handle_insert(const parser::InsertStmt& stmt);
    void handle_select(const parser::SelectStmt& stmt);
    void handle_update(const parser::UpdateStmt& stmt);
    void handle_delete(const parser::DeleteStmt& stmt);
    void handle_drop_table(const parser::DropTableStmt& stmt);
    void handle_show_tables(const parser::ShowTablesStmt& stmt);
    void handle_begin_transaction(const parser::BeginTransactionStmt& stmt);
    void handle_commit_transaction(const parser::CommitTransactionStmt& stmt);
    void handle_abort_transaction(const parser::AbortTransactionStmt& stmt);
    
    // Print help/usage information
    void print_help() const;
    
    // Parse a row of values for INSERT
    db::Row parse_row(const std::vector<std::string>& value_strs, 
                      const std::vector<db::ColumnDef>& columns, 
                      const std::vector<std::string>& col_names = {});
};

} // namespace cli
} // namespace toydb 