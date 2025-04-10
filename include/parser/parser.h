#pragma once

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <variant>
#include "../db/table.h"

namespace toydb {
namespace parser {

// Forward declarations
struct CreateTableStmt;
struct InsertStmt;
struct SelectStmt;
struct UpdateStmt;
struct DeleteStmt;
struct DropTableStmt;
struct ShowTablesStmt;
struct BeginTransactionStmt;
struct CommitTransactionStmt;
struct AbortTransactionStmt;

// Statement is a variant of all possible statement types
using Statement = std::variant<
    CreateTableStmt,
    InsertStmt,
    SelectStmt,
    UpdateStmt,
    DeleteStmt,
    DropTableStmt,
    ShowTablesStmt,
    BeginTransactionStmt,
    CommitTransactionStmt,
    AbortTransactionStmt
>;

// Column definition for CREATE TABLE
struct ColumnDefinition {
    std::string name;
    std::string type;
    bool primary_key = false;
    bool not_null = false;
};

// CREATE TABLE statement
struct CreateTableStmt {
    std::string table_name;
    std::vector<ColumnDefinition> columns;
};

// INSERT statement
struct InsertStmt {
    std::string table_name;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> values; // For multi-row inserts
};

// WHERE condition
struct Condition {
    std::string column;
    std::string op; // =, >, <, >=, <=, !=
    std::string value;
};

// SELECT statement
struct SelectStmt {
    std::vector<std::string> columns; // * is represented as empty vector
    std::string table_name;
    std::vector<Condition> conditions;
};

// UPDATE statement
struct UpdateStmt {
    std::string table_name;
    std::vector<std::pair<std::string, std::string>> updates; // col = value
    std::vector<Condition> conditions;
};

// DELETE statement
struct DeleteStmt {
    std::string table_name;
    std::vector<Condition> conditions;
};

// DROP TABLE statement
struct DropTableStmt {
    std::string table_name;
};

// SHOW TABLES statement
struct ShowTablesStmt {
    // No additional fields needed
};

// BEGIN TRANSACTION statement
struct BeginTransactionStmt {
    // No additional fields needed
};

// COMMIT TRANSACTION statement
struct CommitTransactionStmt {
    uint64_t transaction_id;
};

// ABORT/ROLLBACK TRANSACTION statement
struct AbortTransactionStmt {
    uint64_t transaction_id;
};

// Parser class
class Parser {
public:
    Parser() = default;
    
    // Parse a SQL statement
    std::optional<Statement> parse(const std::string& sql);
    
    // Parse errors
    std::string last_error() const { return error_; }

private:
    // Helper functions for parsing specific statements
    std::optional<CreateTableStmt> parse_create_table(std::vector<std::string>& tokens);
    std::optional<InsertStmt> parse_insert(std::vector<std::string>& tokens);
    std::optional<SelectStmt> parse_select(std::vector<std::string>& tokens);
    std::optional<UpdateStmt> parse_update(std::vector<std::string>& tokens);
    std::optional<DeleteStmt> parse_delete(std::vector<std::string>& tokens);
    std::optional<DropTableStmt> parse_drop_table(std::vector<std::string>& tokens);
    std::optional<ShowTablesStmt> parse_show_tables(std::vector<std::string>& tokens);
    std::optional<BeginTransactionStmt> parse_begin_transaction(std::vector<std::string>& tokens);
    std::optional<CommitTransactionStmt> parse_commit_transaction(std::vector<std::string>& tokens);
    std::optional<AbortTransactionStmt> parse_abort_transaction(std::vector<std::string>& tokens);
    
    // Tokenize the SQL statement
    std::vector<std::string> tokenize(const std::string& sql);
    
    // Helper to parse WHERE conditions
    std::vector<Condition> parse_conditions(std::vector<std::string>& tokens);
    
    // Error handling
    std::string error_;
};

// Helper functions to convert from parser types to DB types
db::ColumnType string_to_column_type(const std::string& type_str);
db::ColumnDef convert_column_def(const ColumnDefinition& col_def);
db::DBValue parse_value(const std::string& value_str, db::ColumnType expected_type);
db::Condition convert_condition(const Condition& cond, const std::vector<db::ColumnDef>& columns);

} // namespace parser
} // namespace toydb 