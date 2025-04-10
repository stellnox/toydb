#include "../../include/parser/parser.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <regex>

namespace toydb {
namespace parser {

// Helper function to convert a string to uppercase
std::string to_upper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

// Helper function to trim whitespace from start and end of string
std::string trim(const std::string& str) {
    const auto start = str.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    
    const auto end = str.find_last_not_of(" \t\r\n");
    return str.substr(start, end - start + 1);
}

// Parse a SQL statement
std::optional<Statement> Parser::parse(const std::string& sql) {
    error_.clear();
    
    // Tokenize the SQL statement
    auto tokens = tokenize(sql);
    if (tokens.empty()) {
        error_ = "Empty SQL statement";
        return std::nullopt;
    }
    
    // Convert first token to uppercase for comparison
    const std::string cmd = to_upper(tokens[0]);
    
    if (cmd == "CREATE") {
        if (tokens.size() > 1 && to_upper(tokens[1]) == "TABLE") {
            return parse_create_table(tokens);
        } else if (tokens.size() > 1 && to_upper(tokens[1]) == "INDEX") {
            return parse_create_index();
        }
    } else if (cmd == "INSERT") {
        return parse_insert(tokens);
    } else if (cmd == "SELECT") {
        return parse_select(tokens);
    } else if (cmd == "UPDATE") {
        return parse_update(tokens);
    } else if (cmd == "DELETE") {
        return parse_delete(tokens);
    } else if (cmd == "DROP") {
        if (tokens.size() > 1 && to_upper(tokens[1]) == "TABLE") {
            return parse_drop_table(tokens);
        } else if (tokens.size() > 1 && to_upper(tokens[1]) == "INDEX") {
            return parse_drop_index();
        }
    } else if (cmd == "SHOW") {
        if (tokens.size() > 1 && to_upper(tokens[1]) == "TABLES") {
            return parse_show_tables(tokens);
        }
    } else if (cmd == "ALTER") {
        if (tokens.size() > 1 && to_upper(tokens[1]) == "TABLE") {
            return parse_alter_table();
        }
    } else if (cmd == "BEGIN") {
        if (tokens.size() > 1 && to_upper(tokens[1]) == "TRANSACTION") {
            return parse_begin_transaction(tokens);
        }
    } else if (cmd == "COMMIT") {
        if (tokens.size() > 1 && to_upper(tokens[1]) == "TRANSACTION") {
            return parse_commit_transaction(tokens);
        }
    } else if (cmd == "ROLLBACK" || cmd == "ABORT") {
        if (tokens.size() > 1 && to_upper(tokens[1]) == "TRANSACTION") {
            return parse_abort_transaction(tokens);
        }
    }
    
    error_ = "Unknown SQL command: " + cmd;
    return std::nullopt;
}

// Tokenize SQL string into individual tokens
std::vector<std::string> Parser::tokenize(const std::string& sql) {
    std::vector<std::string> tokens;
    std::string current_token;
    bool in_quotes = false;
    char quote_char = '\0';
    
    for (size_t i = 0; i < sql.length(); ++i) {
        char c = sql[i];
        
        if (c == '\'' || c == '\"') {
            if (!in_quotes) {
                // Start of quoted string
                if (!current_token.empty()) {
                    tokens.push_back(current_token);
                    current_token.clear();
                }
                in_quotes = true;
                quote_char = c;
                current_token += c;
            } else if (c == quote_char) {
                // End of quoted string
                current_token += c;
                tokens.push_back(current_token);
                current_token.clear();
                in_quotes = false;
            } else {
                // Quote character inside another type of quotes
                current_token += c;
            }
        } else if (in_quotes) {
            // Inside quotes, add character as is
            current_token += c;
        } else if (std::isspace(c)) {
            // Whitespace, end current token
            if (!current_token.empty()) {
                tokens.push_back(current_token);
                current_token.clear();
            }
        } else if (c == ',' || c == '(' || c == ')' || c == ';') {
            // Special characters that are tokens on their own
            if (!current_token.empty()) {
                tokens.push_back(current_token);
                current_token.clear();
            }
            tokens.push_back(std::string(1, c));
        } else if (c == '=' || c == '<' || c == '>' || c == '!') {
            // Operators
            if (!current_token.empty()) {
                tokens.push_back(current_token);
                current_token.clear();
            }
            
            // Check for two-character operators
            if (i + 1 < sql.length() && (sql[i + 1] == '=' || (c == '<' && sql[i + 1] == '>'))) {
                tokens.push_back(sql.substr(i, 2));
                i++; // Skip next character
            } else {
                tokens.push_back(std::string(1, c));
            }
        } else {
            // Normal character, add to current token
            current_token += c;
        }
    }
    
    // Add the last token if there is one
    if (!current_token.empty()) {
        tokens.push_back(current_token);
    }
    
    return tokens;
}

// Parse CREATE TABLE statement
std::optional<CreateTableStmt> Parser::parse_create_table(std::vector<std::string>& tokens) {
    // Ensure we have enough tokens
    if (tokens.size() < 4) {
        error_ = "Invalid CREATE TABLE syntax";
        return std::nullopt;
    }
    
    // Skip "CREATE TABLE" part
    tokens.erase(tokens.begin(), tokens.begin() + 2);
    
    // Get table name
    std::string table_name = tokens[0];
    tokens.erase(tokens.begin());
    
    // Expect opening parenthesis
    if (tokens.empty() || tokens[0] != "(") {
        error_ = "Expected '(' after table name";
        return std::nullopt;
    }
    tokens.erase(tokens.begin());
    
    // Parse column definitions
    std::vector<ColumnDefinition> columns;
    bool parsing_columns = true;
    
    while (parsing_columns && !tokens.empty()) {
        // End of column definitions
        if (tokens[0] == ")") {
            tokens.erase(tokens.begin());
            parsing_columns = false;
            break;
        }
        
        // Parse column name
        if (tokens.empty()) {
            error_ = "Unexpected end of input while parsing column name";
            return std::nullopt;
        }
        
        std::string col_name = tokens[0];
        tokens.erase(tokens.begin());
        
        // Parse column type
        if (tokens.empty()) {
            error_ = "Unexpected end of input while parsing column type";
            return std::nullopt;
        }
        
        std::string col_type = to_upper(tokens[0]);
        tokens.erase(tokens.begin());
        
        // Initialize column definition
        ColumnDefinition col_def;
        col_def.name = col_name;
        col_def.type = col_type;
        
        // Parse column constraints
        while (!tokens.empty() && tokens[0] != "," && tokens[0] != ")") {
            std::string constraint = to_upper(tokens[0]);
            tokens.erase(tokens.begin());
            
            if (constraint == "PRIMARY" && !tokens.empty() && to_upper(tokens[0]) == "KEY") {
                col_def.primary_key = true;
                tokens.erase(tokens.begin());
            } else if (constraint == "NOT" && !tokens.empty() && to_upper(tokens[0]) == "NULL") {
                col_def.not_null = true;
                tokens.erase(tokens.begin());
            } else {
                error_ = "Unknown column constraint: " + constraint;
                return std::nullopt;
            }
        }
        
        columns.push_back(col_def);
        
        // Check for comma or end of column list
        if (!tokens.empty() && tokens[0] == ",") {
            tokens.erase(tokens.begin());
        } else if (!tokens.empty() && tokens[0] != ")") {
            error_ = "Expected ',' or ')' after column definition";
            return std::nullopt;
        }
    }
    
    // Check for semicolon at the end
    if (!tokens.empty() && tokens[0] == ";") {
        tokens.erase(tokens.begin());
    }
    
    if (columns.empty()) {
        error_ = "No columns defined in CREATE TABLE statement";
        return std::nullopt;
    }
    
    CreateTableStmt stmt;
    stmt.table_name = table_name;
    stmt.columns = columns;
    return stmt;
}

// Parse INSERT statement
std::optional<InsertStmt> Parser::parse_insert(std::vector<std::string>& tokens) {
    // Ensure we have enough tokens
    if (tokens.size() < 4) {
        error_ = "Invalid INSERT syntax";
        return std::nullopt;
    }
    
    // Skip "INSERT" part
    tokens.erase(tokens.begin());
    
    // Check for "INTO"
    if (tokens.empty() || to_upper(tokens[0]) != "INTO") {
        error_ = "Expected 'INTO' after INSERT";
        return std::nullopt;
    }
    tokens.erase(tokens.begin());
    
    // Get table name
    if (tokens.empty()) {
        error_ = "Expected table name after INTO";
        return std::nullopt;
    }
    
    std::string table_name = tokens[0];
    tokens.erase(tokens.begin());
    
    InsertStmt stmt;
    stmt.table_name = table_name;
    
    // Check if column names are specified
    if (!tokens.empty() && tokens[0] == "(") {
        tokens.erase(tokens.begin());
        
        // Parse column names
        while (!tokens.empty() && tokens[0] != ")") {
            stmt.columns.push_back(tokens[0]);
            tokens.erase(tokens.begin());
            
            if (!tokens.empty() && tokens[0] == ",") {
                tokens.erase(tokens.begin());
            } else if (!tokens.empty() && tokens[0] != ")") {
                error_ = "Expected ',' or ')' after column name";
                return std::nullopt;
            }
        }
        
        if (tokens.empty() || tokens[0] != ")") {
            error_ = "Expected ')' after column names";
            return std::nullopt;
        }
        tokens.erase(tokens.begin());
    }
    
    // Check for VALUES keyword
    if (tokens.empty() || to_upper(tokens[0]) != "VALUES") {
        error_ = "Expected 'VALUES' keyword";
        return std::nullopt;
    }
    tokens.erase(tokens.begin());
    
    // Parse values
    while (!tokens.empty() && tokens[0] == "(") {
        tokens.erase(tokens.begin());
        
        std::vector<std::string> row_values;
        
        // Parse value list
        while (!tokens.empty() && tokens[0] != ")") {
            row_values.push_back(tokens[0]);
            tokens.erase(tokens.begin());
            
            if (!tokens.empty() && tokens[0] == ",") {
                tokens.erase(tokens.begin());
            } else if (!tokens.empty() && tokens[0] != ")") {
                error_ = "Expected ',' or ')' after value";
                return std::nullopt;
            }
        }
        
        if (tokens.empty() || tokens[0] != ")") {
            error_ = "Expected ')' after values";
            return std::nullopt;
        }
        tokens.erase(tokens.begin());
        
        stmt.values.push_back(row_values);
        
        // Check for more rows
        if (!tokens.empty() && tokens[0] == ",") {
            tokens.erase(tokens.begin());
        } else {
            break;
        }
    }
    
    // Check for semicolon
    if (!tokens.empty() && tokens[0] == ";") {
        tokens.erase(tokens.begin());
    }
    
    return stmt;
}

// Parse WHERE conditions
std::vector<Condition> Parser::parse_conditions(std::vector<std::string>& tokens) {
    std::vector<Condition> conditions;
    
    if (tokens.empty()) {
        return conditions;
    }
    
    // Check for WHERE keyword
    if (to_upper(tokens[0]) != "WHERE") {
        return conditions;
    }
    tokens.erase(tokens.begin());
    
    while (!tokens.empty()) {
        if (tokens.size() < 3) {
            error_ = "Invalid WHERE clause syntax";
            return {};
        }
        
        Condition cond;
        cond.column = tokens[0];
        tokens.erase(tokens.begin());
        
        cond.op = tokens[0];
        tokens.erase(tokens.begin());
        
        cond.value = tokens[0];
        tokens.erase(tokens.begin());
        
        conditions.push_back(cond);
        
        // Check for AND to continue conditions
        if (!tokens.empty() && to_upper(tokens[0]) == "AND") {
            tokens.erase(tokens.begin());
        } else {
            break;
        }
    }
    
    return conditions;
}

// Parse SELECT statement
std::optional<SelectStmt> Parser::parse_select(std::vector<std::string>& tokens) {
    // Ensure we have enough tokens
    if (tokens.size() < 4) {
        error_ = "Invalid SELECT syntax";
        return std::nullopt;
    }
    
    // Skip "SELECT" part
    tokens.erase(tokens.begin());
    
    SelectStmt stmt;
    
    // Parse column list
    while (!tokens.empty() && to_upper(tokens[0]) != "FROM") {
        if (tokens[0] == "*") {
            // SELECT * - empty columns vector indicates all columns
            tokens.erase(tokens.begin());
            break;
        }
        
        stmt.columns.push_back(tokens[0]);
        tokens.erase(tokens.begin());
        
        if (!tokens.empty() && tokens[0] == ",") {
            tokens.erase(tokens.begin());
        } else {
            break;
        }
    }
    
    // Check for FROM keyword
    if (tokens.empty() || to_upper(tokens[0]) != "FROM") {
        error_ = "Expected FROM in SELECT statement";
        return std::nullopt;
    }
    tokens.erase(tokens.begin());
    
    // Get table name
    if (tokens.empty()) {
        error_ = "Expected table name after FROM";
        return std::nullopt;
    }
    
    stmt.table_name = tokens[0];
    tokens.erase(tokens.begin());
    
    // Parse WHERE conditions if present
    if (!tokens.empty() && to_upper(tokens[0]) == "WHERE") {
        stmt.conditions = parse_conditions(tokens);
    }
    
    // Check for semicolon
    if (!tokens.empty() && tokens[0] == ";") {
        tokens.erase(tokens.begin());
    }
    
    return stmt;
}

// Parse UPDATE statement
std::optional<UpdateStmt> Parser::parse_update(std::vector<std::string>& tokens) {
    // Ensure we have enough tokens
    if (tokens.size() < 5) {
        error_ = "Invalid UPDATE syntax";
        return std::nullopt;
    }
    
    // Skip "UPDATE" part
    tokens.erase(tokens.begin());
    
    // Get table name
    if (tokens.empty()) {
        error_ = "Expected table name after UPDATE";
        return std::nullopt;
    }
    
    UpdateStmt stmt;
    stmt.table_name = tokens[0];
    tokens.erase(tokens.begin());
    
    // Check for SET keyword
    if (tokens.empty() || to_upper(tokens[0]) != "SET") {
        error_ = "Expected SET in UPDATE statement";
        return std::nullopt;
    }
    tokens.erase(tokens.begin());
    
    // Parse update assignments
    while (!tokens.empty() && to_upper(tokens[0]) != "WHERE") {
        if (tokens.size() < 3) {
            error_ = "Invalid SET clause in UPDATE statement";
            return std::nullopt;
        }
        
        std::string column = tokens[0];
        tokens.erase(tokens.begin());
        
        if (tokens.empty() || tokens[0] != "=") {
            error_ = "Expected '=' after column name in SET clause";
            return std::nullopt;
        }
        tokens.erase(tokens.begin());
        
        std::string value = tokens[0];
        tokens.erase(tokens.begin());
        
        stmt.updates.emplace_back(column, value);
        
        // Check for comma to continue updates
        if (!tokens.empty() && tokens[0] == ",") {
            tokens.erase(tokens.begin());
        } else {
            break;
        }
    }
    
    // Parse WHERE conditions if present
    if (!tokens.empty() && to_upper(tokens[0]) == "WHERE") {
        stmt.conditions = parse_conditions(tokens);
    }
    
    // Check for semicolon
    if (!tokens.empty() && tokens[0] == ";") {
        tokens.erase(tokens.begin());
    }
    
    return stmt;
}

// Parse DELETE statement
std::optional<DeleteStmt> Parser::parse_delete(std::vector<std::string>& tokens) {
    // Ensure we have enough tokens
    if (tokens.size() < 4) {
        error_ = "Invalid DELETE syntax";
        return std::nullopt;
    }
    
    // Skip "DELETE" part
    tokens.erase(tokens.begin());
    
    // Check for FROM keyword
    if (tokens.empty() || to_upper(tokens[0]) != "FROM") {
        error_ = "Expected FROM in DELETE statement";
        return std::nullopt;
    }
    tokens.erase(tokens.begin());
    
    // Get table name
    if (tokens.empty()) {
        error_ = "Expected table name after FROM";
        return std::nullopt;
    }
    
    DeleteStmt stmt;
    stmt.table_name = tokens[0];
    tokens.erase(tokens.begin());
    
    // Parse WHERE conditions if present
    if (!tokens.empty() && to_upper(tokens[0]) == "WHERE") {
        stmt.conditions = parse_conditions(tokens);
    }
    
    // Check for semicolon
    if (!tokens.empty() && tokens[0] == ";") {
        tokens.erase(tokens.begin());
    }
    
    return stmt;
}

// Parse DROP TABLE statement
std::optional<DropTableStmt> Parser::parse_drop_table(std::vector<std::string>& tokens) {
    // Ensure we have enough tokens
    if (tokens.size() < 3) {
        error_ = "Invalid DROP TABLE syntax";
        return std::nullopt;
    }
    
    // Skip "DROP TABLE" part
    tokens.erase(tokens.begin(), tokens.begin() + 2);
    
    // Get table name
    if (tokens.empty()) {
        error_ = "Expected table name after DROP TABLE";
        return std::nullopt;
    }
    
    DropTableStmt stmt;
    stmt.table_name = tokens[0];
    tokens.erase(tokens.begin());
    
    // Check for semicolon
    if (!tokens.empty() && tokens[0] == ";") {
        tokens.erase(tokens.begin());
    }
    
    return stmt;
}

// Parse SHOW TABLES statement
std::optional<ShowTablesStmt> Parser::parse_show_tables(std::vector<std::string>& tokens) {
    // Ensure we have enough tokens
    if (tokens.size() < 2) {
        error_ = "Invalid SHOW TABLES syntax";
        return std::nullopt;
    }
    
    // Skip "SHOW TABLES" part
    tokens.erase(tokens.begin(), tokens.begin() + 2);
    
    // Check for semicolon
    if (!tokens.empty() && tokens[0] == ";") {
        tokens.erase(tokens.begin());
    }
    
    return ShowTablesStmt{};
}

// Convert string type to ColumnType enum
db::ColumnType string_to_column_type(const std::string& type_str) {
    std::string upper_type = to_upper(type_str);
    
    if (upper_type == "INT" || upper_type == "INTEGER") {
        return db::ColumnType::Int;
    } else if (upper_type == "FLOAT" || upper_type == "REAL") {
        return db::ColumnType::Float;
    } else if (upper_type == "TEXT" || upper_type == "VARCHAR" || upper_type == "CHAR") {
        return db::ColumnType::Text;
    } else {
        // Default to NULL type for unrecognized types
        return db::ColumnType::Null;
    }
}

// Convert parser column definition to DB column definition
db::ColumnDef convert_column_def(const ColumnDefinition& col_def) {
    db::ColumnDef db_col;
    db_col.name = col_def.name;
    db_col.type = string_to_column_type(col_def.type);
    db_col.primary_key = col_def.primary_key;
    db_col.not_null = col_def.not_null;
    return db_col;
}

// Parse a string value to DBValue
db::DBValue parse_value(const std::string& value_str, db::ColumnType expected_type) {
    // Check for NULL
    if (to_upper(value_str) == "NULL") {
        return db::DBNull{};
    }
    
    // Handle quoted strings
    if ((value_str.front() == '\'' && value_str.back() == '\'') ||
        (value_str.front() == '\"' && value_str.back() == '\"')) {
        return value_str.substr(1, value_str.length() - 2);
    }
    
    // Parse based on expected type
    switch (expected_type) {
        case db::ColumnType::Int: {
            try {
                return static_cast<db::DBInt>(std::stoll(value_str));
            } catch (...) {
                // If parse fails, fallback to TEXT
                return value_str;
            }
        }
        case db::ColumnType::Float: {
            try {
                return static_cast<db::DBFloat>(std::stod(value_str));
            } catch (...) {
                // If parse fails, fallback to TEXT
                return value_str;
            }
        }
        case db::ColumnType::Text:
            return value_str;
        default:
            return db::DBNull{};
    }
}

// Convert parser condition to DB condition
db::Condition convert_condition(const Condition& cond, const std::vector<db::ColumnDef>& columns) {
    db::Condition db_cond;
    db_cond.column_name = cond.column;
    db_cond.op = cond.op;
    
    // Find the column type
    db::ColumnType col_type = db::ColumnType::Text; // Default
    for (const auto& col : columns) {
        if (col.name == cond.column) {
            col_type = col.type;
            break;
        }
    }
    
    db_cond.value = parse_value(cond.value, col_type);
    return db_cond;
}

std::unique_ptr<Statement> Parser::parse_alter_table() {
    auto stmt = std::make_unique<AlterTableStmt>();
    
    // Parse table name
    stmt->table_name = parse_identifier();
    
    // Parse ALTER command
    std::string command = parse_keyword();
    if (command == "ADD") {
        stmt->command = AlterTableStmt::Command::ADD;
        if (parse_keyword() == "COLUMN") {
            stmt->column_def = parse_column_def();
        } else {
            throw std::runtime_error("Expected COLUMN keyword after ADD");
        }
    } else if (command == "DROP") {
        stmt->command = AlterTableStmt::Command::DROP;
        if (parse_keyword() == "COLUMN") {
            stmt->column_name = parse_identifier();
        } else {
            throw std::runtime_error("Expected COLUMN keyword after DROP");
        }
    } else if (command == "RENAME") {
        stmt->command = AlterTableStmt::Command::RENAME;
        if (parse_keyword() == "TO") {
            stmt->new_name = parse_identifier();
        } else {
            throw std::runtime_error("Expected TO keyword after RENAME");
        }
    } else {
        throw std::runtime_error("Invalid ALTER TABLE command");
    }
    
    return stmt;
}

std::unique_ptr<Statement> Parser::parse_create_index() {
    auto stmt = std::make_unique<CreateIndexStmt>();
    
    // Parse index name
    stmt->index_name = parse_identifier();
    
    // Parse ON keyword
    if (parse_keyword() != "ON") {
        throw std::runtime_error("Expected ON keyword after index name");
    }
    
    // Parse table name
    stmt->table_name = parse_identifier();
    
    // Parse column list
    if (parse_keyword() != "(") {
        throw std::runtime_error("Expected ( after table name");
    }
    
    do {
        stmt->columns.push_back(parse_identifier());
    } while (parse_keyword() == ",");
    
    return stmt;
}

std::unique_ptr<Statement> Parser::parse_drop_index() {
    auto stmt = std::make_unique<DropIndexStmt>();
    
    // Parse index name
    stmt->index_name = parse_identifier();
    
    // Parse ON keyword
    if (parse_keyword() != "ON") {
        throw std::runtime_error("Expected ON keyword after index name");
    }
    
    // Parse table name
    stmt->table_name = parse_identifier();
    
    return stmt;
}

// Parse BEGIN TRANSACTION statement
std::optional<BeginTransactionStmt> Parser::parse_begin_transaction(std::vector<std::string>& tokens) {
    // Verify syntax: BEGIN TRANSACTION
    if (tokens.size() < 2 || to_upper(tokens[0]) != "BEGIN" || to_upper(tokens[1]) != "TRANSACTION") {
        error_ = "Invalid BEGIN TRANSACTION syntax";
        return std::nullopt;
    }
    
    // Return a simple begin transaction statement
    return BeginTransactionStmt{};
}

// Parse COMMIT TRANSACTION statement
std::optional<CommitTransactionStmt> Parser::parse_commit_transaction(std::vector<std::string>& tokens) {
    // Verify syntax: COMMIT TRANSACTION <transaction_id>
    if (tokens.size() < 3 || to_upper(tokens[0]) != "COMMIT" || to_upper(tokens[1]) != "TRANSACTION") {
        error_ = "Invalid COMMIT TRANSACTION syntax";
        return std::nullopt;
    }
    
    // Parse transaction ID
    try {
        uint64_t transaction_id = std::stoull(tokens[2]);
        return CommitTransactionStmt{transaction_id};
    } catch (const std::exception& e) {
        error_ = "Invalid transaction ID: " + tokens[2];
        return std::nullopt;
    }
}

// Parse ABORT/ROLLBACK TRANSACTION statement
std::optional<AbortTransactionStmt> Parser::parse_abort_transaction(std::vector<std::string>& tokens) {
    // Verify syntax: ABORT/ROLLBACK TRANSACTION <transaction_id>
    if (tokens.size() < 3 || 
        (to_upper(tokens[0]) != "ABORT" && to_upper(tokens[0]) != "ROLLBACK") || 
        to_upper(tokens[1]) != "TRANSACTION") {
        error_ = "Invalid ABORT/ROLLBACK TRANSACTION syntax";
        return std::nullopt;
    }
    
    // Parse transaction ID
    try {
        uint64_t transaction_id = std::stoull(tokens[2]);
        return AbortTransactionStmt{transaction_id};
    } catch (const std::exception& e) {
        error_ = "Invalid transaction ID: " + tokens[2];
        return std::nullopt;
    }
}

} // namespace parser
} // namespace toydb 