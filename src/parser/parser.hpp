class AlterTableStmt : public Statement {
public:
    enum class Command {
        ADD,
        DROP,
        RENAME
    };
    
    std::string table_name;
    Command command;
    std::string column_name;
    std::string new_name;
    ColumnDef column_def;
    
    void execute(Database& db) override;
};

class CreateIndexStmt : public Statement {
public:
    std::string index_name;
    std::string table_name;
    std::vector<std::string> columns;
    
    void execute(Database& db) override;
};

class DropIndexStmt : public Statement {
public:
    std::string index_name;
    std::string table_name;
    
    void execute(Database& db) override;
};

class Parser {
public:
    // ... existing code ...
    
private:
    // ... existing code ...
    std::unique_ptr<Statement> parse_alter_table();
    std::unique_ptr<Statement> parse_create_index();
    std::unique_ptr<Statement> parse_drop_index();
    // ... existing code ...
}; 