#include <iostream>
#include <string>
#include <stdexcept>
#include "../include/cli/cli.h"

int main(int argc, char* argv[]) {
    try {
        std::cout << "Welcome to ToyDB - A simple C++ database with B+ Tree indexing\n"
                  << "---------------------------------------------------------------\n";
                  
        toydb::cli::CLI cli;
        
        // If we have command-line arguments, execute each as a command
        if (argc > 1) {
            for (int i = 1; i < argc; ++i) {
                cli.execute_command(argv[i]);
            }
        } else {
            // Otherwise, start the interactive CLI
            cli.start();
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Unknown error occurred" << std::endl;
        return 1;
    }
}