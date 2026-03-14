#pragma once
#include "TokenType.hpp"
#include <string>

struct Token {
    TokenType   type;
    std::string value;
    int         line;
    int         col;

    Token(TokenType type, std::string value, int line, int col)
        : type(type), value(std::move(value)), line(line), col(col) {}
};
