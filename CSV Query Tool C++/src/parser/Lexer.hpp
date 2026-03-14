#pragma once
#include "Token.hpp"
#include <vector>
#include <string>
#include <unordered_map>

/**
 * SQL Lexer — tokenizes raw SQL text into a list of Tokens.
 * Hand-written (no regex), handles:
 *   - Keywords (case-insensitive)
 *   - Identifiers (unquoted and double-quoted)
 *   - String literals (single-quoted, '' escape for embedded quotes)
 *   - Numeric literals (integers and decimals)
 *   - Operators and punctuation
 *   - Line comments (-- ...)
 *   - Whitespace skipping
 */
class Lexer {
public:
    explicit Lexer(const std::string& input);
    std::vector<Token> tokenize();

private:
    std::string input_;
    size_t      pos_;
    int         line_;
    int         col_;

    static const std::unordered_map<std::string, TokenType> KEYWORDS;

    char   current() const;
    char   advance();
    bool   hasMore() const;
    char   peekChar(int offset = 1) const;

    void  skipWhitespaceAndComments();
    Token readIdentifierOrKeyword();
    Token readQuotedIdentifier();
    Token readNumber();
    Token readString();
    Token readOperatorOrPunctuation();

    static std::string toUpper(std::string s);
};
