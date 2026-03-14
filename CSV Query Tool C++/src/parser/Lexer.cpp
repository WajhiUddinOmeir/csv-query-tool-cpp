#include "Lexer.hpp"
#include <algorithm>
#include <stdexcept>

// ──────────────────────────────────────────────────────────────
//  Keyword table
// ──────────────────────────────────────────────────────────────
const std::unordered_map<std::string, TokenType> Lexer::KEYWORDS = {
    {"SELECT",    TokenType::SELECT},    {"FROM",      TokenType::FROM},
    {"WHERE",     TokenType::WHERE},     {"AS",        TokenType::AS},
    {"DISTINCT",  TokenType::DISTINCT},  {"ALL",       TokenType::ALL},
    {"AND",       TokenType::AND},       {"OR",        TokenType::OR},
    {"NOT",       TokenType::NOT},
    {"IN",        TokenType::IN},        {"BETWEEN",   TokenType::BETWEEN},
    {"IS",        TokenType::IS},        {"NULL",      TokenType::NULL_TOK},
    {"LIKE",      TokenType::LIKE},
    {"JOIN",      TokenType::JOIN},      {"INNER",     TokenType::INNER},
    {"LEFT",      TokenType::LEFT},      {"RIGHT",     TokenType::RIGHT},
    {"OUTER",     TokenType::OUTER},     {"CROSS",     TokenType::CROSS},
    {"ON",        TokenType::ON},
    {"GROUP",     TokenType::GROUP},     {"BY",        TokenType::BY},
    {"HAVING",    TokenType::HAVING},
    {"ORDER",     TokenType::ORDER},     {"ASC",       TokenType::ASC},
    {"DESC",      TokenType::DESC},
    {"LIMIT",     TokenType::LIMIT},     {"OFFSET",    TokenType::OFFSET},
    {"TRUE",      TokenType::TRUE_TOK},  {"FALSE",     TokenType::FALSE_TOK},
    {"COUNT",     TokenType::COUNT},     {"SUM",       TokenType::SUM},
    {"AVG",       TokenType::AVG},       {"MIN",       TokenType::MIN_TOK},
    {"MAX",       TokenType::MAX_TOK},
    {"UPPER",     TokenType::UPPER},     {"LOWER",     TokenType::LOWER},
    {"LENGTH",    TokenType::LENGTH},    {"TRIM",      TokenType::TRIM_TOK},
    {"ABS",       TokenType::ABS_TOK},   {"ROUND",     TokenType::ROUND_TOK},
    {"COALESCE",  TokenType::COALESCE_TOK}, {"CONCAT", TokenType::CONCAT_TOK},
    {"SUBSTRING", TokenType::SUBSTRING_TOK},
    {"LOAD",      TokenType::LOAD},      {"SHOW",      TokenType::SHOW},
    {"TABLES",    TokenType::TABLES},    {"DESCRIBE",  TokenType::DESCRIBE},
    {"CREATE",    TokenType::CREATE},    {"INDEX",     TokenType::INDEX},
    {"DROP",      TokenType::DROP},      {"INDEXES",   TokenType::INDEXES},
    {"TABLE",     TokenType::TABLE},     {"INSERT",    TokenType::INSERT},
    {"INTO",      TokenType::INTO},      {"VALUES",    TokenType::VALUES},
    {"DELETE",    TokenType::DELETE},
    {"ALTER",     TokenType::ALTER},     {"COLUMN",    TokenType::COLUMN},
    {"RENAME",    TokenType::RENAME},    {"TO",        TokenType::TO},
    {"ADD",       TokenType::ADD},
    {"UPDATE",    TokenType::UPDATE},    {"SET",       TokenType::SET},
    {"EXPORT",    TokenType::EXPORT},    {"FORMAT",    TokenType::FORMAT},
};

// ──────────────────────────────────────────────────────────────
//  Constructor
// ──────────────────────────────────────────────────────────────

Lexer::Lexer(const std::string& input)
    : input_(input), pos_(0), line_(1), col_(1) {}

// ──────────────────────────────────────────────────────────────
//  Public: tokenize
// ──────────────────────────────────────────────────────────────

std::vector<Token> Lexer::tokenize() {
    std::vector<Token> tokens;
    while (pos_ < input_.size()) {
        skipWhitespaceAndComments();
        if (pos_ >= input_.size()) break;

        char c = current();
        if (std::isalpha((unsigned char)c) || c == '_')
            tokens.push_back(readIdentifierOrKeyword());
        else if (std::isdigit((unsigned char)c))
            tokens.push_back(readNumber());
        else if (c == '\'')
            tokens.push_back(readString());
        else if (c == '"')
            tokens.push_back(readQuotedIdentifier());
        else
            tokens.push_back(readOperatorOrPunctuation());
    }
    tokens.emplace_back(TokenType::EOF_TOK, "", line_, col_);
    return tokens;
}

// ──────────────────────────────────────────────────────────────
//  Character helpers
// ──────────────────────────────────────────────────────────────

char Lexer::current() const { return input_[pos_]; }

char Lexer::advance() {
    char c = input_[pos_++];
    if (c == '\n') { line_++; col_ = 1; }
    else col_++;
    return c;
}

bool Lexer::hasMore() const { return pos_ < input_.size(); }

char Lexer::peekChar(int offset) const {
    size_t idx = pos_ + offset;
    return idx < input_.size() ? input_[idx] : '\0';
}

std::string Lexer::toUpper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), ::toupper);
    return s;
}

// ──────────────────────────────────────────────────────────────
//  Skipping
// ──────────────────────────────────────────────────────────────

void Lexer::skipWhitespaceAndComments() {
    while (hasMore()) {
        char c = current();
        if (std::isspace((unsigned char)c)) {
            advance();
        } else if (c == '-' && peekChar(1) == '-') {
            // Line comment: skip to end of line
            while (hasMore() && current() != '\n') advance();
        } else {
            break;
        }
    }
}

// ──────────────────────────────────────────────────────────────
//  Token readers
// ──────────────────────────────────────────────────────────────

Token Lexer::readIdentifierOrKeyword() {
    int startLine = line_, startCol = col_;
    std::string sb;
    while (hasMore() && (std::isalnum((unsigned char)current()) || current() == '_'))
        sb += advance();
    auto it = KEYWORDS.find(toUpper(sb));
    if (it != KEYWORDS.end())
        return Token(it->second, sb, startLine, startCol);
    return Token(TokenType::IDENTIFIER, sb, startLine, startCol);
}

Token Lexer::readQuotedIdentifier() {
    int startLine = line_, startCol = col_;
    advance(); // skip opening "
    std::string sb;
    while (hasMore() && current() != '"') sb += advance();
    if (hasMore()) advance(); // skip closing "
    return Token(TokenType::IDENTIFIER, sb, startLine, startCol);
}

Token Lexer::readNumber() {
    int startLine = line_, startCol = col_;
    std::string sb;
    bool isDecimal = false;

    while (hasMore() && std::isdigit((unsigned char)current())) sb += advance();

    if (hasMore() && current() == '.' && std::isdigit((unsigned char)peekChar(1))) {
        isDecimal = true;
        sb += advance(); // dot
        while (hasMore() && std::isdigit((unsigned char)current())) sb += advance();
    }

    TokenType tt = isDecimal ? TokenType::DECIMAL_LITERAL : TokenType::INTEGER_LITERAL;
    return Token(tt, sb, startLine, startCol);
}

Token Lexer::readString() {
    int startLine = line_, startCol = col_;
    advance(); // skip opening '
    std::string sb;
    while (hasMore()) {
        char c = current();
        if (c == '\'') {
            advance();
            if (hasMore() && current() == '\'') {
                sb += advance(); // escaped quote '' → '
            } else {
                break;
            }
        } else {
            sb += advance();
        }
    }
    return Token(TokenType::STRING_LITERAL, sb, startLine, startCol);
}

Token Lexer::readOperatorOrPunctuation() {
    int startLine = line_, startCol = col_;
    char c = advance();
    switch (c) {
        case '(': return Token(TokenType::LEFT_PAREN,  "(", startLine, startCol);
        case ')': return Token(TokenType::RIGHT_PAREN, ")", startLine, startCol);
        case ',': return Token(TokenType::COMMA,       ",", startLine, startCol);
        case ';': return Token(TokenType::SEMICOLON,   ";", startLine, startCol);
        case '.': return Token(TokenType::DOT,         ".", startLine, startCol);
        case '+': return Token(TokenType::PLUS,        "+", startLine, startCol);
        case '-': return Token(TokenType::MINUS,       "-", startLine, startCol);
        case '*': return Token(TokenType::STAR,        "*", startLine, startCol);
        case '/': return Token(TokenType::DIVIDE,      "/", startLine, startCol);
        case '%': return Token(TokenType::MODULO,      "%", startLine, startCol);
        case '=': return Token(TokenType::EQUALS,      "=", startLine, startCol);
        case '<':
            if (hasMore() && current() == '=') { advance(); return Token(TokenType::LESS_THAN_EQUALS, "<=", startLine, startCol); }
            if (hasMore() && current() == '>') { advance(); return Token(TokenType::NOT_EQUALS,        "<>", startLine, startCol); }
            return Token(TokenType::LESS_THAN, "<", startLine, startCol);
        case '>':
            if (hasMore() && current() == '=') { advance(); return Token(TokenType::GREATER_THAN_EQUALS, ">=", startLine, startCol); }
            return Token(TokenType::GREATER_THAN, ">", startLine, startCol);
        case '!':
            if (hasMore() && current() == '=') { advance(); return Token(TokenType::NOT_EQUALS, "!=", startLine, startCol); }
            throw std::runtime_error("Unexpected character '!' at line " + std::to_string(startLine));
        default:
            throw std::runtime_error(std::string("Unexpected character '") + c +
                "' at line " + std::to_string(startLine));
    }
}
