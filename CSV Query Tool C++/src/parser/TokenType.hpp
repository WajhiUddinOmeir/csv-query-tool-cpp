#pragma once

/** All token types recognized by the SQL lexer. */
enum class TokenType {
    // Keywords
    SELECT, FROM, WHERE, AS, DISTINCT, ALL,
    AND, OR, NOT,
    IN, BETWEEN, IS, NULL_TOK, LIKE,
    JOIN, INNER, LEFT, RIGHT, OUTER, CROSS, ON,
    GROUP, BY, HAVING,
    ORDER, ASC, DESC,
    LIMIT, OFFSET,
    TRUE_TOK, FALSE_TOK,

    // Aggregate function keywords
    COUNT, SUM, AVG, MIN_TOK, MAX_TOK,

    // Scalar function keywords
    UPPER, LOWER, LENGTH, TRIM_TOK, ABS_TOK, ROUND_TOK,
    COALESCE_TOK, CONCAT_TOK, SUBSTRING_TOK,

    // DDL / DML keywords
    LOAD, SHOW, TABLES, DESCRIBE,
    CREATE, INDEX, DROP, INDEXES,
    TABLE, INSERT, INTO, VALUES, DELETE,
    ALTER, COLUMN, RENAME, TO, ADD,
    UPDATE, SET,
    EXPORT, FORMAT,

    // Literals
    IDENTIFIER,
    STRING_LITERAL,
    INTEGER_LITERAL,
    DECIMAL_LITERAL,

    // Operators
    EQUALS,               // =
    NOT_EQUALS,           // != or <>
    LESS_THAN,            // <
    GREATER_THAN,         // >
    LESS_THAN_EQUALS,     // <=
    GREATER_THAN_EQUALS,  // >=
    PLUS,                 // +
    MINUS,                // -
    STAR,                 // * (SELECT * and multiplication)
    DIVIDE,               // /
    MODULO,               // %

    // Punctuation
    LEFT_PAREN,           // (
    RIGHT_PAREN,          // )
    COMMA,                // ,
    SEMICOLON,            // ;
    DOT,                  // .

    // End of input
    EOF_TOK
};
