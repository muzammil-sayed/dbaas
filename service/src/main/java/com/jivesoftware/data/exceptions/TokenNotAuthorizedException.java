package com.jivesoftware.data.exceptions;

public class TokenNotAuthorizedException extends RuntimeException {

    public TokenNotAuthorizedException(String message) {
        super(message);
    }
}
