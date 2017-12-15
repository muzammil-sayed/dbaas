package com.jivesoftware.data.exceptions;


public class DatabaseNotFoundException extends RuntimeException {

    public DatabaseNotFoundException(String message) {
        super(message);
    }
}
