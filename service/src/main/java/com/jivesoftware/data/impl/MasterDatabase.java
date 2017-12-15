package com.jivesoftware.data.impl;


public class MasterDatabase {

    private final String username;
    private final String password;
    private final String host;
    private final Integer port;
    private final String schema;

    public MasterDatabase(String host, String username, String password, Integer port, String schema) {
        this.port = port;
        this.host = host;
        this.password = password;
        this.username = username;
        this.schema = schema;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getSchema() {
        return schema;
    }
}
