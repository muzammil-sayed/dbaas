package com.jivesoftware.data.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;

@SuppressWarnings("unused")
public class CreationRequestMessage {

    private final DatabaseCreationRequest databaseCreationRequest;
    private final String databaseId;
    private final String password;
    private final CreationStep creationStep;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public CreationRequestMessage(@JsonProperty("databaseId") String databaseId,
                                  @JsonProperty("password") String password,
                                  @JsonProperty("creationStep") CreationStep creationStep,
                                  @JsonProperty("databaseCreationRequest") DatabaseCreationRequest databaseCreationRequest) {
        this.databaseId = databaseId;
        this.password = password;
        this.creationStep = creationStep;
        this.databaseCreationRequest = databaseCreationRequest;
    }

    @JsonProperty(required = true)
    public DatabaseCreationRequest getDatabaseCreationRequest() {
        return databaseCreationRequest;
    }

    @JsonProperty(required = true)
    public String getDatabaseId() {
        return databaseId;
    }

    @JsonProperty(required = true)
    public String getPassword() {
        return password;
    }

    @JsonProperty(required = true)
    public CreationStep getCreationStep() {
        return creationStep;
    }

}
