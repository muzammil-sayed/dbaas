package com.jivesoftware.data.impl;

import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;

import java.util.Optional;

public interface CreateCommandProcessor {

    Optional<CreationStep> process(String databaseID, String password, DatabaseCreationRequest databaseCreationRequest);

}
