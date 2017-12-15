package com.jivesoftware.data.impl.deletion;


import java.util.Optional;

public interface DeleteCommandProcessor {

    Optional<DeletionStep> process(String databaseID, String password);

}
