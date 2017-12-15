package com.jivesoftware.data.impl.deletion;

import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.impl.DatabaseIDHelper;
import com.jivesoftware.data.impl.InstanceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;

public class PasswordResetProcessor implements DeleteCommandProcessor {

    private final static Logger logger = LoggerFactory.getLogger(PasswordResetProcessor.class);

    private final InstanceManager instanceManager;
    private final DatabaseIDHelper databaseIDHelper;

    @Inject
    public PasswordResetProcessor(InstanceManager instanceManager,
                                  DatabaseIDHelper databaseIDHelper) {

        this.instanceManager = instanceManager;
        this.databaseIDHelper = databaseIDHelper;

    }

    @Override
    public Optional<DeletionStep> process(String databaseId, String password) {

        logger.debug(String.format("Resetting master password for %s", databaseId));

        Optional<DBInstance> dbInstanceOptional = instanceManager.getDBInstance(
                databaseIDHelper.getDBInstanceId(databaseId));

        if(!dbInstanceOptional.isPresent()) {
            return Optional.of(DeletionStep.DELETING);
        }

        instanceManager.modifyMasterPassword(dbInstanceOptional.get(),
                password);

        return Optional.of(DeletionStep.RESETTING_PASSWORD);

    }
}
