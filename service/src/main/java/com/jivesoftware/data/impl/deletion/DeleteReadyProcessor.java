package com.jivesoftware.data.impl.deletion;

import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.impl.DatabaseIDHelper;
import com.jivesoftware.data.impl.InstanceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;

public class DeleteReadyProcessor implements DeleteCommandProcessor {

    private final static Logger logger = LoggerFactory.getLogger(DeleteReadyProcessor.class);

    private final InstanceManager instanceManager;
    private final DatabaseIDHelper databaseIDHelper;

    @Inject
    public DeleteReadyProcessor(InstanceManager instanceManager,
                                DatabaseIDHelper databaseIDHelper) {
        this.instanceManager = instanceManager;
        this.databaseIDHelper = databaseIDHelper;
    }

    @Override
    public Optional<DeletionStep> process(String databaseId, String password) {

        logger.debug(String.format("Checking if instance %s ready for password change", databaseId));

        Optional<DBInstance> dbInstanceOptional = instanceManager.getDBInstance(
                databaseIDHelper.getDBInstanceId(databaseId));

        if(!dbInstanceOptional.isPresent()) {
            return Optional.of(DeletionStep.PREPARING);
        }

        if(instanceManager.isAvailable(dbInstanceOptional.get())){
            return Optional.of(DeletionStep.DELETING);
        }
        else {
            return Optional.of(DeletionStep.PREPARING);
        }
    }
}
