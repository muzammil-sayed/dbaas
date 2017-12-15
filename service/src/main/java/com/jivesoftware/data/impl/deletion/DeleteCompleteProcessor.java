package com.jivesoftware.data.impl.deletion;

import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.impl.DatabaseIDHelper;
import com.jivesoftware.data.impl.InstanceManager;
import com.jivesoftware.data.impl.SchemaManager;
import com.jivesoftware.data.impl.MasterDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;

public class DeleteCompleteProcessor implements DeleteCommandProcessor {

    private final static Logger logger = LoggerFactory.getLogger(DeleteCompleteProcessor.class);

    private final InstanceManager instanceManager;
    private final SchemaManager schemaManager;
    private final DatabaseIDHelper databaseIDHelper;
    private MasterDatabase masterDatabase;

    @Inject
    public DeleteCompleteProcessor(InstanceManager instanceManager,
                                   SchemaManager schemaManager,
                                   DatabaseIDHelper databaseIDHelper) {
        this.instanceManager = instanceManager;
        this.schemaManager = schemaManager;
        this.databaseIDHelper = databaseIDHelper;
    }

    @Override
    public Optional<DeletionStep> process(String databaseId, String password) {

        logger.debug(String.format("Checking if instance %s password reset complete", databaseId));

        Optional<DBInstance> dbInstanceOptional = instanceManager.getDBInstance(
                databaseIDHelper.getDBInstanceId(databaseId)
        );

        if(!dbInstanceOptional.isPresent()) {
            return Optional.of(DeletionStep.RESETTING_PASSWORD);
        }

        DBInstance instance = dbInstanceOptional.get();

        if(instanceManager.isReady(instance)){
            masterDatabase =
                    new MasterDatabase(instance.getEndpoint().getAddress(),
                            instance.getMasterUsername(), password,
                            instance.getEndpoint().getPort(),
                            instance.getDBName());
            schemaManager.changeSchemaPassword(masterDatabase, databaseId, databaseId, password);
            logger.debug(String.format("Password reset finished for instance %s", databaseId));
            return Optional.empty();
        }
        else {
            return Optional.of(DeletionStep.RESETTING_PASSWORD);
        }
    }
}
