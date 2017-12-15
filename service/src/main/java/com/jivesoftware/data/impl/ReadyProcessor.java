package com.jivesoftware.data.impl;

import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.exceptions.DatabaseNotFoundException;
import com.jivesoftware.data.resources.entities.Database;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;

public class ReadyProcessor implements CreateCommandProcessor {

    private final static Logger logger = LoggerFactory.getLogger(ReadyProcessor.class);
    private final InstanceManager instanceManager;
    private final DatabaseDAO databaseDAO;
    private final DatabaseIDHelper databaseIDHelper;

    @Inject
    public ReadyProcessor(InstanceManager instanceManager,
                          DatabaseDAO databaseDAO,
                          DatabaseIDHelper databaseIDHelper){
        this.instanceManager = instanceManager;
        this.databaseDAO = databaseDAO;
        this.databaseIDHelper = databaseIDHelper;
    }

    @Override
    public Optional<CreationStep> process(String databaseId, String password,
                                          DatabaseCreationRequest databaseCreationRequest){

        logger.debug("Start of READY process step");

        Optional<DatabaseInfo> instanceIdOptional = databaseDAO.getDatabaseInfo(databaseId);
        if (instanceIdOptional.isPresent()) {
            Optional<DBInstance> dbInstanceOptional =
                    instanceManager.getDBInstance(
                            databaseIDHelper.getDBInstanceId(instanceIdOptional.get().getInstanceId()));
            if (dbInstanceOptional.isPresent()) {
                DBInstance dbInstance = dbInstanceOptional.get();
                if (instanceManager.isReady(dbInstance)) {

                    databaseDAO.putDatabase(databaseId,
                            databaseIDHelper.getDatabaseInstanceId(dbInstance),
                            new Database(databaseId,
                                    dbInstance.getMasterUsername(),
                                    dbInstance.getEndpoint().getAddress(),
                                    dbInstance.getEndpoint().getPort(),
                                    dbInstance.getDBName()),
                            databaseCreationRequest.getDataLocality(),
                            databaseCreationRequest.getTenancyType(),
                            databaseCreationRequest.getServiceTag());
                    return Optional.of(CreationStep.SCHEMA);
                }
                else {
                    return Optional.of(CreationStep.INSTANCE_READY);
                }
            } else {
                throw new DatabaseNotFoundException(String.format(
                        "Specified instance not found in this RDS client for database %s", databaseId));
            }
        } else {
            throw new DatabaseNotFoundException(String.format(
                    "Specified databaseId (%s) not found in Dynamo table", databaseId));
        }

    }
}
