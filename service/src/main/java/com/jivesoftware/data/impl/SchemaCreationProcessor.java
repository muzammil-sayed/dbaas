package com.jivesoftware.data.impl;

import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.exceptions.SchemaOperationException;
import com.jivesoftware.data.resources.entities.Database;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;

public class SchemaCreationProcessor implements CreateCommandProcessor {

    private final static Logger logger = LoggerFactory.getLogger(SchemaCreationProcessor.class);
    private final InstanceManager instanceManager;
    private final DatabaseDAO databaseDAO;
    private final DatabaseIDHelper databaseIDHelper;
    private final SchemaManager schemaManager;
    private final PasswordManager passwordManager;

    @Inject
    public SchemaCreationProcessor(InstanceManager instanceManager,
                                   DatabaseDAO databaseDAO,
                                   DatabaseIDHelper databaseIDHelper,
                                   SchemaManager schemaManager,
                                   PasswordManager passwordManager){
        this.instanceManager = instanceManager;
        this.databaseDAO = databaseDAO;
        this.databaseIDHelper = databaseIDHelper;
        this.schemaManager = schemaManager;
        this.passwordManager = passwordManager;
    }

    @Override
    public Optional<CreationStep> process(String databaseId, String password,
                                          DatabaseCreationRequest databaseCreationRequest){

        logger.debug("Schema creation beginning");
        String schemaName = databaseId;
        String user = schemaName;
        MasterDatabase targetMasterDatabase = null;
        String host = null;
        Optional<DBInstance> dbInstance = Optional.empty();
        if (databaseCreationRequest.getTenancyType() == DatabaseCreationRequest.TenancyType.SHARED) {
            logger.debug("Creating schema on shared instance");
            dbInstance = instanceManager.findSharedInstance();
            if (dbInstance.isPresent()) {
                host = dbInstance.get().getEndpoint().getAddress();
                targetMasterDatabase = getMasterDatabase(dbInstance.get());
            }
        }
        else {
            logger.debug("Creating schema on new instance");
            Optional<DatabaseInfo> databaseInfo = databaseDAO.getDatabaseInfo(databaseId);
            if (databaseInfo.isPresent()) {
                dbInstance = instanceManager.getDBInstance(databaseIDHelper
                        .getDBInstanceId(databaseInfo.get().getInstanceId()));
                if (dbInstance.isPresent()) {
                    host = dbInstance.get().getEndpoint().getAddress();
                    int port = dbInstance.get().getEndpoint().getPort();
                    targetMasterDatabase = new MasterDatabase(host, "postgres",
                            password, port, "postgres");
                }
            }
        }

        if (dbInstance.isPresent()) {
            try {
                schemaManager.createSchema(targetMasterDatabase, schemaName, user, password);
                Database database = new Database(databaseId,
                        user,
                        host,
                        dbInstance.get().getEndpoint().getPort(),
                        schemaName);
                databaseDAO.putDatabase(databaseId,
                        databaseIDHelper.getDatabaseInstanceId(dbInstance.get()),
                        database,
                        databaseCreationRequest.getDataLocality(),
                        databaseCreationRequest.getTenancyType(),
                        databaseCreationRequest.getServiceTag());
                logger.debug("Schema step completed");
            } catch (Exception e) {
                logger.error(String.format("Error creating schema %s", databaseId), e);
                throw new SchemaOperationException(e.getMessage());
            }
        } else {
            logger.error("No appropriate dbInstance found");
            throw new SchemaOperationException(String.format(
                    "Can't find the instance to create a schema for %s", databaseId));
        }

        if (databaseCreationRequest.getSourceDatabaseIdOptional().isPresent()) {
            return Optional.of(CreationStep.CLONE);
        } else {
            return Optional.empty();
        }
    }

    private MasterDatabase getMasterDatabase(DBInstance host) {
        String password = passwordManager.getInstance(databaseIDHelper.getDatabaseInstanceId(host))
                .get().getPassword();
        return new MasterDatabase(host.getEndpoint().getAddress(),
                host.getMasterUsername(), password, host.getEndpoint().getPort(), host.getDBName());
    }
}
