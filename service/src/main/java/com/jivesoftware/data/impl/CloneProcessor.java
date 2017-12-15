package com.jivesoftware.data.impl;

import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.exceptions.DatabaseNotFoundException;
import com.jivesoftware.data.resources.entities.Database;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;

public class CloneProcessor implements CreateCommandProcessor {

    private final static Logger logger = LoggerFactory.getLogger(CloneProcessor.class);
    private final InstanceManager instanceManager;
    private final DatabaseDAO databaseDAO;
    private final DatabaseIDHelper databaseIDHelper;
    private final CloneManager cloneManager;
    private final PasswordManager passwordManager;

    @Inject
    public CloneProcessor(InstanceManager instanceManager,
                          DatabaseDAO databaseDAO,
                          DatabaseIDHelper databaseIDHelper,
                          CloneManager cloneManager,
                          PasswordManager passwordManager){
        this.instanceManager = instanceManager;
        this.databaseDAO = databaseDAO;
        this.databaseIDHelper = databaseIDHelper;
        this.cloneManager = cloneManager;
        this.passwordManager = passwordManager;
    }

    @Override
    public Optional<CreationStep> process(String databaseId, String password,
                                          DatabaseCreationRequest databaseCreationRequest){

        logger.debug(String.format("Cloning process beginning for database %s", databaseId));

        if (databaseCreationRequest.getSourceDatabaseIdOptional().isPresent()) {
            Optional<DatabaseInfo> targetDatabaseOptional = databaseDAO.getDatabaseInfo(databaseId);
            if (targetDatabaseOptional.isPresent()) {
                Optional<DBInstance> targetInstanceOptional =
                        instanceManager.getDBInstance(
                                databaseIDHelper.getDBInstanceId(
                                        targetDatabaseOptional.get().getInstanceId()));

                MasterDatabase targetMasterDatabase = getMasterDatabase(
                        targetInstanceOptional.get());
                MasterDatabase sourceMasterDatabase = null;
                Optional<DatabaseInfo> sourceDatabaseOptional =
                        databaseDAO.getDatabaseInfo(
                                databaseCreationRequest.getSourceDatabaseIdOptional().get());

                if (sourceDatabaseOptional.isPresent()) {
                    Database sourceDatabase = sourceDatabaseOptional.get().getDatabase();
                    // If cloning within same host
                    if (sourceDatabase.getHost().equals(targetMasterDatabase.getHost())) {
                        sourceMasterDatabase = targetMasterDatabase;
                    } else {
                        Optional<DBInstance> sourceInstanceOptional =
                                instanceManager.getDBInstance(sourceDatabase.getId());

                        if (sourceInstanceOptional.isPresent()) {
                            sourceMasterDatabase = getMasterDatabase(sourceInstanceOptional.get());
                        }
                    }

                    logger.debug(String.format("Cloning starting now for source: %s target: %s", sourceDatabase.getSchema(), targetMasterDatabase.getSchema()));

                    cloneManager.clone(sourceMasterDatabase, sourceDatabase.getSchema(),
                            targetDatabaseOptional.get().getDatabase(), password);

                    return Optional.empty();
                } else {
                    logger.error(String.format("Database source %s does not exist",
                            databaseCreationRequest.getSourceDatabaseIdOptional().get()));
                    throw new DatabaseNotFoundException(String.format(
                            "Database source %s does not exist",
                            databaseCreationRequest.getSourceDatabaseIdOptional().get()));
                }

            }
            else {
                logger.error("Target database not fully created before reaching cloning logic.");
                throw new DatabaseNotFoundException(String.format(
                        "Target database (%s) does not exist, how did you get here?", databaseId));
            }
        }
        else {
            logger.error("No idea how you got here. In clone processor with no sourceDatabaseID");
            throw new DatabaseNotFoundException(String.format(
                    "No source database ID found in creation request, but you got to this logic- " +
                            "huh?? Target databaseID: %s", databaseId));
        }
    }

    private MasterDatabase getMasterDatabase(DBInstance host) {
        String password = passwordManager.getInstance(databaseIDHelper.getDatabaseInstanceId(host))
                .get().getPassword();
        return new MasterDatabase(host.getEndpoint().getAddress(),
                host.getMasterUsername(), password, host.getEndpoint().getPort(), host.getDBName());
    }
}
