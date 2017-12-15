package com.jivesoftware.data.impl;


import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.*;
import com.jivesoftware.data.impl.deletion.InstanceDeleteRequestProcessor;
import com.jivesoftware.data.impl.DefaultInstanceLoader;
import com.jivesoftware.data.resources.entities.Database;
import com.jivesoftware.data.resources.entities.DatabaseCreateResponse;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import com.jivesoftware.data.resources.entities.DatabaseStatus;
import com.jivesoftware.data.resources.entities.UserCreateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.text.SimpleDateFormat;

public class DatabaseManager {

    private final static Logger logger = LoggerFactory.getLogger(DatabaseManager.class);

    private static final String COLORED_FORMAT = "%s-%s";

    private final DatabaseIDHelper databaseIDHelper;
    private final SchemaManager schemaManager;
    private final DatabaseDAO databaseDAO;
    private final PasswordManager passwordManager;
    private final InstanceManager instanceManager;
    private final DatabaseCreateRequestProcessor databaseCreateRequestProcessor;
    private final PermissionManager permissionManager;
    private final InstanceDeleteRequestProcessor instanceDeleteRequestProcessor;
    private final DBaaSConfiguration dBaaSConfiguration;
    private final DefaultInstanceLoader defaultInstanceLoader;

    @Inject
    public DatabaseManager(DBaaSConfiguration dBaaSConfiguration,
                           DatabaseIDHelper databaseIDHelper,
                           SchemaManager schemaManager,
                           DatabaseDAO databaseDAO,
                           PasswordManager passwordManager,
                           InstanceManager instanceManager,
                           DatabaseCreateRequestProcessor databaseCreateRequestProcessor,
                           PermissionManager permissionManager,
                           InstanceDeleteRequestProcessor instanceDeleteRequestProcessor,
                           DefaultInstanceLoader defaultInstanceLoader) {
        this.dBaaSConfiguration = dBaaSConfiguration;
        this.databaseIDHelper = databaseIDHelper;
        this.passwordManager = passwordManager;
        this.instanceManager = instanceManager;
        this.schemaManager = schemaManager;
        this.databaseDAO = databaseDAO;
        this.databaseCreateRequestProcessor = databaseCreateRequestProcessor;
        this.permissionManager = permissionManager;
        this.instanceDeleteRequestProcessor = instanceDeleteRequestProcessor;
        this.defaultInstanceLoader = defaultInstanceLoader;

        for (DefaultInstanceLoader.DefaultInstance defaultInstance :
                defaultInstanceLoader.getInstanceList()) {

            String colorCodedId = String.format(COLORED_FORMAT,
                    dBaaSConfiguration.getSharedInstanceDeployColor(),
                    defaultInstance.getIdentifier());

            String realID = databaseIDHelper
                    .getDBInstanceId(colorCodedId);

            Optional<DBInstance> configDefaultInstance = instanceManager.getDBInstance(realID);

            if (!configDefaultInstance.isPresent()) {
                try {
                    Optional<PasswordManager.Instance> instanceOptional =
                            passwordManager.getInstance(colorCodedId);

                    if (instanceOptional.isPresent()) {
                        PasswordManager.Instance instance = instanceOptional.get();
                        instanceManager.createSharedInstance(realID,
                                instance.getUsername(), instance.getPassword(),
                                defaultInstance.getDbName());
                    } else {
                        logger.error(String.format("Was not able to find instance password " +
                                        "information for instance %s",
                                colorCodedId));
                        throw new InstanceCreationException(String.format(
                                "Was not able to find instance password information for instance %s",
                                colorCodedId));
                    }
                } catch (InstanceCreationException e) {
                    logger.error(String.format("Error creating an instance %s", realID), e);
                    throw new InstanceCreationException(e.getMessage());
                }
            }
            else {
                logger.debug(String.format("%s already exists in this environment- checking tags",
                        colorCodedId));
                try{
                    instanceManager.correctDefaultTagCheck(configDefaultInstance.get());
                } catch(Exception e) {
                    logger.error(String.format("Could not retrieve tags for %s: %s",
                            colorCodedId, e.getMessage()));
                }
                try{
                    instanceManager.checkTemplateChanges(configDefaultInstance.get(),
                            defaultInstance.getAllocatedStorage(),
                            defaultInstance.getDbInstanceClass());
                } catch(Exception e) {
                    logger.error(String.format("Could not update instance %s per template config: %s",
                            colorCodedId, e.getMessage()));
                }

            }
        }

    }

    /**
     * Tries to create the database async
     *
     * @param databaseCreationRequest database creation request
     * @return DatabaseCreate response which contains the database id assigned and the password for the
     *  database when it's done creating
     *
     * @throws DatabaseNotFoundException if the source database id is provided but does not exist
     */
    public DatabaseCreateResponse createDatabase(DatabaseCreationRequest databaseCreationRequest) {

        if (databaseCreationRequest.getSourceDatabaseIdOptional().isPresent()) {
            String sourceDatabaseId = databaseCreationRequest.getSourceDatabaseIdOptional().get();
            Optional<DatabaseInfo> databaseOptional = databaseDAO.getDatabaseInfo(sourceDatabaseId);
            if (!databaseOptional.isPresent()) {
                logger.error(String.format("The source Database with id %s not found",
                        sourceDatabaseId));
                throw new DatabaseNotFoundException(String.format(
                        "The source Database with id %s not found", sourceDatabaseId));
            }
        }

        return databaseCreateRequestProcessor.requestDatabaseCreation(databaseCreationRequest);
    }

    /**
     *
     * Get the status of the database being created
     *
     * @param databaseId database id
     * @return DatabaseStatus which contains the state of the database such as CREATING, READY, ERROR
     *
     * @throws DatabaseNotFoundException if the status for the database id is not found
     *
     */
    public DatabaseStatus getDatabaseStatus(String databaseId) {
        Optional<DatabaseStatus> statusOptional = databaseDAO.getDatabaseStatus(databaseId);
        if (!statusOptional.isPresent()) {
            logger.error(String.format("Status for database with id %s not found",
                    databaseId));
            throw new DatabaseNotFoundException(String.format("Status for database with id %s not found",
                    databaseId));
        }
        return statusOptional.get();
    }

    /**
     *
     * Get the database connection information for a READY database
     *
     * @param databaseId id of the database
     * @return Database which contains the connection information
     *
     * @throws DatabaseNotFoundException if the database id is not found or not ready
     *
     */
    public Database getDatabase(String databaseId) {
        Optional<DatabaseStatus> databaseOptional = databaseDAO.getDatabaseStatus(databaseId);
        if (!databaseOptional.isPresent()
                || DatabaseStatus.Status.READY != databaseOptional.get().getStatus()) {
            logger.error(String.format("Database with id %s not found or still being created",
                    databaseId));
            throw new DatabaseNotFoundException(String.format(
                    "Database with id %s not found or still being created", databaseId));
        }

        return databaseDAO.getDatabaseInfo(databaseId).get().getDatabase();
    }

    /**
     *
     * Mark the database as deleted if exists
     *
     * @param databaseId id of the database
     */
    public void deleteDatabase(String databaseId) {
        Optional<DatabaseInfo> databaseInfoOptional = databaseDAO.getDatabaseInfo(databaseId);
        DatabaseStatus status = getDatabaseStatus(databaseId);
        if (status.getStatus()!= DatabaseStatus.Status.DELETED) {
            if (databaseInfoOptional.isPresent()) {
                DatabaseInfo databaseInfo = databaseInfoOptional.get();
                Optional<DBInstance> dbInstanceOptional =
                        instanceManager.getDBInstance(databaseIDHelper.getDBInstanceId(
                                databaseInfo.getInstanceId()));
                if (dbInstanceOptional.isPresent()) {
                    Database database = databaseInfo.getDatabase();
                    if (databaseInfo.getTenancyType() ==
                            DatabaseCreationRequest.TenancyType.SHARED) {
                        try {
                            schemaManager.changeSchemaPassword(
                                    getMasterDatabase(dbInstanceOptional.get(),
                                            DatabaseCreationRequest.TenancyType.SHARED),
                                    database.getSchema(), database.getUser(),
                                    passwordManager.generatePassword());
                            softDeletionUpdate(databaseId);
                            logger.debug(String.format("Database %s successfully deleted",
                                    database.getSchema()));
                        }
                        catch (SchemaOperationException soe) {
                            logger.error(String.format("Database not found in instance %s. It is " +
                                            "likely the instance was deleted and recreated.",
                                    dbInstanceOptional.get().getDBInstanceIdentifier()));
                            softDeletionUpdate(databaseId);
                            throw new DatabaseDeletionException(
                                    String.format("Database %s not found in " +
                                                    "instance %s. Cause- %s.  " +
                                                    "Database record removed from Dynamo.",
                                            database.getSchema(),
                                            dbInstanceOptional.get().getDBInstanceIdentifier(),
                                            soe));
                        }
                    }
                    else {
                        try {
                            instanceDeleteRequestProcessor.requestSoftDelete(databaseId);
                        } catch (QueueSendingException e) {
                            throw new DatabaseDeletionException(
                                    String.format("Error during deletion steps for %s",
                                            dbInstanceOptional.get().getDBInstanceIdentifier()));
                        }
                    }
                } else {
                    logger.debug(String.format("Instance %s not found in RDS.  Host must have " +
                                    "been deleted.",
                            databaseIDHelper.getDBInstanceId(databaseInfo.getInstanceId())));
                    softDeletionUpdate(databaseId);
                }
            }
            else {
                logger.debug(String.format("Database %s not found in Dynamo", databaseId));
                throw new DatabaseNotFoundException(String.format("No database by the ID %s exists",
                        databaseId));
            }
        } else {
            logger.debug(String.format("Database %s already reports as deleted", databaseId));
        }
    }

    /**
     *
     * Create a temp user for the given databaseId
     *
     * @param databaseId id of the database
     */
    public UserCreateResponse createUser(String databaseId) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, 60);
        String validUntil = dateFormat.format(cal.getTime());

        String password = passwordManager.generatePassword();

        UserCreateResponse userCreateResponse = new UserCreateResponse("mq2user",password);

        Optional<DatabaseInfo> databaseInfoOptional = databaseDAO.getDatabaseInfo(databaseId);
        DatabaseStatus status = getDatabaseStatus(databaseId);

        if (databaseInfoOptional.isPresent()) {
            if (status.getStatus()== DatabaseStatus.Status.READY) {

                DatabaseInfo databaseInfo = databaseInfoOptional.get();
                Optional<DBInstance> dbInstanceOptional = instanceManager.getDBInstance(databaseIDHelper.getDBInstanceId(databaseInfo.getInstanceId()));

                Database database = databaseInfo.getDatabase();
                try {
                    schemaManager.createUser(getMasterDatabase(dbInstanceOptional.get(), DatabaseCreationRequest.TenancyType.SHARED), database.getSchema(), password,validUntil);
                } catch (SchemaOperationException e){
                    logger.error(String.format("Error creating user"), e);
                    throw new SchemaOperationException(e.getMessage());
                }
            } else {
                logger.debug(String.format("Current database status is %s which is not a valid state to run queries", status.getStatus()));
                throw new DatabaseNotFoundException(String.format("Current database status is %s which is not a valid state to run queries", status.getStatus()));
            }

        } else {
            logger.debug(String.format("Database %s not found in Dynamo", databaseId));
            throw new DatabaseNotFoundException(String.format("No database by the ID %s exists", databaseId));
        }
        return userCreateResponse;
    }

    private void softDeletionUpdate(String databaseId){
        DatabaseStatus databaseStatus =
                new DatabaseStatus(DatabaseStatus.Status.DELETED, null, databaseId);
        databaseDAO.updateStatus(databaseStatus);
    }

    MasterDatabase getMasterDatabase(DBInstance host, DatabaseCreationRequest.TenancyType type) {
        String password;
        if(type.equals(DatabaseCreationRequest.TenancyType.SHARED)){
            password = passwordManager.getInstance(databaseIDHelper.getDatabaseInstanceId(host))
                    .get().getPassword();
        }
        else {
            logger.error(String.format("This method is not suitable to be used on a dedicated " +
                    " instance.  It does not block so instance %s will not be in ready state when " +
                    "next bit of code is run against this master database object",
                    host.getDBInstanceIdentifier()));
            throw new DatabaseDeletionException(String.format("getMasterDatabase should not be used" +
                    " on a dedicated instance: Instance %s. This will require some kind of " +
                    "synchronous blocking against RDS so the instance isn't affected while updating",
                    host.getDBInstanceIdentifier()));
        }
        return new MasterDatabase(host.getEndpoint().getAddress(),
                host.getMasterUsername(), password, host.getEndpoint().getPort(), host.getDBName());
    }

    public void hardDeleteDatabase(String token) {

        if(!permissionManager.isAllowed(token)){
            throw new TokenNotAuthorizedException("Your token does not match the authorized" +
                    " admin token to allow hard deletion");
        } else {
            List<Database> deleted = databaseDAO.getDeletedDatabases();

            for(Database deleteDB : deleted){
                try {
                    logger.debug(String.format("Hard deleting %s", deleteDB.getId()));

                    Optional<DatabaseInfo> databaseInfoOptional = databaseDAO.getDatabaseInfo(
                            deleteDB.getId());
                    if (databaseInfoOptional.isPresent()) {
                        DatabaseInfo databaseInfo = databaseInfoOptional.get();

                        Optional<DBInstance> dbInstanceOptional =
                                instanceManager.getDBInstance(databaseIDHelper.getDBInstanceId(
                                        databaseInfo.getInstanceId()));
                        if(dbInstanceOptional.isPresent()) {
                            if(databaseInfo.getTenancyType() ==
                                    DatabaseCreationRequest.TenancyType.DEDICATED) {
                                logger.debug(String.format("Attempting to delete instance with " +
                                        "instanceId %s", databaseInfo.getInstanceId()));
                                instanceManager.deleteInstance(databaseInfo.getInstanceId());
                            }
                            else {
                                Database database = databaseInfo.getDatabase();
                                MasterDatabase masterDatabase = getMasterDatabase(
                                        dbInstanceOptional.get(), databaseInfo.getTenancyType());
                                if(schemaManager.isSchemaExists(masterDatabase, deleteDB.getSchema())){
                                    schemaManager.hardDeleteSchema(masterDatabase,
                                            database.getUser(), database.getSchema());
                                    logger.warn(String.format("Schema %s deleted on instance %s",
                                            database.getSchema(), database.getId()));
                                } else {
                                    logger.warn(String.format("Schema %s not found on instance %s",
                                            database.getSchema(), database.getId()));
                                }
                            }
                        } else {
                            logger.warn(String.format("DBInstance not found for %s",
                                    databaseInfo.getInstanceId()));
                        }
                    }
                    databaseDAO.deleteDatabaseRecord(deleteDB.getId());
                } catch (SchemaOperationException soe) {
                    logger.error(String.format("Error performing SQL to delete database %s",
                            deleteDB.getId()));
                } catch (DatabaseDeletionException dde) {
                    logger.error(String.format("Error deleting dedicated instance- %s", dde));
                }
                catch (Exception e) {
                    logger.error(String.format("Error deleting database %s. Host: %s, User: %s, " +
                                    "Schema: %s", deleteDB.getId(), deleteDB.getHost(),
                            deleteDB.getUser(), deleteDB.getSchema()));
                }
            }
        }
    }

    public List<DBaaSConfiguration.InstanceType> getListOfInstanceTypes(){
        return dBaaSConfiguration.getInstanceTypes();
    }

}
