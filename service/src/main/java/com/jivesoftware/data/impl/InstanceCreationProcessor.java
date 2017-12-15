package com.jivesoftware.data.impl;

import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.InstanceCreationException;
import com.jivesoftware.data.resources.entities.Database;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;

public class InstanceCreationProcessor implements CreateCommandProcessor {

    private final static Logger logger = LoggerFactory.getLogger(InstanceCreationProcessor.class);
    private final DatabaseIDHelper databaseIDHelper;
    private final InstanceManager instanceManager;
    private final DatabaseDAO databaseDAO;
    private final DBaaSConfiguration dBaaSConfiguration;

    @Inject
    public InstanceCreationProcessor(DatabaseIDHelper databaseIDHelper,
                                     InstanceManager instanceManager,
                                     DatabaseDAO databaseDAO,
                                     DBaaSConfiguration dBaaSConfiguration){
        this.databaseIDHelper = databaseIDHelper;
        this.instanceManager = instanceManager;
        this.databaseDAO = databaseDAO;
        this.dBaaSConfiguration = dBaaSConfiguration;
    }

    @Override
    public Optional<CreationStep> process(String databaseId, String password,
                                          DatabaseCreationRequest databaseCreationRequest){

        logger.debug("Instance creation step beginning");

        String instanceClass;
        Integer instanceStorage;

        if(databaseCreationRequest.getInstanceClassOptional().isPresent()) {
            instanceClass = getClassNameConverter(
                    databaseCreationRequest.getInstanceClassOptional().get());
        }
        else {
            instanceClass = getClassNameConverter(dBaaSConfiguration.getInstanceTemplate().getDbInstanceClass());
        }

        if(databaseCreationRequest.getInstanceStorageOptional().isPresent()) {
            instanceStorage = databaseCreationRequest.getInstanceStorageOptional().get();
        }
        else {
            instanceStorage = dBaaSConfiguration.getInstanceTemplate().getAllocatedStorage();
        }

        try {
            String realId = getRealId(databaseIDHelper.getDBInstanceId(databaseId));
            DBInstance dbInstance = instanceManager.createDedicatedInstance(realId, password,
                    databaseCreationRequest.getServiceTag(),
                    databaseCreationRequest.getServiceComponentTag(),
                    instanceClass, instanceStorage);
            databaseDAO.putDatabase(databaseId,
                    databaseIDHelper.getDatabaseInstanceId(dbInstance),
                    new Database(databaseId,
                            dbInstance.getMasterUsername(),
                            "pending",
                            -1,
                            dbInstance.getDBName()), databaseCreationRequest.getDataLocality(),
                    databaseCreationRequest.getTenancyType(),
                    databaseCreationRequest.getServiceTag());
        } catch (Exception e) {
            logger.error(String.format("Error creating instance %s", databaseId), e);
            throw new InstanceCreationException(e.getMessage());
        }

        return Optional.of(CreationStep.INSTANCE_READY);
    }

    private String getRealId(String dbInstanceId){
        return dbInstanceId.replace("_", "-");
    }

    private String getClassNameConverter(String requestedClass) { return "db." + requestedClass; }
}
