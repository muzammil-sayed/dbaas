package com.jivesoftware.data.impl;

import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.DBaaSConfiguration;
import org.apache.commons.lang3.RandomStringUtils;

import javax.inject.Inject;

public class DatabaseIDHelper {

    private static final String IDENTIFIER_FORMAT = "%s-%s";
    private static final String DATABASE_ID_FORMAT = "%s_%s";
    private static final int INSTANCE_IDENTIFIER_LIMIT = 63;
    private static final int MAXIMUM_UNIQUE_LENGTH = 8;
    private static final int MINIMUM_UNIQUE_LENGTH = 4;
    private static final int EXTRA_BUFFER_LENGTH = 2;
    private final DBaaSConfiguration dBaaSConfiguration;

    @Inject
    public DatabaseIDHelper(DBaaSConfiguration dBaaSConfiguration) {
        this.dBaaSConfiguration = dBaaSConfiguration;
    }

    public String generateDatabaseId(String category) {
        String replaceCategory = category.replace("-", "_");
        return affixUniqueIdentifier(replaceCategory);
    }

    public String getDBInstanceId(String identifier) {
        identifier = identifier.replace("_", "-");
        return String.format(IDENTIFIER_FORMAT, dBaaSConfiguration.getInstanceIdentifierPrefix(), identifier);
    }

    public String getDatabaseInstanceId(DBInstance dbInstance) {
        String realInstanceId = dbInstance.getDBInstanceIdentifier();
        return realInstanceId.substring(dBaaSConfiguration.getInstanceIdentifierPrefix().length() + 1,
                        realInstanceId.length());
    }

    private int checkAllowedLength(String name) {
        int totalLength = name.length() + dBaaSConfiguration.getInstanceIdentifierPrefix().length()
                + MINIMUM_UNIQUE_LENGTH;
        return INSTANCE_IDENTIFIER_LIMIT - totalLength;
    }

    private String affixUniqueIdentifier(String submittedName) {
        int bufferLength = checkAllowedLength(submittedName);
        if(bufferLength < 0){
            submittedName = submittedName.substring(0,
                    (submittedName.length() - (Math.abs(bufferLength) + EXTRA_BUFFER_LENGTH)));

            return String.format(DATABASE_ID_FORMAT, submittedName,
                    RandomStringUtils.randomAlphanumeric(MINIMUM_UNIQUE_LENGTH).toLowerCase());
        }
        else if(bufferLength <= MINIMUM_UNIQUE_LENGTH){
            int pareSize = MINIMUM_UNIQUE_LENGTH - bufferLength;
            submittedName = submittedName.substring(0, (submittedName.length() -
                    (pareSize)));

            return String.format(DATABASE_ID_FORMAT, submittedName,
                    RandomStringUtils.randomAlphanumeric(MINIMUM_UNIQUE_LENGTH).toLowerCase());
        }
        else {
            return String.format(
                    DATABASE_ID_FORMAT, submittedName,
                    RandomStringUtils.randomAlphanumeric((
                            (bufferLength - EXTRA_BUFFER_LENGTH < MAXIMUM_UNIQUE_LENGTH) ?
                                    (bufferLength - EXTRA_BUFFER_LENGTH) :
                                    (MAXIMUM_UNIQUE_LENGTH))).toLowerCase());
        }
    }

}
