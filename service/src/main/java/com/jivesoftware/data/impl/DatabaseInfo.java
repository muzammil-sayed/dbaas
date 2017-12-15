package com.jivesoftware.data.impl;


import com.jivesoftware.data.resources.entities.Database;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;

import javax.annotation.Nonnull;

class DatabaseInfo {

    private final Database database;
    private final String instanceId;
    private final DatabaseCreationRequest.TenancyType tenancyType;
    private final DatabaseCreationRequest.DataLocality dataLocality;

    public DatabaseInfo(Database database, String instanceId, DatabaseCreationRequest.TenancyType tenancyType,
                        DatabaseCreationRequest.DataLocality dataLocality) {
        this.database = database;
        this.instanceId = instanceId;
        this.tenancyType = tenancyType;
        this.dataLocality = dataLocality;
    }

    @Nonnull
    public Database getDatabase() {
        return database;
    }

    @Nonnull
    public String getInstanceId() {
        return instanceId;
    }

    @Nonnull
    public DatabaseCreationRequest.TenancyType getTenancyType() {
        return tenancyType;
    }

    @Nonnull
    public DatabaseCreationRequest.DataLocality getDataLocality() {
        return dataLocality;
    }
}
