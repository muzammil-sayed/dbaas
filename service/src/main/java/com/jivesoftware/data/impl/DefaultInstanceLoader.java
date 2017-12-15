package com.jivesoftware.data.impl;

import javax.inject.Inject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.data.DBaaSConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultInstanceLoader {

    private final static Logger LOG = LoggerFactory.getLogger(DefaultInstanceLoader.class);

    private final DBaaSConfiguration dBaaSConfiguration;
    private final List<String> instanceNames;
    private final List<DefaultInstance> instanceList;

    @Inject
    public DefaultInstanceLoader(DBaaSConfiguration dBaaSConfiguration) {

        this.dBaaSConfiguration = dBaaSConfiguration;
        this.instanceNames = dBaaSConfiguration.getDefaultInstances();
        instanceList = load(instanceNames);
    }

    public List<DefaultInstance> getInstanceList() {
        return instanceList;
    }

    private List<DefaultInstance> load(List<String> instanceNames) {

        return ImmutableList.copyOf(instanceNames.stream().map(name -> new DefaultInstance(name,
                dBaaSConfiguration.getInstanceTemplate().getDbInstanceClass(),
                dBaaSConfiguration.getInstanceTemplate().getAllocatedStorage(),
                dBaaSConfiguration.getDefaultDBName())).collect(Collectors.toList()));
    }

    @SuppressWarnings("unused")
    public static class DefaultInstance {

        private final String identifier;
        private final Integer allocatedStorage;
        private final String dbName;
        private final String dbInstanceClass;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public DefaultInstance(@JsonProperty("identifier") String identifier,
                               @JsonProperty("dbInstanceClass") String dbInstanceClass,
                               @JsonProperty("allocatedStorage") int allocatedStorage,
                               @JsonProperty("dbName") String dbName) {
            this.identifier = identifier;
            this.dbInstanceClass = dbInstanceClass;
            this.allocatedStorage = allocatedStorage;
            this.dbName = dbName;
        }

        @JsonProperty(required = true)
        public Integer getAllocatedStorage() {
            return allocatedStorage;
        }

        @JsonProperty(required = true)
        public String getDbName() {
            return dbName;
        }

        @JsonProperty(required = true)
        public String getIdentifier() {
            return identifier;
        }

        @JsonProperty(required = true)
        public String getDbInstanceClass() {
            return dbInstanceClass;
        }

    }
}
