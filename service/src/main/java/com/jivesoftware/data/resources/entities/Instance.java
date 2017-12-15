package com.jivesoftware.data.resources.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description = "Instance details")
public class Instance {

    private final String instanceIdentifier;
    private final String instanceClass;
    private final String totalRam;
    private final Integer totalCores;
    private final String storage;
    private final String dbName;
    private final String username;
    private final String endpoint;
    private final Integer port;
    private final String availabilityZone;
    private final String instanceStatus;

    @JsonCreator
    public Instance(@JsonProperty("instanceIdentifier") String instanceIdentifier,
                    @JsonProperty("instanceClass") String instanceClass,
                    @JsonProperty("totalRam") String totalRam,
                    @JsonProperty("totalCores") Integer totalCores,
                    @JsonProperty("storage") String storage,
                    @JsonProperty("dbName") String dbName,
                    @JsonProperty("username") String username,
                    @JsonProperty("endpoint") String endpoint,
                    @JsonProperty("port") Integer port,
                    @JsonProperty("availabilityZone") String availabilityZone,
                    @JsonProperty("instanceStatus") String instanceStatus) {

        this.instanceIdentifier = instanceIdentifier;
        this.instanceClass = instanceClass;
        this.totalRam = totalRam;
        this.totalCores = totalCores;
        this.storage = storage;
        this.dbName = dbName;
        this.username = username;
        this.endpoint = endpoint;
        this.port = port;
        this.availabilityZone = availabilityZone;
        this.instanceStatus = instanceStatus;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The instance id", required = true, dataType = "string")
    public String getInstanceIdentifier() { return instanceIdentifier; }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The instance class", required = true, dataType = "string")
    public String getInstanceClass() { return instanceClass; }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The amount of RAM available on this instance", required = true, dataType = "integer")
    public String getTotalRam() { return totalRam; }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The number of cores this instance uses", required = true, dataType = "integer")
    public Integer getTotalCores() { return totalCores; }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The instance storage", required = true, dataType = "string")
    public String getStorage() { return storage; }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The database name", required = true, dataType = "string")
    public String getDbName() { return dbName; }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The database user", required = true, dataType = "string")
    public String getUsername() { return username; }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The instance endpoint", required = true, dataType = "string")
    public String getEndpoint() { return endpoint; }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The port the database runs on", required = true, dataType = "integer")
    public Integer getPort() { return port; }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The instance availability zone", required = true, dataType = "string")
    public String getAvailabilityZone() { return availabilityZone; }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The instance db status", required = true, dataType = "string")
    public String getInstanceStatus() { return instanceStatus; }

}
