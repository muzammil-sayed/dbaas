package com.jivesoftware.data.resources.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description = "Database information")
public class Database {

    private final String id;
    private final String user;
    private final String host;
    private final Integer port;
    private final String schema;

    @JsonCreator
    public Database(@JsonProperty("id") String id,
                    @JsonProperty("user") String user,
                    @JsonProperty("host") String host,
                    @JsonProperty("port") Integer port,
                    @JsonProperty("schema") String schema) {
        this.id = id;
        this.user = user;
        this.host = host;
        this.port = port;
        this.schema = schema;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The database id", required = true, dataType = "string")
    public String getId() {
        return id;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The database user", required = true, dataType = "string")
    public String getUser() {
        return user;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The database host", required = true, dataType = "string")
    public String getHost() {
        return host;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The database schema", required = true, dataType = "string")
    public String getSchema() {
        return schema;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The database port", required = true, dataType = "int")
    public Integer getPort() {
        return port;
    }

}
