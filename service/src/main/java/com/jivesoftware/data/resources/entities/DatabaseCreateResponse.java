package com.jivesoftware.data.resources.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description = "A response to database create request")
public class DatabaseCreateResponse {

    private final String databaseId;
    private final String password;

    @JsonCreator
    public DatabaseCreateResponse(@JsonProperty("databaseId") String databaseId,
                                  @JsonProperty("password") String password) {

        this.databaseId = databaseId;
        this.password = password;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The id assigned to the database", required = true, dataType = "string")
    public String getDatabaseId() {
        return databaseId;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The database password", required = true, dataType = "string")
    public String getPassword() {
        return password;
    }

}
