package com.jivesoftware.data.resources.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description = "A request to create a temporary user for a given databaseId")
public class UserCreateRequest {

    private final String databaseId;

    @JsonCreator
    public UserCreateRequest(@JsonProperty("databaseId") String userName) {

        this.databaseId = userName;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The databaseId for which to create a user", required = true, dataType = "string")
    public String getDatabaseId() {return databaseId;}

}