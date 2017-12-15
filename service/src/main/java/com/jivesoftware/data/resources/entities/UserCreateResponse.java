package com.jivesoftware.data.resources.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description = "A response to a temp user create request")
public class UserCreateResponse {

    private final String userName;
    private final String password;

    @JsonCreator
    public UserCreateResponse(@JsonProperty("UserName") String userName,
                                  @JsonProperty("password") String password) {

        this.userName = userName;
        this.password = password;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The username created", required = true, dataType = "string")
    public String getUserName() {
        return userName;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The password for the user", required = true, dataType = "string")
    public String getPassword() {
        return password;
    }

}
