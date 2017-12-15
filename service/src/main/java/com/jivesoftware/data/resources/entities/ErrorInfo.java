package com.jivesoftware.data.resources.entities;


import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description = "Error information")
public class ErrorInfo {

    private final String message;

    public ErrorInfo(String message) {
        this.message = message;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The error description", required = true, dataType = "string")
    public String getMessage() {
        return message;
    }
}
