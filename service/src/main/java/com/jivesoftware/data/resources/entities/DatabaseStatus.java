package com.jivesoftware.data.resources.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.Optional;

@ApiModel(description = "A response to database create request and the status of the database")
public class DatabaseStatus {

    private final String databaseId;
    private final Optional<String> message;
    private final Status status;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public DatabaseStatus(@JsonProperty("status") Status status,
                          @JsonProperty("message") String message,
                          @JsonProperty("databaseId") String databaseId) {
        this.status = status;
        this.message = Optional.ofNullable(message);
        this.databaseId = databaseId;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The id assigned to the database", required = true, dataType = "string")
    public String getDatabaseId() {
        return databaseId;
    }

    @JsonProperty(required = false)
    @ApiModelProperty(value = "The message associated with the database creation request such as the error message",
            required = false, dataType = "string")
    public String getMessage() {
        return message.orElse(null);
    }

    @JsonIgnore
    public Optional<String> getMessageOptional() {
        return message;
    }


    @JsonProperty(required = false)
    @ApiModelProperty(value = "The status of the database creation. " +
            " Possible values: READY, CREATING, ERROR, DELETED",
            required = true, dataType = "string")
    public Status getStatus() {
        return status;
    }

    public enum Status{
        READY, CREATING, ERROR, DELETED, DELETING;

        @JsonCreator
        public static Status create(String val) {
            Status[] states = Status.values();
            for (Status state : states) {
                if (state.name().equalsIgnoreCase(val)) {
                    return state;
                }
            }
            return ERROR;
        }
    }

}