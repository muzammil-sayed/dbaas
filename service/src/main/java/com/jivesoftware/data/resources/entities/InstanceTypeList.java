package com.jivesoftware.data.resources.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import com.jivesoftware.data.DBaaSConfiguration.InstanceType;

import java.util.List;

public class InstanceTypeList {

    @JsonProperty
    private final List<InstanceType> instanceTypes;

    @JsonCreator
    public InstanceTypeList(@JsonProperty("instanceTypes") List<InstanceType> instanceTypes) {
        this.instanceTypes = instanceTypes;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "The list of available instance types", required = true, dataType = "List")
    public List<InstanceType> getInstanceTypes() {
        return instanceTypes;
    }

}
