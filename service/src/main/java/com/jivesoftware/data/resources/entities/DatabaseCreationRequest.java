package com.jivesoftware.data.resources.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.data.resources.entities.jersey_validators.ValidMinimumStorage;
import com.jivesoftware.data.resources.entities.jersey_validators.ClassValue;
import com.jivesoftware.data.resources.entities.jersey_validators.ClassAllowedMultiAZ;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Optional;

@SuppressWarnings("unused")
@ApiModel(description = "A request to create a database")
@ClassAllowedMultiAZ(message = "Multi-AZ deployments are not allowed for the selected class size")
@XmlRootElement(name = "DatabaseCreationRequest")
public class DatabaseCreationRequest {

    private static final String DEFAULTINSTANCECLASS = "m4.large";
    private static final Integer DEFAULTINSTANCESTORAGE = 100;

    private final String category;
    private final TenancyType tenancyType;
    private final DataLocality dataLocality;
    private final String serviceTag;
    private final String serviceComponentTag;
    private final Optional<String> sourceDatabaseId;
    private final Boolean highlyAvailable;

    @ClassValue(message = "This is not a valid RDS class type")
    private final Optional<String> instanceClass;
    @ValidMinimumStorage(message = "The instance storage needs to be at least 100gb")
    private final Optional<Integer> instanceStorage;

    //"{"category":"test","tenancyType":"shared","dataLocality":"US","serviceTag":"testServiceTag",
    // "serviceComponentTag":"testServiceComponentTag","highlyAvailable":false}"

    //"{"category":"test","tenancyType":"shared","instanceClass":"m4.large","dataLocality":"US",
    // "serviceTag":"testServiceTag","serviceComponentTag":"testServiceComponentTag","highlyAvailable":false}"

    //"{"category":"test","tenancyType":"shared","instanceClass":"m4.large",
    // "instanceStorage":"100","dataLocality":"US","serviceTag":"testServiceTag",
    // "serviceComponentTag":"testServiceComponentTag","highlyAvailable":false}"

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public DatabaseCreationRequest(@JsonProperty("category") String category,
                                   @JsonProperty("tenancyType")  TenancyType tenancyType,
                                   @JsonProperty("instanceClass") String instanceClass,
                                   @JsonProperty("instanceStorage") Integer instanceStorage,
                                   @JsonProperty("dataLocality")  DataLocality dataLocality,
                                   @JsonProperty("serviceTag") String serviceTag,
                                   @JsonProperty("serviceComponentTag") String serviceComponentTag,
                                   @JsonProperty("sourceDatabaseId") String sourceDatabaseId,
                                   @JsonProperty("highlyAvailable") Boolean highlyAvailable) {
        this.category = category;
        this.tenancyType = tenancyType;
        this.instanceClass = Optional.ofNullable(instanceClass);
        this.instanceStorage = Optional.ofNullable(instanceStorage);
        this.dataLocality = dataLocality;
        this.serviceTag = serviceTag;
        this.serviceComponentTag = serviceComponentTag;
        this.sourceDatabaseId = Optional.ofNullable(sourceDatabaseId);
        this.highlyAvailable = highlyAvailable;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "Category for the database being created", required = true, dataType = "string")
    public String getCategory() {
        return category;
    }

    @JsonProperty(required = false, defaultValue = "SHARED")
    @ApiModelProperty(value = "The type of tenancy - SHARED for co-locating databases on a shared " +
            "instances, DEDICATED for a dedicated database", required = false, dataType = "string")
    public TenancyType getTenancyType() {
        return tenancyType;
    }

    @JsonProperty(required = false, defaultValue = "US")
    @ApiModelProperty(value = "The geo locality of the database " +
            "current supported values: US, EU", required = false, dataType = "string")
    public DataLocality getDataLocality() {
        return dataLocality;
    }

    @JsonProperty(required = false)
    @ApiModelProperty(value = "The id of the database to clone on on creation", required = false, dataType = "string")
    public String getSourceDatabaseId() {
        return sourceDatabaseId.orElse(null);
    }

    @JsonIgnore
    public Optional<String> getSourceDatabaseIdOptional() {
        return sourceDatabaseId;
    }

    @JsonProperty(required = false, defaultValue = "m4.large")
    @ApiModelProperty(value = "The class of dedicated instance to create- " +
            "Check endpoint /allowedClasses to see specifics", required = false, dataType = "string")
    public String getInstanceClass() { return instanceClass.orElse(DEFAULTINSTANCECLASS); }

    @JsonIgnore
    public Optional<String> getInstanceClassOptional() { return instanceClass; }

    @JsonProperty(required = false, defaultValue = "100")
    @ApiModelProperty(value = "The amount of storage to allocate for a new dedicated instance",
            required = false, allowableValues = "range[100, infinity]", dataType = "integer")
    public Integer getInstanceStorage() { return instanceStorage.orElse(DEFAULTINSTANCESTORAGE); }

    @JsonIgnore
    public Optional<Integer> getInstanceStorageOptional() { return instanceStorage; }

    @JsonProperty(required = false, defaultValue = "false")
    @ApiModelProperty(value = "Whether or not the database is highly available - false by default",
            required = false, dataType = "boolean")
    public Boolean getHighlyAvailable() {
        return highlyAvailable;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "Service tag asking for database being created", required = true, dataType = "string")
    public String getServiceTag() {
        return serviceTag;
    }

    @JsonProperty(required = true)
    @ApiModelProperty(value = "Service component tag asking for database being created",
            required = true, dataType = "string")
    public String getServiceComponentTag() {
        return serviceComponentTag;
    }

    public enum TenancyType {
        SHARED, DEDICATED;

        @JsonCreator
        @SuppressWarnings("unused")
        public static TenancyType create(String val) {
            TenancyType[] states = TenancyType.values();
            for (TenancyType state : states) {
                if (state.name().equalsIgnoreCase(val)) {
                    return state;
                }
            }
            return SHARED;
        }
    }

    public enum DataLocality {
        US, EU;

        @JsonCreator
        @SuppressWarnings("unused")
        public static DataLocality create(String val) {
            DataLocality[] states = DataLocality.values();
            for (DataLocality state : states) {
                if (state.name().equalsIgnoreCase(val)) {
                    return state;
                }
            }
            return US;
        }
    }

}
