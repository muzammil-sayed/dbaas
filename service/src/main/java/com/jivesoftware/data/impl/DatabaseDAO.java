package com.jivesoftware.data.impl;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.resources.entities.Database;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import com.jivesoftware.data.resources.entities.DatabaseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import java.util.Optional;
import java.util.*;

public class DatabaseDAO {

    private final static Logger LOG = LoggerFactory.getLogger(DatabaseDAO.class);

    private static final String MANAGED_BY_SERVICE_COMPONENT_TAG_KEY = "managed_by_service";
    private static final String MAKO_ENVIRONMENT = "mako-environment";

    private final AmazonDynamoDBClient client;
    private final DBaaSConfiguration dBaaSConfiguration;
    private final DBaaSConfiguration.DatabaseDaoConfiguration databaseDaoConfiguration;

    @Inject
    public DatabaseDAO(AmazonDynamoDBClient client, DBaaSConfiguration dbaasConfig) {
        this.client = client;
        this.dBaaSConfiguration = dbaasConfig;
        this.databaseDaoConfiguration = dbaasConfig.getDatabaseDaoConfiguration();
        Table table;
        try {
            DynamoDB dynamoDB = makeDynamo();
            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(databaseDaoConfiguration.getTableName())
                    .withKeySchema(ImmutableList.of(new KeySchemaElement()
                            .withAttributeName("databaseId").withKeyType(KeyType.HASH)))
                    .withAttributeDefinitions(ImmutableList.of(
                            new AttributeDefinition().withAttributeName("databaseId")
                                    .withAttributeType("S")
                    ))
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(databaseDaoConfiguration.getReadUnits())
                            .withWriteCapacityUnits(databaseDaoConfiguration.getWriteUnits()));

            table = dynamoDB.createTable(request);
        } catch (ResourceInUseException e) {
            // wait to become active
           table = getDatabaseTable();
        }

        try {
            table.waitForActive();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        TableDescription tableDescription = table.getDescription();
        String tableARN = tableDescription.getTableArn();
        ListTagsOfResourceRequest tagQueryRequest = new ListTagsOfResourceRequest().withResourceArn(tableARN);
        if(!client.listTagsOfResource(tagQueryRequest).getTags().removeAll(getTags())) {
            LOG.debug(String.format("Adding tags to table %s", tableDescription.getTableName()));
            client.tagResource(new TagResourceRequest().withResourceArn(tableARN).withTags(getTags()));
        }

        List<GlobalSecondaryIndexDescription> indexList = client.describeTable(
                databaseDaoConfiguration.getTableName()).getTable().getGlobalSecondaryIndexes();
        boolean indexExists = false;

        if(indexList == null){
            LOG.debug("Global secondary index 'dbStatus' is null");
            indexList = ImmutableList.of();
        }

        for (GlobalSecondaryIndexDescription index : indexList) {
            if(index.getIndexName().equals("dbStatus")){
                indexExists = true;
            }
        }

        if(!indexExists){
            Index index = table.createGSI(new CreateGlobalSecondaryIndexAction()
                            .withIndexName("dbStatus")
                            .withKeySchema(new KeySchemaElement("dbStatus", KeyType.HASH))
                            .withProvisionedThroughput(new ProvisionedThroughput(25l, 25l))
                            .withProjection(new Projection()
                                    .withProjectionType(ProjectionType.ALL)),
                    new AttributeDefinition("dbStatus", ScalarAttributeType.S));

            try {
                index.waitForActive();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

    }

    public Optional<DatabaseInfo> getDatabaseInfo(String databaseId) {

        GetItemSpec spec = new GetItemSpec()
                .withPrimaryKey("databaseId", databaseId)
                .withProjectionExpression("host,username,port,schemaName,dbStatus,instanceId,tenancyType,dataLocality")
                .withConsistentRead(true);

        Item item = getDatabaseTable().getItem(spec);

        if (item == null) {
            return Optional.empty();
        }

        return Optional.of(
                new DatabaseInfo(
                    new Database(
                        databaseId,
                        item.getString("username"),
                        item.getString("host"),
                        item.getInt("port"),
                        item.getString("schemaName")
                    ),
                    item.getString("instanceId"),
                    DatabaseCreationRequest.TenancyType.create(item.getString("tenancyType")),
                    DatabaseCreationRequest.DataLocality.create(item.getString("dataLocality"))));
    }

    public Optional<DatabaseStatus> getDatabaseStatus(String databaseId) {

        GetItemSpec spec = new GetItemSpec()
                .withPrimaryKey("databaseId", databaseId)
                .withProjectionExpression("dbStatus,message")
                .withConsistentRead(true);

        Item item = getDatabaseTable().getItem(spec);

        if (item == null) {
            return Optional.empty();
        }

        return Optional.of(new DatabaseStatus(
                DatabaseStatus.Status.valueOf(item.getString("dbStatus")),
                (item.getString("message")), databaseId
        ));
    }


    public void updateStatus(DatabaseStatus databaseStatus) {

        PrimaryKey primaryKey = new PrimaryKey("databaseId", databaseStatus.getDatabaseId());

        long currentTime = System.currentTimeMillis();

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey(primaryKey)
                .withUpdateExpression("SET #S = :dbStatus, #M = :message, #T = :updateTime")
                .withNameMap(ImmutableMap.of("#S", "dbStatus", "#M", "message", "#T", "updateTime"))
                .withValueMap(ImmutableMap.of(":dbStatus", databaseStatus.getStatus().name(),
                        ":message", databaseStatus.getMessageOptional().orElse("message"),
                        ":updateTime", currentTime))
                .withReturnValues(ReturnValue.NONE);
        getDatabaseTable().updateItem(updateItemSpec);
    }


    public void putDatabase(@NotNull String databaseId,
                            @NotNull String instanceId,
                            @NotNull Database database,
                            @NotNull DatabaseCreationRequest.DataLocality dataLocality,
                            @NotNull DatabaseCreationRequest.TenancyType tenancyType,
                            @NotNull String serviceTag) {

        PrimaryKey primaryKey = new PrimaryKey("databaseId", databaseId);

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey(primaryKey)
                .withUpdateExpression("SET #H = :host, #U = :username, #PO = :port, #ST = :serviceTag, " +
                        "#SN = :schemaName, #I = :instanceId, #TT = :tenancyType, #DL = :dataLocality")
                .withNameMap(new ImmutableMap.Builder<String, String>()
                        .put("#H", "host")
                        .put("#U", "username")
                        .put("#PO", "port")
                        .put("#SN", "schemaName")
                        .put("#I", "instanceId")
                        .put("#TT", "tenancyType")
                        .put("#DL", "dataLocality")
                        .put("#ST", "serviceTag")
                        .build())
                .withValueMap(new ImmutableMap.Builder<String, Object>()
                        .put(":host", database.getHost())
                        .put(":username", database.getUser())
                        .put(":port", database.getPort())
                        .put(":schemaName", database.getSchema())
                        .put(":instanceId", instanceId)
                        .put(":tenancyType", tenancyType.name())
                        .put(":dataLocality", dataLocality.name())
                        .put(":serviceTag", serviceTag)
                        .build())
                .withReturnValues(ReturnValue.NONE);
        UpdateItemOutcome outcome = getDatabaseTable().updateItem(updateItemSpec);

        LOG.debug(String.format("Created a record for the database: %s", databaseId));


    }

    public List<Database> getDeletedDatabases() {

        List<Database> deletables = new ArrayList<>();
        long currentEpoch = System.currentTimeMillis();
        long fifteenMinutes = dBaaSConfiguration.getHardDeleteDelay();

        Table table = getDatabaseTable();
        Index index = table.getIndex("dbStatus");

        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression("dbStatus = :deleted")
                .withValueMap(new ValueMap()
                    .withString(":deleted", "DELETED"))
                .withProjectionExpression("databaseId,host,username,port,schemaName,dbStatus,updateTime");

        ItemCollection<QueryOutcome> deletedDatabases = index.query(spec);

        for(Item item : deletedDatabases) {

            if(item.getLong("updateTime") < (currentEpoch - fifteenMinutes)){

                Database deletedDB = new Database(item.getString("databaseId"),
                        item.getString("username"),
                        item.getString("host"),
                        item.getInt("port"),
                        item.getString("schemaName")
                );
                LOG.debug(String.format("%s set to be deleted in hard delete", item.getString("schemaName")));
                deletables.add(deletedDB);
            } else {
                LOG.debug(String.format("%s was deleted too recently to be reaped by hard delete", item.getString("schemaName")));
            }

        }

        return deletables;

    }

    public void deleteDatabaseRecord(String databaseId) {

        Table table = getDatabaseTable();
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("databaseId", new AttributeValue().withS(databaseId));
        DeleteItemRequest deleteRequest = new DeleteItemRequest()
                .withTableName(table.getTableName())
                .withKey(key);

        LOG.debug(String.format("Attempting to delete item %s from Dynamo", databaseId));

        client.deleteItem(deleteRequest);
    }


    Table getDatabaseTable() {
        DynamoDB dynamoDB = makeDynamo();

        String tableName = databaseDaoConfiguration.getTableName();

        return dynamoDB.getTable(tableName);
    }

    private ImmutableSet<Tag> getTags() {
        ImmutableSet<Tag> tags = new ImmutableSet.Builder<Tag>()
                .add(new Tag().withKey(MAKO_ENVIRONMENT)
                        .withValue(dBaaSConfiguration.getMakoEnvironment()))
                .add(new Tag().withKey(MANAGED_BY_SERVICE_COMPONENT_TAG_KEY)
                        .withValue(dBaaSConfiguration.getManagedServiceTag()))
                .build();
        return tags;
    }

    DynamoDB makeDynamo() {
        return new DynamoDB(client);
    }
}
