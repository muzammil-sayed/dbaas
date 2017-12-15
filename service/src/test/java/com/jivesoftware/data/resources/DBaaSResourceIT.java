package com.jivesoftware.data.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import com.jivesoftware.data.resources.entities.Database;
import com.jivesoftware.data.resources.entities.DatabaseCreateResponse;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import com.jivesoftware.data.resources.entities.DatabaseStatus;
import com.jivesoftware.data.DBaaSConfiguration.InstanceType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

import static com.jayway.restassured.RestAssured.given;

public class DBaaSResourceIT {

    private static final String serviceHost = System.getProperty("serviceHost", "http://localhost:8080");

    private final static Logger logger = LoggerFactory.getLogger(DBaaSResourceIT.class);

    private static final String CREATE_TABLE = "CREATE TABLE Test (" +
            "ID serial PRIMARY KEY," +
            "url VARCHAR (255) NOT NULL," +
            "name VARCHAR (255) NOT NULL," +
            "description VARCHAR (255)," +
            "rel VARCHAR (50))";

    private static final String INSERT_ROW = "INSERT INTO Test (url, name) VALUES (?,?)";

    private static final String SELECT_ROW = "SELECT COUNT(*) as total FROM Test";


    @AfterClass
    public static void cleanUpInstances() {
        given().header("token", "token").post(serviceHost + "/v1/databases/harddelete");
    }


    @Test
    public void testDatabaseNotFoundDatabaseRespondsJson() {
        logger.debug("Starting testDatabaseNotFoundDatabaseRespondsJson");
        given().get(serviceHost + "/v1/databases/lame-id/status").then().contentType(ContentType.JSON).statusCode(404);
    }


    @Test
    public void testCreateSharedDatabaseResource() throws Exception {

        logger.debug("Starting testCreateSharedDatabaseResource");

        String databaseId = "";
        String category = "test";
        String cloneFrom = null;
        try {

            DatabaseCreateResponse databaseCreateResponse = createDatabase(category, cloneFrom,
                    DatabaseCreationRequest.TenancyType.SHARED);

            waitUntilReadyOrError(databaseCreateResponse);

            databaseId = databaseCreateResponse.getDatabaseId();
            Response response = given().when().get(serviceHost + "/v1/databases/" + databaseCreateResponse.getDatabaseId());
            Database database = response.as(Database.class);

            try (Connection connection = getConnection(database, databaseCreateResponse.getPassword())){
                Assert.assertNotNull(connection);
            }

        }
        finally {
            logger.debug(String.format("Clean up: Deleting %s", databaseId));
            given().delete(serviceHost + "/v1/databases/" + databaseId);

        }
    }

    @Ignore //Currently ignoring for deploys, only use locally
    @Test
    public void testCreateDedicatedDatabaseResource() throws Exception {

        logger.debug("Starting testCreateDedicatedDatabaseResource");

        String databaseId = "";
        String category = "test";
        String cloneFrom = null;
        try {

            DatabaseCreateResponse databaseCreateResponse = createDatabase(category, cloneFrom,
                    DatabaseCreationRequest.TenancyType.DEDICATED);

            waitUntilReadyOrError(databaseCreateResponse);

            databaseId = databaseCreateResponse.getDatabaseId();
            Response response = given().when().get(serviceHost + "/v1/databases/" + databaseCreateResponse.getDatabaseId());
            Database database = response.as(Database.class);

            try (Connection connection = getConnection(database, databaseCreateResponse.getPassword())) {
                Assert.assertNotNull(connection);
                try (Statement statement = connection.createStatement();
                     PreparedStatement preparedStatement = connection.prepareStatement(INSERT_ROW)) {
                    statement.executeUpdate(CREATE_TABLE);
                    preparedStatement.setString(1, "https://google.com");
                    preparedStatement.setString(2, "Google");
                    preparedStatement.execute();
                    try (ResultSet rs = statement.executeQuery(SELECT_ROW)) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(1, rs.getInt("total"));
                    }
                }
            }

        }
        finally {
            logger.debug(String.format("Clean up: Deleting %s", databaseId));
            given().delete(serviceHost + "/v1/databases/" + databaseId);

        }
    }


    @Test
    public void testCreateClonedDatabaseNotFound() {

        logger.debug("Starting testCreateClonedDatabaseNotFound");

        DatabaseCreationRequest databaseCreationRequest =
                new DatabaseCreationRequest("test",
                        DatabaseCreationRequest.TenancyType.SHARED,
                        "t2.micro",
                        100,
                        DatabaseCreationRequest.DataLocality.US,
                        "testServiceTag", "testServiceComponentTag",
                        "randomId", false);

        final RequestSpecification request = given().contentType(ContentType.JSON)
                .body(databaseCreationRequest).accept(ContentType.JSON);

        request.when().post(serviceHost + "/v1/databases").then()
                .statusCode(javax.ws.rs.core.Response.Status.NOT_FOUND.getStatusCode());

    }

    @Test
    @Ignore
    public void testCreateClonedSharedDatabase() throws Exception {

        logger.debug("Staring testCreateClonedSharedDatabase");

        String databaseIdFrom = "";
        String databaseIdTo = "";
        String category = "test";
        try {

            DatabaseCreateResponse databaseCreateResponseFrom = createDatabase(category, null,
                    DatabaseCreationRequest.TenancyType.SHARED);
            waitUntilReadyOrError(databaseCreateResponseFrom);
            logger.debug(String.format("Database %s created to clone from", databaseIdFrom));

            databaseIdFrom = databaseCreateResponseFrom.getDatabaseId();

            Response responseFrom = given().when().get(
                    serviceHost + "/v1/databases/" + databaseIdFrom);
            Database databaseFrom = responseFrom.as(Database.class);
            String passwordFrom = databaseCreateResponseFrom.getPassword();

            try (Connection connection = getConnection(databaseFrom, passwordFrom);
                 Statement statement = connection.createStatement();
                 PreparedStatement preparedStatement = connection.prepareStatement(INSERT_ROW)){
                statement.executeUpdate(CREATE_TABLE);
                preparedStatement.setString(1, "https://google.com");
                preparedStatement.setString(2, "Google");
                preparedStatement.execute();
                logger.debug("Test rows inserted");
                try (ResultSet rs = statement.executeQuery(SELECT_ROW)){
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(1, rs.getInt("total"));
                }
            }

            DatabaseCreateResponse databaseCreateResponseTo = createDatabase(category,
                    databaseIdFrom, DatabaseCreationRequest.TenancyType.SHARED);

            waitUntilReadyOrError(databaseCreateResponseTo);
            databaseIdTo = databaseCreateResponseTo.getDatabaseId();
            logger.debug(String.format("Database %s created to clone to", databaseIdTo));
            String passwordTo = databaseCreateResponseTo.getPassword();

            Response responseTo = given().when().get(serviceHost + "/v1/databases/" + databaseIdTo);
            Database databaseTo = responseTo.as(Database.class);

            try (Connection connection = getConnection(databaseTo, passwordTo);
                 Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(SELECT_ROW)){
                Assert.assertTrue(rs.next());
                Assert.assertEquals(1, rs.getInt("total"));
            }
        }
        finally {
            logger.debug(String.format("Clean up: Deleting %s", databaseIdFrom));
            logger.debug(String.format("Clean up: Deleting %s", databaseIdTo));
            given().delete(serviceHost + "/v1/databases/" + databaseIdFrom);
            given().delete(serviceHost + "/v1/databases/" + databaseIdTo);

        }

    }

    @Test(expected = PSQLException.class)
    public void testDeleteSharedDatabaseSuccess() throws Exception {

        logger.debug("Starting testDeleteSharedDatabaseSuccess");

        String category = "test";
        String cloneFrom = null;

        DatabaseCreateResponse databaseCreateResponse =
                createDatabase(category, cloneFrom, DatabaseCreationRequest.TenancyType.SHARED);

        waitUntilReadyOrError(databaseCreateResponse);

        String databaseId = databaseCreateResponse.getDatabaseId();

        Response response = given().when().get(serviceHost + "/v1/databases/" + databaseId);
        Database database = response.as(Database.class);

        try (Connection connection = getConnection(database, databaseCreateResponse.getPassword())){
            Assert.assertNotNull(connection);
        }
        logger.debug(String.format("Created %s, now deleting", databaseId));
        given().delete(serviceHost + "/v1/databases/" + databaseId).then()
                .statusCode(javax.ws.rs.core.Response.Status.NO_CONTENT.getStatusCode());

        try (Connection connection = getConnection(database, databaseCreateResponse.getPassword())){
            Assert.assertNull(connection);
        }
    }

    @Test
    public void testDeleteSharedDatabaseNotFound() throws Exception {

        logger.debug("Starting testDeleteSharedDatabaseNotFound");

        String databaseId = "liesdirtylies";
        given().delete(serviceHost + "/v1/databases/" + databaseId).then()
                .statusCode(javax.ws.rs.core.Response.Status.NOT_FOUND.getStatusCode());

    }

    @Ignore //Currently ignoring for deploys, only use locally
    @Test(expected = PSQLException.class)
    public void testDeleteDedicatedDatabaseSuccess() throws Exception {

        logger.debug("Starting testDeleteDedicatedDatabaseSuccess");

        String category = "test";
        String cloneFrom = null;

        DatabaseCreateResponse databaseCreateResponse = createDatabase(category, cloneFrom,
                DatabaseCreationRequest.TenancyType.DEDICATED);

        waitUntilReadyOrError(databaseCreateResponse);

        String databaseId = databaseCreateResponse.getDatabaseId();
        Response response = given().when()
                .get(serviceHost + "/v1/databases/" + databaseCreateResponse.getDatabaseId());
        Database database = response.as(Database.class);

        try (Connection connection = getConnection(database,
                databaseCreateResponse.getPassword())) {
            Assert.assertNotNull(connection);
        }

        logger.debug(String.format("Created %s, now deleting", databaseId));
        given().delete(serviceHost + "/v1/databases/" + databaseId).then()
                .statusCode(javax.ws.rs.core.Response.Status.NO_CONTENT.getStatusCode());

        waitUntilDeletedOrError(databaseId);

        try (Connection connection = getConnection(database, databaseCreateResponse.getPassword())){
            Assert.assertNull(connection);
        }
    }

    @Ignore //Can't actually test that hard delete worked w/o master password
    @Test
    public void testHardDeleteSharedSuccess() throws Exception {

        logger.debug("Starting testHardDeleteSharedSuccess");

        String category = "test";
        String cloneFrom = null;

        DatabaseCreateResponse databaseCreateResponse =
                createDatabase(category, cloneFrom, DatabaseCreationRequest.TenancyType.SHARED);

        waitUntilReadyOrError(databaseCreateResponse);

        String databaseId = databaseCreateResponse.getDatabaseId();

        Response response = given().when().get(serviceHost + "/v1/databases/" + databaseId);
        Database database = response.as(Database.class);

        try (Connection connection = getConnection(database, databaseCreateResponse.getPassword())){
            Assert.assertNotNull(connection);
        }
        logger.debug(String.format("Created %s, now deleting", databaseId));
        given().delete(serviceHost + "/v1/databases/" + databaseId).then()
                .statusCode(javax.ws.rs.core.Response.Status.NO_CONTENT.getStatusCode());

        try (Connection connection = getConnection(database, databaseCreateResponse.getPassword())){
            Assert.assertNull(connection);
        }

        given().header("token", "token").delete(serviceHost + "/v1/databases/harddelete").then()
                .statusCode(javax.ws.rs.core.Response.Status.NO_CONTENT.getStatusCode());

//        Database masterDatabase = new Database(database.getId(), "postgres", database.getHost(),
//                database.getPort(), database.getSchema());
//
//        try (Connection connection = getConnection(masterDatabase, )){
//            Assert.assertNull(connection);
//        }

        given().get(serviceHost + "/v1/databases/" + databaseId + "/status").then().contentType(ContentType.JSON).statusCode(
                404);
    }

    @Test
    public void testHardDeleteBadToken() throws Exception {

        given().header("token", "notcorrect").delete(serviceHost + "/v1/databases/harddelete").then()
                .statusCode(javax.ws.rs.core.Response.Status.FORBIDDEN.getStatusCode());
    }

    @Test
    public void testHardDeleteNoToken() throws Exception {

        given().delete(serviceHost + "/v1/databases/harddelete").then()
                .statusCode(javax.ws.rs.core.Response.Status.FORBIDDEN.getStatusCode());
    }

    @Test
    public void instanceNotFoundTest() {
        logger.debug("Starting instanceNotFoundTest");
        given().get(serviceHost + "/v1/instances/lame-id").then().contentType(
                ContentType.JSON).statusCode(404);
    }

    //Ignoring for deploys
    @Ignore
    @Test
    public void instanceDetailsTest() throws Exception{

        logger.debug("Starting instanceDetailsTest");

        String databaseId = "";
        String category = "test";
        String cloneFrom = null;

        try {

            DatabaseCreateResponse databaseCreateResponse = createDatabase(category, cloneFrom,
                    DatabaseCreationRequest.TenancyType.DEDICATED);

            waitUntilReadyOrError(databaseCreateResponse);

            databaseId = databaseCreateResponse.getDatabaseId();
            given().get(serviceHost + "/v1/databases/" + databaseId + "/instanceinfo")
                    .then().contentType(ContentType.JSON).statusCode(200);

        }
        finally {
            logger.debug(String.format("Clean up: Deleting %s", databaseId));
            given().delete(serviceHost + "/v1/databases/" + databaseId);

        }

    }




    Connection getConnection(@NotNull Database database, String password) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", database.getUser());
        props.setProperty("password", password);
        return DriverManager.getConnection(String.format("jdbc:postgresql://%s:%d/%s", database.getHost(), database.getPort(), database.getSchema()), props);
    }

    private DatabaseCreateResponse createDatabase(String category, String cloneFrom,
                                                  DatabaseCreationRequest.TenancyType tenancyType) {

        logger.debug(String.format("Creating a database of type %s", tenancyType.toString()));

        DatabaseCreationRequest databaseCreationRequest =
                new DatabaseCreationRequest(category,
                        tenancyType,
                        "t2.micro",
                        100,
                        DatabaseCreationRequest.DataLocality.US,
                        "testServiceTag", "testServiceComponentTag",
                        cloneFrom, false);

        final RequestSpecification createRequest = given().contentType(ContentType.JSON)
                .body(databaseCreationRequest).accept(ContentType.JSON);

        Response response = createRequest.when().post(serviceHost + "/v1/databases");

        return response.as(DatabaseCreateResponse.class);

    }

    private void waitUntilReadyOrError(DatabaseCreateResponse databaseCreationResponse)
     throws Exception{

        Response response = given().get(serviceHost + "/v1/databases/" + databaseCreationResponse.getDatabaseId()
         + "/status");

        DatabaseStatus databaseStatus = response.as(DatabaseStatus.class);

        long waitTime = 1200000;
        while (databaseStatus.getStatus() == DatabaseStatus.Status.CREATING && waitTime > 0) {

            Thread.sleep(10000);

            waitTime = waitTime - 10000;
            response = given().get(serviceHost + "/v1/databases/" + databaseCreationResponse.getDatabaseId()
                    + "/status");

            databaseStatus = response.as(DatabaseStatus.class);

        }

    }

    private void waitUntilDeletedOrError(String databaseId) throws Exception {

        Response response = given().get(serviceHost + "/v1/databases/" + databaseId + "/status");

        DatabaseStatus databaseStatus = response.as(DatabaseStatus.class);

        long waitTime = 600000;
        while (databaseStatus.getStatus() == DatabaseStatus.Status.DELETING && waitTime > 0) {

            Thread.sleep(3000);

            waitTime = waitTime - 3000;
            response = given().get(serviceHost + "/v1/databases/" + databaseId + "/status");

            databaseStatus = response.as(DatabaseStatus.class);

        }
    }

    @Test
    public void testAvailableInstances() throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        String expectedResponse = "[{\"instanceClass\":\"t2.micro\",\"totalRam\":1,\"totalCores\":1,\"prodApproved\":false,\"encryptedAvailable\":false,\"multiAZAvailable\":false},{\"instanceClass\":\"t2.small\",\"totalRam\":2,\"totalCores\":1,\"prodApproved\":false,\"encryptedAvailable\":false,\"multiAZAvailable\":false},{\"instanceClass\":\"t2.medium\",\"totalRam\":4,\"totalCores\":2,\"prodApproved\":false,\"encryptedAvailable\":false,\"multiAZAvailable\":false},{\"instanceClass\":\"m4.large\",\"totalRam\":8,\"totalCores\":2,\"prodApproved\":true,\"encryptedAvailable\":true,\"multiAZAvailable\":true},{\"instanceClass\":\"m4.xlarge\",\"totalRam\":16,\"totalCores\":4,\"prodApproved\":true,\"encryptedAvailable\":true,\"multiAZAvailable\":true},{\"instanceClass\":\"m4.2xlarge\",\"totalRam\":32,\"totalCores\":8,\"prodApproved\":true,\"encryptedAvailable\":true,\"multiAZAvailable\":true},{\"instanceClass\":\"m4.4xlarge\",\"totalRam\":64,\"totalCores\":16,\"prodApproved\":true,\"encryptedAvailable\":true,\"multiAZAvailable\":true},{\"instanceClass\":\"m4.10xlarge\",\"totalRam\":160,\"totalCores\":40,\"prodApproved\":true,\"encryptedAvailable\":true,\"multiAZAvailable\":true},{\"instanceClass\":\"r3.large\",\"totalRam\":15,\"totalCores\":2,\"prodApproved\":true,\"encryptedAvailable\":true,\"multiAZAvailable\":true},{\"instanceClass\":\"r3.xlarge\",\"totalRam\":30,\"totalCores\":4,\"prodApproved\":true,\"encryptedAvailable\":true,\"multiAZAvailable\":true},{\"instanceClass\":\"r3.2xlarge\",\"totalRam\":61,\"totalCores\":8,\"prodApproved\":true,\"encryptedAvailable\":true,\"multiAZAvailable\":true},{\"instanceClass\":\"r3.4xlarge\",\"totalRam\":122,\"totalCores\":16,\"prodApproved\":true,\"encryptedAvailable\":true,\"multiAZAvailable\":true},{\"instanceClass\":\"r3.8xlarge\",\"totalRam\":244,\"totalCores\":32,\"prodApproved\":true,\"encryptedAvailable\":true,\"multiAZAvailable\":true}]";
        Response response = given().contentType(ContentType.JSON).when().get(serviceHost + "/v1/databases/availableinstances");
        InstanceType[] instanceTypes = response.as(InstanceType[].class);
        String arrayToJson = mapper.writeValueAsString(instanceTypes);
        Assert.assertEquals(expectedResponse,arrayToJson);

    }

}


