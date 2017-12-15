package com.jivesoftware.data.resources.cdc;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import com.jivesoftware.data.resources.entities.Database;
import com.jivesoftware.data.resources.entities.DatabaseCreateResponse;
import com.jivesoftware.data.resources.entities.DatabaseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static com.jayway.restassured.RestAssured.given;

public class GSONJSONObjectCreationIT {

    private static final String serviceHost = System.getProperty("serviceHost", "http://localhost:8080");

    private final static Logger logger = LoggerFactory.getLogger(GSONJSONObjectCreationIT.class);

    @AfterClass
    public static void cleanUpInstances() {
        given().header("token", "token").post(serviceHost + "/v1/databases/harddelete");
    }

    @Test
    public void testGSONtoJsonObjectForCreation() throws Exception {

        logger.debug("Starting testJsonObjectForCreation");

        String databaseId = "";

        try {

            JsonObject postBodyJson = new JsonObject();
            postBodyJson.addProperty("category", "test");
            postBodyJson.addProperty("tenancyType", "SHARED");
            postBodyJson.addProperty("dataLocality", "US");
            postBodyJson.addProperty("serviceTag", "testServiceTag");
            postBodyJson.addProperty("serviceComponentTag", "testServiceComponentTag");
            postBodyJson.addProperty("highlyAvailable", false);
            String createPostBodyJsonGSON = new Gson().toJson(postBodyJson);

            final RequestSpecification createRequest = given().contentType(ContentType.JSON)
                    .body(createPostBodyJsonGSON).accept(ContentType.JSON);

            Response postResponse = createRequest.when().post(serviceHost + "/v1/databases");

            DatabaseCreateResponse databaseCreateResponse = postResponse.as(DatabaseCreateResponse.class);

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

    @Test
    public void testJsonStringForCreation() throws Exception {

        logger.debug("Starting testJsonObjectForCreation");

        String databaseId = "";

        try {

            String createPostBodyJson = "{\"category\":\"test\",\"tenancyType\":\"shared\",\"dataLocality\":\"US\",\"serviceTag\":\"testServiceTag\",\"serviceComponentTag\":\"testServiceComponentTag\",\"highlyAvailable\":false}";

            final RequestSpecification createRequest = given().contentType(ContentType.JSON)
                    .body(createPostBodyJson).accept(ContentType.JSON);

            Response postResponse = createRequest.when().post(serviceHost + "/v1/databases");

            DatabaseCreateResponse databaseCreateResponse = postResponse.as(DatabaseCreateResponse.class);

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

    Connection getConnection(@NotNull Database database, String password) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", database.getUser());
        props.setProperty("password", password);
        return DriverManager.getConnection(
                String.format("jdbc:postgresql://%s:%d/%s", database.getHost(), database.getPort(),
                        database.getSchema()), props);
    }

    private void waitUntilReadyOrError(DatabaseCreateResponse databaseCreationResponse)
            throws Exception{

        Response response = given().get(serviceHost + "/v1/databases/" + databaseCreationResponse.getDatabaseId()
                + "/status");

        DatabaseStatus databaseStatus = response.as(DatabaseStatus.class);

        long waitTime = 600000;
        while (databaseStatus.getStatus() == DatabaseStatus.Status.CREATING && waitTime > 0) {

            Thread.sleep(3000);

            waitTime = waitTime - 3000;
            response = given().get(serviceHost + "/v1/databases/" + databaseCreationResponse.getDatabaseId()
                    + "/status");

            databaseStatus = response.as(DatabaseStatus.class);

        }
    }
}
