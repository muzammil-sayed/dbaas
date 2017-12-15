package com.jivesoftware.data.resources;

import com.google.gson.JsonObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.jayway.restassured.RestAssured.given;

public class RequestValidationIT {

    private static final String serviceHost = System.getProperty("serviceHost", "http://localhost:8080");

    private final static Logger logger = LoggerFactory.getLogger(RequestValidationIT.class);

    //    "{"category":"test","tenancyType":"shared","instanceClass":"m4.large",
    //    "instanceStorage":"100","dataLocality":"US","serviceTag":"testServiceTag",
    //    "serviceComponentTag":"testServiceComponentTag","highlyAvailable":false}"

    @Test
    public void testInstanceClassNotValid() {
        logger.debug("Starting testInstanceClassNotValid");

        JsonObject request = new JsonObject();

        request.addProperty("category", "test");
        request.addProperty("tenancyType", "dedicated");
        request.addProperty("instanceClass", "blah");
        request.addProperty("dataLocality", "US");
        request.addProperty("serviceTag", "testServiceTag");
        request.addProperty("serviceComponentTag", "testServiceComponentTag");
        request.addProperty("highlyAvailable", false);

        given().contentType("application/json")
                .body(request.toString())
                .when()
                .post(serviceHost + "/v1/databases/")
                .then().statusCode(422);

    }

    @Test
    public void testInstanceStorageNotValid() {
        logger.debug("Starting testInstanceStorageNotValid");

        JsonObject request = new JsonObject();

        request.addProperty("category", "test");
        request.addProperty("tenancyType", "dedicated");
        request.addProperty("instanceClass", "t2.small");
        request.addProperty("instanceStorage", "90");
        request.addProperty("dataLocality", "US");
        request.addProperty("serviceTag", "testServiceTag");
        request.addProperty("serviceComponentTag", "testServiceComponentTag");
        request.addProperty("highlyAvailable", false);

        given().contentType("application/json")
                .body(request.toString())
                .when()
                .post(serviceHost + "/v1/databases/")
                .then().statusCode(422);

    }

    @Test
    public void testHighAvailabilityNotAvailable() {
        logger.debug("Starting testHighAvailabilityNotAvailable");

        JsonObject request = new JsonObject();

        request.addProperty("category", "test");
        request.addProperty("tenancyType", "dedicated");
        request.addProperty("instanceClass", "t2.small");
        request.addProperty("dataLocality", "US");
        request.addProperty("serviceTag", "testServiceTag");
        request.addProperty("serviceComponentTag", "testServiceComponentTag");
        request.addProperty("highlyAvailable", true);

        given().contentType("application/json")
                .body(request.toString())
                .when()
                .post(serviceHost + "/v1/databases/")
                .then().statusCode(422);
    }
}
