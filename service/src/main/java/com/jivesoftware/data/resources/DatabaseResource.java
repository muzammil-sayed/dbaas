package com.jivesoftware.data.resources;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.*;
import com.amazonaws.services.rds.model.Tag;
import com.codahale.metrics.annotation.Timed;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.DBaaSConfiguration.InstanceType;
import com.jivesoftware.data.impl.DatabaseManager;
import com.jivesoftware.data.impl.InstanceManager;
import com.jivesoftware.data.impl.URLHelper;
import com.jivesoftware.data.resources.entities.*;
import io.swagger.annotations.*;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

@Api(value = "database", description = "Operations on databases")
@Path("v1/databases")
public class DatabaseResource {

    private final DatabaseManager databaseManager;
    private final URLHelper urlHelper;
    private final InstanceManager instanceManager;
    private final DBaaSConfiguration dBaaSConfiguration;

    @Inject
    public DatabaseResource(DatabaseManager databaseManager,
                            URLHelper urlHelper,
                            InstanceManager instanceManager,
                            DBaaSConfiguration dBaaSConfiguration) {
        this.databaseManager = databaseManager;
        this.urlHelper = urlHelper;
        this.instanceManager = instanceManager;
        this.dBaaSConfiguration = dBaaSConfiguration;
    }

    @ApiOperation(value = "Requesting a database creation",
            notes = "Request to create a database in either dedicated or shared instance and " +
                    "possible to clone an existing database into it. Response should be immediate, " +
                    "but it appears some lag has developed in the network or response. Will dig " +
                    "into this, in the meanwhile please check /v1/databases/{databaseId}/status " +
                    "before retrying.",
            response = DatabaseCreateResponse.class)
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Successfully requested a database to be created",
                    response = DatabaseCreateResponse.class),
            @ApiResponse(code = 404, message = "The database with the clone database id does " +
                    "not exist"),
            @ApiResponse(code = 422, message = "The request is faulty- this is returned if " +
                    "the instance class specified is not valid, the allocated storage is too low, " +
                    "or high availability was requested on an instance class that does not support it"),
            @ApiResponse(code = 503, message = "There is no capacity on existing instances to " +
                    "create a shared instance")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @POST
    @Timed
    public Response createDatabase(@Valid
                                       @ApiParam(value = "DatabaseCreationRequest object",
                                               required = true)
                                       DatabaseCreationRequest databaseCreationRequest) {
        DatabaseCreateResponse createResponse =
                databaseManager.createDatabase(databaseCreationRequest);
        URI url = urlHelper.buildURL(createResponse.getDatabaseId()).get();

        return Response.created(url).entity(createResponse).build();
    }

    @ApiOperation(value = "Status of a database",
            notes = "Request to get the status of a database which can tell whether" +
                    " it's READY, CREATING, ERROR. Do note that ERROR will be reported but in the " +
                    "case of SchemaExceptions DBAAS will retry and usually succeed",
            response = DatabaseStatus.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved status of the database",
                    response = DatabaseStatus.class),
            @ApiResponse(code = 404, message = "The database with specified id does not exist")
    })
    @Path("{databaseId}/status")
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatabaseStatus(@PathParam("databaseId") String databaseId) {
        return Response.ok().entity(databaseManager.getDatabaseStatus(databaseId)).build();
    }

    @ApiOperation(value = "Responds with 200",
            notes = "Used as status check by JCX")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The dbaas service is up")
    })
    @Path("dbaasStatusCheck")
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response dbaasStatusCheck() {
        return Response.status(200).build();
    }


    @ApiOperation(value = "Get the database information",
            notes = "Get the information for the database to connect to",
            response = Database.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved the database connection information",
                    response = Database.class),
            @ApiResponse(code = 404, message = "The database with id does not exist")
    })
    @Path("{databaseId}")
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatabase(@PathParam("databaseId") String databaseId) {
        return Response.ok().entity(databaseManager.getDatabase(databaseId)).build();
    }

    @ApiOperation(value = "Delete the specified database",
            notes = "Deletes the database identified in the path parameter",
            response = DatabaseCreateResponse.class)
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Successfully deleted the database by id or " +
                    "database is not found."),
    })
    @DELETE
    @Timed
    @Path("{databaseId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteDatabase(@PathParam("databaseId") String databaseId) {
        databaseManager.deleteDatabase(databaseId);
        return Response.status(204).build();
    }

    @ApiOperation(value = "Creates a temp user for magic query",
            notes = "Creates a temp user for magic query",
            response = UserCreateResponse.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully created user."),
            @ApiResponse(code = 404, message = "The database with id does not exist or status is not READY"),
            @ApiResponse(code = 500, message = "Error creating user")
    })
    @POST
    @Timed
    @Path("/createuser")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createUser(UserCreateRequest userCreateRequest) {
        return Response.status(200).entity(databaseManager.createUser(userCreateRequest.getDatabaseId())).build();
    }

    @ApiOperation(value = "Hard delete all soft deleted DBs", hidden = true)
    @Path("/harddelete")
    @DELETE
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response hardDelete(@HeaderParam("token") String token) {
        databaseManager.hardDeleteDatabase(token);
        return Response.status(204).build();
    }

    @ApiOperation(value = "Return list of available instance types",
            notes = "Return list of available instance types",
            response = InstanceType.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved instance types."),
    })

    @GET
    @Timed
    @Path("/availableinstances")
    @Produces(MediaType.APPLICATION_JSON)
    public Response availableInstances() {
        return Response.ok().entity(databaseManager.getListOfInstanceTypes()).build();
    }

    @ApiOperation(value = "Details of an instances",
            notes = "Request to get the details of a specific RDS instance",
            response = Instance.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved the instance information",
                    response = Instance.class),
            @ApiResponse(code = 404, message = "The instance with specified id does not exist"),
            @ApiResponse(code = 500, message = "Either the instance reported an unmapped instance " +
                    "class or something went very screwy somewhere")
    })

    @Path("{instanceId}/instanceinfo")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDetails(@PathParam("instanceId") String instanceId) {
        return Response.ok().entity(instanceManager.getInstanceDetails(instanceId)).build();
    }



    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Instance Class change started Successfully."),
    })
    @PUT
    @Path("{instanceId}/{instanceClass}/upsize")
    @Produces(MediaType.APPLICATION_JSON)
    public Response modifyInstanceClass(@PathParam("instanceId") String instanceId, @PathParam("instanceClass") String instanceClass){
        AmazonRDS client = new AmazonRDSClient();


        ModifyDBInstanceRequest request= new ModifyDBInstanceRequest()
                .withDBInstanceIdentifier(instanceId)
                .withDBInstanceClass(String.valueOf(instanceClass))
                .withApplyImmediately(true);

        client.setRegion(dBaaSConfiguration.getAwsRegion());
        DBInstance response = client.modifyDBInstance(request);
        return Response.ok().build();
    }

    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Snapshot created Successfully."),
            @ApiResponse(code = 404, message = "The specified DB instance is not in the available state."),
            @ApiResponse(code = 405, message = "DBInstanceIdentifier does not refer to an existing DB instance."),
    })
    @POST
    @Path("{instanceId}/snapshot")
    @Produces(MediaType.APPLICATION_JSON)
    public Response createInstanceSnapshot(@PathParam("instanceId") String instanceId){
        AmazonRDS client = new AmazonRDSClient();

        Format formatter = new SimpleDateFormat("-YYYY-MM-dd-hh-mm");
        String mydbsnapshot = instanceId + formatter.format(Date.from(Instant.now()));
        CreateDBSnapshotRequest request = new CreateDBSnapshotRequest()
                .withDBInstanceIdentifier(instanceId)
                .withDBSnapshotIdentifier(mydbsnapshot)
                .withTags(new Tag().withKey("manualSnapshot").withValue("DBaaS-Created-Snapshot"));
        client.setRegion(dBaaSConfiguration.getAwsRegion());
        DBSnapshot response = client.createDBSnapshot(request);

        URI url = urlHelper.buildURL(response.getDBSnapshotIdentifier()).get();

        return Response.created(url).entity(response).build();
        //return Response.ok().build();
    }

    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Snapshot created Successfully."),
            @ApiResponse(code = 404, message = "The specified DB instance is not in the available state."),
            @ApiResponse(code = 405, message = "DBInstanceIdentifier does not refer to an existing DB instance."),
    })
    @POST
    @Path("{snapshotId}/{instanceId}/restore")
    @Produces(MediaType.APPLICATION_JSON)
    public Response restoreInstanceSnapshot(@PathParam("snapshotId") String instanceId,@PathParam("instanceId") String snapshotId){
        AmazonRDS client = new AmazonRDSClient();
        RestoreDBInstanceFromDBSnapshotRequest request = new RestoreDBInstanceFromDBSnapshotRequest()
                .withDBSnapshotIdentifier(snapshotId)
                .withDBInstanceIdentifier(instanceId);
        client.setRegion(dBaaSConfiguration.getAwsRegion());
        DBInstance response = client.restoreDBInstanceFromDBSnapshot(request);

        URI url = urlHelper.buildURL(response.getDBInstanceStatus()).get();

        return Response.created(url).entity(response).build();
        //return Response.ok().build();
    }


}
