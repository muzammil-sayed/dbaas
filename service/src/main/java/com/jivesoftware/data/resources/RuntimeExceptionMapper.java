package com.jivesoftware.data.resources;

import com.jivesoftware.data.exceptions.*;
import com.jivesoftware.data.resources.entities.ErrorInfo;
import com.jivesoftware.data.resources.entities.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class RuntimeExceptionMapper implements ExceptionMapper<RuntimeException> {

    private final static Logger LOG = LoggerFactory.getLogger(RuntimeExceptionMapper.class);

    @Override
    public Response toResponse(RuntimeException runtime) {

        if (runtime instanceof NoCapacityException) {
            return Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorInfo(runtime.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } else if (runtime instanceof DatabaseNotFoundException) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorInfo(runtime.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } else if (runtime instanceof TokenNotAuthorizedException) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorInfo(runtime.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } else if (runtime instanceof SchemaOperationException) {
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorInfo(runtime.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } else if (runtime instanceof InstanceNotFoundException) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorInfo(runtime.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } else if (runtime instanceof InstanceClassNotFoundException) {
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorInfo(runtime.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }

        // Build default response
        Response defaultResponse = Response
                .serverError()
                .type(MediaType.APPLICATION_JSON)
                .entity(new ErrorInfo(runtime.getMessage()))
                .build();

        // Check for any specific handling
        if (runtime instanceof WebApplicationException) {
            return handleWebApplicationException(runtime, defaultResponse);
        }

        // Use the default
        LOG.error("Error", runtime.getMessage());
        return defaultResponse;

    }

    private Response handleWebApplicationException(RuntimeException exception, Response defaultResponse) {
        WebApplicationException webAppException = (WebApplicationException) exception;

        if (webAppException.getResponse().getStatus() == 401) {
            LOG.debug(String.format("Received an unauthorized request, 401 returned"));
            return Response
                    .status(Response.Status.UNAUTHORIZED)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorInfo(webAppException.getMessage()))
                    .build();
        }
        if (webAppException.getResponse().getStatus() == 404) {
            LOG.debug(String.format("404 returned"));
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorInfo(webAppException.getMessage()))
                    .build();
        }

        return defaultResponse;
    }

}