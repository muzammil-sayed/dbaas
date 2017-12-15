package com.jivesoftware.data.resources.entities.jersey_validators;

import com.google.common.collect.ImmutableSet;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Set;

public class ClassAllowedMultiAZValidator implements
        ConstraintValidator<ClassAllowedMultiAZ, DatabaseCreationRequest> {

    private static final Set<String> SMALLCLASSES = ImmutableSet.of
            ("t2.micro", "t2.small", "t2.medium");

    @Override
    public void initialize(ClassAllowedMultiAZ constraintAnnotation) {

    }

    /**
     * Checks if the value passed for instance class is acceptable for a multi-AZ deploy
     *
     * @param creationRequest The DatabaseCreationRequest sent with a create POST
     *
     * @return TRUE when the creation request does not request high availability, when it does
     * request HA, but the instance class is an acceptable value, or when no specific instance
     * class is requested
     *
     */
    @Override
    public boolean isValid(DatabaseCreationRequest creationRequest, ConstraintValidatorContext context) {

        return !(creationRequest.getInstanceClassOptional().isPresent() &&
                (SMALLCLASSES.contains(creationRequest.getInstanceClassOptional().get()) &&
                creationRequest.getHighlyAvailable()));

    }
}
