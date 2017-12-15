package com.jivesoftware.data.resources.entities.jersey_validators;

import com.google.common.collect.ImmutableSet;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Optional;
import java.util.Set;

public class ClassValueValidator
        implements ConstraintValidator<ClassValue, Optional<String>> {

    private static final Set<String> INSTANCECLASSES = ImmutableSet
            .of("t2.micro", "t2.small", "t2.medium", "m4.large", "r3.large", "m4.xlarge",
                    "r3.xlarge", "m4.2xlarge", "r3.2xlarge", "m4.4xlarge", "r3.4xlarge",
                    "m4.10xlarge", "r3.8xlarge");

    @Override
    public void initialize(ClassValue constraintAnnotation) {

    }

    /**
     * Checks if the value passed for instance class is acceptable
     *
     * @param instanceClass The value passed in the DatabaseCreationRequest for RDS instance class
     *                      on a new dedicated instance
     *
     * @return TRUE when value of instanceClass is within the accepted set or no specific
     * instanceClass is requested (uses default)
     *
     */
    @Override
    public boolean isValid(Optional<String> instanceClass, ConstraintValidatorContext context) {
        return !(instanceClass.isPresent() && !INSTANCECLASSES.contains(instanceClass.get()));
    }
}
