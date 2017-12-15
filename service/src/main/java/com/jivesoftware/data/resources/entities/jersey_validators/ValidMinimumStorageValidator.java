package com.jivesoftware.data.resources.entities.jersey_validators;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Optional;

public class ValidMinimumStorageValidator implements
        ConstraintValidator<ValidMinimumStorage, Optional<Integer>> {

    private final static Integer MINIMUM_ALLOWED_SSD_STORAGE = 100;

    @Override
    public void initialize(ValidMinimumStorage constraintAnnotation) {

    }

    /**
     * Checks if the value passed for storage is acceptable
     *
     * @param instanceStorage The value passed in the DatabaseCreationRequest for gigs of storage
     *                        on a dedicated instance
     *
     * @return TRUE when value of storage passed is higher than minimum recommended value for SSD
     * storage (currently 100gigs) or no value is
     *
     */
    @Override
    public boolean isValid(Optional<Integer> instanceStorage, ConstraintValidatorContext context) {

        return !(instanceStorage.isPresent() && instanceStorage.get() < MINIMUM_ALLOWED_SSD_STORAGE);
    }
}
