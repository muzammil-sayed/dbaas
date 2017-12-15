package com.jivesoftware.data.resources.entities.jersey_validators;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.*;

@Target({ ElementType.FIELD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = { ValidMinimumStorageValidator.class })
@Documented
public @interface ValidMinimumStorage {
    String message();

    Class<?>[] groups() default { };

    Class<? extends Payload>[] payload() default { };
}
