package com.bilik.ditto.core.configuration.validation;

import com.bilik.ditto.core.common.DurationResolver;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class StringDurationValidator implements ConstraintValidator<ValidDuration, String> {

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        try {
            DurationResolver.resolveDuration(value);
        } catch (Exception ex) {
            return false;
        }
        return true;
    }
}
