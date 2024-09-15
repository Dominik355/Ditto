package com.bilik.ditto.core.configuration.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.net.URI;
import java.net.URISyntaxException;

public class URIValidator implements ConstraintValidator<ValidURI, String> {

    /**
     * Tries to create URI from given String, if invalid - exception wil be thrown and so false returned.
     */
    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        try {
            URI uri = new URI(value);
        } catch (URISyntaxException ex) {
            return false;
        }
        return true;
    }

}
