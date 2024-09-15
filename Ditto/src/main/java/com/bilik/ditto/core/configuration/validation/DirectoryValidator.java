package com.bilik.ditto.core.configuration.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.Files.isDirectory;

public class DirectoryValidator implements ConstraintValidator<ValidDirectory, String> {

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        Path path = Path.of(value);
        return isDirectory(path) && !Files.notExists(path);
    }

}
