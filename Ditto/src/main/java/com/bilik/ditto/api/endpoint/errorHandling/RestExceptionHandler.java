package com.bilik.ditto.api.endpoint.errorHandling;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@Order(Ordered.HIGHEST_PRECEDENCE)
@ControllerAdvice
public class RestExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler({ Exception.class })
    public ResponseEntity<Object> handleAll(Exception ex, HttpServletRequest httpRequest) {
        return ResponseEntity.internalServerError()
                .body(new ApiError(
                        HttpStatus.INTERNAL_SERVER_ERROR,
                        ex.getLocalizedMessage(),
                        httpRequest.getRequestURI()));
    }

    @ExceptionHandler({ NotFoundException.class })
    public ResponseEntity<Object> handleNotFound(Exception ex, HttpServletRequest httpRequest) {
        return ResponseEntity.internalServerError()
                .body(new ApiError(
                        HttpStatus.NOT_FOUND,
                        ex.getLocalizedMessage(),
                        httpRequest.getRequestURI()));
    }


}