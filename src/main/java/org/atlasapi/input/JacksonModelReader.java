package org.atlasapi.input;

import java.io.IOException;
import java.io.Reader;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonModelReader implements ModelReader {

    private ObjectMapper mapper;
    private Validator validator;

    public JacksonModelReader(ObjectMapper mapper) {
        this.mapper = mapper;
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        this.validator = factory.getValidator();
    }

    @Override
    public <T> T read(Reader reader, Class<T> cls) throws IOException, ReadException {
        return validate(mapper.readValue(reader, cls));
    }

    private <T> T validate(T input) {
        Set<ConstraintViolation<T>> constraintViolations = validator.validate(input);
        if(constraintViolations.isEmpty()) {
            return input;
        } else {
            throw new ConstraintViolationException(constraintViolations);
        }

    }
}
