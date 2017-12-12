package org.atlasapi.input;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.validation.*;
import java.io.IOException;
import java.io.Reader;
import java.util.Set;

public class JacksonModelReader implements ModelReader {

    private ObjectMapper failOnUnknownPropertyMapper;
    private ObjectMapper mapper;
    private Validator validator;

    public JacksonModelReader(ObjectMapper failOnUnknownPropertyMapper, ObjectMapper mapper) {
        this.failOnUnknownPropertyMapper = failOnUnknownPropertyMapper;
        this.mapper = mapper;
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        this.validator = factory.getValidator();
    }

    @Override
    public <T> T read(Reader reader, Class<T> cls, Boolean strict) throws IOException, ReadException {
        if(strict) {
            return validate(failOnUnknownPropertyMapper.readValue(reader, cls));
        }
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