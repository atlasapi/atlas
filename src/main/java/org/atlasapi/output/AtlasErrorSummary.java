package org.atlasapi.output;

import java.util.Map;
import java.util.UUID;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;

import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.webapp.query.DateTimeInQueryParser.MalformedDateTimeException;

import com.google.common.collect.ImmutableMap;

public class AtlasErrorSummary {

    private static class AtlasExceptionBuilder {

        private final String friendly;
        private final HttpStatusCode httpStatus;

        public AtlasExceptionBuilder(String friendlyCode, HttpStatusCode httpStatusCode) {
            this.friendly = friendlyCode;
            this.httpStatus = httpStatusCode;
        }

        public String friendly() {
            return friendly;
        }

        public HttpStatusCode httpStatus() {
            return httpStatus;
        }

        public AtlasErrorSummary build(Exception exception) {
            return new AtlasErrorSummary(exception).withErrorCode(friendly()).withStatusCode(httpStatus());
        }
    }

    private static class ExceptionExposingAtlasExceptionBuilder extends AtlasExceptionBuilder{

        public ExceptionExposingAtlasExceptionBuilder(String friendlyCode, HttpStatusCode httpStatusCode) {
            super(friendlyCode, httpStatusCode);
        }

        public AtlasErrorSummary build(Exception exception) {
            return new AtlasErrorSummary(exception).withErrorCode(friendly()).withStatusCode(httpStatus()).withMessage(exception.getMessage());
        }
    }

    private static Map<Class<? extends Exception>, AtlasExceptionBuilder> exceptionCodes = exceptionMap();

    public static AtlasErrorSummary forException(Exception exception) {
        AtlasExceptionBuilder builder = exceptionCodes.get(exception.getClass());
        if (builder != null) {
            return builder.build(exception);
        } else {
            return new AtlasErrorSummary(exception);
        }
    }

    private static Map<Class<? extends Exception>, AtlasExceptionBuilder> exceptionMap() {
        return ImmutableMap.<Class<? extends Exception>, AtlasExceptionBuilder>builder()
                .put(
                        IllegalArgumentException.class,
                        new ExceptionExposingAtlasExceptionBuilder(
                                "BAD_QUERY_ATTRIBUTE", HttpStatusCode.BAD_REQUEST
                        )
                )
                .put(
                        MalformedDateTimeException.class,
                        new ExceptionExposingAtlasExceptionBuilder(
                                "BAD_DATE_TIME_VALUE", HttpStatusCode.BAD_REQUEST
                        )
                )
                .put(
                        ApiKeyNotFoundException.class,
                        new ExceptionExposingAtlasExceptionBuilder(
                                "API_KEY_NOT_FOUND", HttpStatusCode.FORBIDDEN
                        )
                )
                .put(
                        InvalidIpForApiKeyException.class,
                        new ExceptionExposingAtlasExceptionBuilder(
                                "INVALID_IP_FOR_API_KEY", HttpStatusCode.FORBIDDEN
                        ))
                .put(
                        RevokedApiKeyException.class,
                        new ExceptionExposingAtlasExceptionBuilder(
                                "REVOKED_API_KEY", HttpStatusCode.FORBIDDEN
                        )
                )
                .put(
                        MissingApplicationOwlAccessRoleException.class,
                        new ExceptionExposingAtlasExceptionBuilder(
                                "API_KEY_NO_OWL_ACCESS", HttpStatusCode.FORBIDDEN
                        )
                )
                .build();
    }

    private String id;
    private Exception exception;
    private String errorCode = "INTERNAL_ERROR";
    private HttpStatusCode statusCode = HttpStatusCode.SERVER_ERROR;
    private String message = "An internal server error occurred";

    public AtlasErrorSummary(Exception exception) {
        this.exception = exception;
        this.id = UUID.randomUUID().toString();
    }

    public AtlasErrorSummary() {}

    public String id() {
        return id;
    }

    public Exception exception() {
        return exception;
    }

    public AtlasErrorSummary withStatusCode(HttpStatusCode statusCode) {
        this.statusCode = statusCode;
        return this;
    }

    public HttpStatusCode statusCode() {
        return statusCode;
    }

    public AtlasErrorSummary withErrorCode(String errorCode) {
        this.errorCode = errorCode;
        return this;
    }
    public String errorCode() {
        return errorCode;
    }

    public AtlasErrorSummary withMessage(String message) {
        this.message = message;
        return this;
    }
    public String message() {
        return this.message;
    }
}
