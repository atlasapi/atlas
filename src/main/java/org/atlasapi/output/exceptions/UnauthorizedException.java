package org.atlasapi.output.exceptions;


public class UnauthorizedException extends RuntimeException {

    public UnauthorizedException() {
        super();
    }

    public UnauthorizedException(String message) {
        super(message);
    }

    public UnauthorizedException(String meesage, Throwable throwable) {
        super(meesage, throwable);
    }

}
