package org.atlasapi.output;

public class MissingApplicationOwlAccessRoleException extends RuntimeException {

    private MissingApplicationOwlAccessRoleException() {
        super("This key does not have permission to access /3.0/ APIs. "
                + "Please use /4/ APIs instead");
    }

    public static MissingApplicationOwlAccessRoleException create() {
        return new MissingApplicationOwlAccessRoleException();
    }
}
