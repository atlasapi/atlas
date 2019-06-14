package org.atlasapi.remotesite.amazon;



public class NoDataException extends Exception {

    private static final long serialVersionUID = 7292664400238873965L;
    
    public NoDataException(String message) {
        super(message);
    }
}
