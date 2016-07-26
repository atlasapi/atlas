package org.atlasapi.input;

import java.io.IOException;
import java.io.Reader;

public interface ModelReader {

    <T> T read(Reader reader, Class<T> cls, Boolean strict) throws IOException, ReadException;


}
