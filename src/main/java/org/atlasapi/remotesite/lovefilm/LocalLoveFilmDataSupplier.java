package org.atlasapi.remotesite.lovefilm;

import java.io.File;
import java.nio.charset.Charset;

import com.google.common.io.Files;

public class LocalLoveFilmDataSupplier implements LoveFilmDataSupplier {

    private File data;

    public LocalLoveFilmDataSupplier(File data) {
        this.data = data;
    }
    
    @Override
    public LoveFilmData getLatestData() {
        Charset charset = Charset.forName("windows-1252");
        return new LoveFilmData(Files.newReaderSupplier(data, charset));
    }

}
