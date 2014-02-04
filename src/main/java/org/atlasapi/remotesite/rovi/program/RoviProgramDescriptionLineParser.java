package org.atlasapi.remotesite.rovi.program;

import org.apache.commons.lang.StringUtils;
import org.atlasapi.remotesite.rovi.RoviConstants;
import org.atlasapi.remotesite.rovi.RoviLineParser;
import org.atlasapi.remotesite.rovi.RoviUtils;


public class RoviProgramDescriptionLineParser implements RoviLineParser<RoviProgramDescriptionLine> {

    private static final int PROGRAM_ID_POS = 0;
    private static final int SOURCE_ID_POS = 1;
    private static final int DESCRIPTION_CULTURE_POS = 2;
    private static final int DESCRIPTION_TYPE_POS = 3;
    private static final int DESCRIPTION_POS = 4;
    
    @Override
    public RoviProgramDescriptionLine parseLine(String line) {
        Iterable<String> parts = RoviConstants.LINE_SPLITTER.split(line);
        RoviProgramDescriptionLine.Builder builder = RoviProgramDescriptionLine.builder();
        
        builder.withProgramId(RoviUtils.getPartAtPosition(parts, PROGRAM_ID_POS));
        
        String sourceId = RoviUtils.getPartAtPosition(parts, SOURCE_ID_POS);
        if (StringUtils.isNotBlank(sourceId) && StringUtils.isNotEmpty(sourceId)) {
            builder.withSourceId(sourceId);
        }
        
        builder.withDescriptionCulture(RoviUtils.getPartAtPosition(parts, DESCRIPTION_CULTURE_POS));
        builder.withDescriptionType(RoviUtils.getPartAtPosition(parts, DESCRIPTION_TYPE_POS));
        builder.withDescription(RoviUtils.getPartAtPosition(parts, DESCRIPTION_POS));
        
        return builder.build();
    }

}
