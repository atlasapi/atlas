package org.atlasapi.remotesite.bbc.nitro;

import org.atlasapi.AtlasFetchModule;
import org.atlasapi.persistence.MongoContentPersistenceModule;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({AtlasFetchModule.WriterModule.class, MongoContentPersistenceModule.class})
public class NitroPersistenceModule {
    @Autowired
    private ContentWriter rawContentWriter;

    @Autowired
    private ContentResolver contentResolver;
    
    @Bean
    @Qualifier("nitroContentWriter")
    public ContentWriter contentWriter() {
        return new LastUpdatedSettingContentWriter(contentResolver, rawContentWriter);
    }
}
