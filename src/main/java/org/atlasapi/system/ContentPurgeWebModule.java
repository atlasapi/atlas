package org.atlasapi.system;

import org.atlasapi.persistence.MongoContentPersistenceModule;
import org.atlasapi.persistence.content.ContentPurger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import( { MongoContentPersistenceModule.class })
public class ContentPurgeWebModule {

    @Autowired
    private ContentPurger contentPurger;
    
    @Bean
    public LyrebirdYoutubeContentPurgeController lyrebirdYoutubeContentPurgeController() {
        return new LyrebirdYoutubeContentPurgeController(contentPurger);
    }
}
