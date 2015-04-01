package org.atlasapi.system;

import org.atlasapi.persistence.MongoContentPersistenceModule;
import org.atlasapi.persistence.content.ContentPurger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import( { MongoContentPersistenceModule.class })
/**
 * Module for constructing controllers that allow deletion of content
 * using a {@link ContentPurger}. Only specific-publisher controllers
 * should be added, rather than general ones that take a publisher as 
 * a parameter; the latter is far too dangerous.
 * 
 * @author tom
 *
 */
public class ContentPurgeWebModule {

    @Autowired
    private ContentPurger contentPurger;
    
    @Bean
    public LyrebirdYoutubeContentPurgeController lyrebirdYoutubeContentPurgeController() {
        return new LyrebirdYoutubeContentPurgeController(contentPurger);
    }
    
    @Bean
    public ScrubbablesProducerContentPurgeController scrubbablesProducerContentPurgeController() {
        return new ScrubbablesProducerContentPurgeController(contentPurger);
    }
    
    @Bean
    public BtVodContentPurgeController btVodContentPurgeController() {
        return new BtVodContentPurgeController(contentPurger);
    }
}
