package org.atlasapi.system;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.equiv.EquivTaskModule;
import org.atlasapi.equiv.EquivalenceBreaker;
import org.atlasapi.persistence.MongoContentPersistenceModule;
import org.atlasapi.persistence.content.ContentPurger;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.atlasapi.persistence.MongoContentPersistenceModule.NO_EQUIVALENCE_WRITING_CONTENT_WRITER;

@Configuration
@Import( { MongoContentPersistenceModule.class, EquivTaskModule.class })
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

    @Autowired
    private ContentResolver contentResolver;

    @Autowired
    private LookupEntryStore lookupEntryStore;

    @Autowired
    private ContentWriter contentWriter;

    @Autowired
    @Qualifier(NO_EQUIVALENCE_WRITING_CONTENT_WRITER)
    private ContentWriter noEquivalenceWritingContentWriter;

    @Autowired
    private EquivalenceBreaker equivalenceBreaker;

    @Bean
    public LyrebirdYoutubeContentPurgeController lyrebirdYoutubeContentPurgeController() {
        return new LyrebirdYoutubeContentPurgeController(contentPurger);
    }
    
    @Bean
    public ScrubbablesProducerContentPurgeController scrubbablesProducerContentPurgeController() {
        return new ScrubbablesProducerContentPurgeController(contentPurger);
    }
    
    @Bean
    public ContentPurgeController btVodContentPurgeController() {
        return new ContentPurgeController(contentPurger);
    }

    @Bean
    public UnpublishContentController unpublishContentController() {
        return new UnpublishContentController(
                SubstitutionTableNumberCodec.lowerCaseOnly(),
                contentResolver,
                lookupEntryStore,
                contentWriter,
                noEquivalenceWritingContentWriter,
                equivalenceBreaker
        );
    }
}
