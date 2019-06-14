package org.atlasapi.equiv.generators;

import com.google.common.collect.ImmutableList;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.SearchResolver;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TitleSearchGeneratorTest {

    @Test
    public void testDoesntSearchForPublisherOfSubjectContent() {
        
        final Publisher subjectPublisher = Publisher.C4;
        Brand subject = new Brand("uri","curie", subjectPublisher);
        subject.setId(1L);
        final String title = "test";
        subject.setTitle(title);

        SearchResolver searchResolver = (query, application) -> {

            assertFalse(query.getIncludedPublishers().contains(subjectPublisher));
            assertFalse(application.getConfiguration().getEnabledReadSources().contains(subjectPublisher));

            assertTrue(query.getTerm().equals(title));

            Brand result = new Brand("result","curie", Publisher.PA);
            result.setTitle(title);
            return ImmutableList.of(result);
        };
        
        TitleSearchGenerator<Container> generator = TitleSearchGenerator.create(searchResolver, Container.class, Publisher.all(), 2);
        ScoredCandidates<Container> generated = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        
        assertTrue(generated.candidates().keySet().size() == 1);
        
    }

}
