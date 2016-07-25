package org.atlasapi.equiv.generators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMultimap;
import junit.framework.TestCase;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.EquivalenceSummary;
import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.collect.ImmutableOptionalMap;

@RunWith(MockitoJUnitRunner.class)
public class ContainerChildEquivalenceGeneratorTest extends TestCase {

    private static final ImmutableSet<String> NO_CANDIDATES = ImmutableSet.<String>of();
    
    private final ContentResolver resolver = mock(ContentResolver.class);
    private final EquivalenceSummaryStore equivSummaryStore = mock(EquivalenceSummaryStore.class);

    private final ContainerChildEquivalenceGenerator generator = new ContainerChildEquivalenceGenerator(resolver, equivSummaryStore);
    
    @Test
    public void testExtractsContainerFromStrongItemEquivalents() {

        Container subject = new Container("subject","s",Publisher.BBC);
        subject.setChildRefs(ImmutableSet.of(
            new Episode("child1","c1",Publisher.BBC).childRef(),
            new Episode("child2","c2",Publisher.BBC).childRef()
        ));
        Container equiv1 = new Container("equivalent1","e1",Publisher.PA);
        Container equiv2 = new Container("equivalent2","e2",Publisher.ITV);

        when(equivSummaryStore.summariesForUris(argThat(hasItems("child1","child2")))).thenReturn(
            ImmutableOptionalMap.fromMap(ImmutableMap.of(
                "child1",
                new EquivalenceSummary("child1","subject",NO_CANDIDATES, ImmutableMultimap.of(
                    Publisher.BBC, new ContentRef("equivItem",Publisher.BBC,""),
                    Publisher.PA, new ContentRef("equivC1", Publisher.PA, "equivalent1"))),
                "child2",
                new EquivalenceSummary("child2","subject",NO_CANDIDATES,ImmutableMultimap.of(
                    Publisher.BBC, new ContentRef("equivC2",Publisher.BBC,"equivalent2"),
                    Publisher.PA, new ContentRef("equivC1",Publisher.PA, "equivalent1"))
                )
            ))
        );

        ResolvedContent content = ResolvedContent.builder()
                .put(equiv1.getCanonicalUri(), equiv1)
                .put(equiv2.getCanonicalUri(), equiv2)
                .build();

        when(resolver.findByCanonicalUris(argThat(hasItems("equivalent1","equivalent2")))).thenReturn(
            content
        );

        ScoredCandidates<Container> scores = generator.generate(subject, new DefaultDescription());

        assertThat(scores.candidates(), hasEntry(equiv1, Score.ONE));
        assertThat(scores.candidates(), hasEntry(equiv2, Score.valueOf(0.5)));
    }

}
