package org.atlasapi.equiv.generators;

import com.google.api.client.util.Lists;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.SourceLimitedEquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.search.model.SearchQuery;

import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

public class ExactTitleGenerator<T extends Content> implements EquivalenceGenerator<T> {

    // any punctuation, whitespace, alpha numeric
    private static final Pattern STANDARD_CHARS = Pattern.compile("[\\p{P}\\p{Space}a-zA-Z0-9]+");

    private static final int SEARCH_LIMIT = 20;
    private static final String NAME = "Exact Title";

    private final Set<Publisher> publisher;
    private final SearchResolver searchResolver;
    private final Class<? extends T> cls;

    //filter search results to same specialization as the given content
    private final boolean useContentSpecialization;

    /**
     * Because title searching isn't very efficient it's currently being limited to either
     * foreign character containing titles or when the names are very short to catch some
     * initial easy cases.
     */
    public ExactTitleGenerator(
            SearchResolver searchResolver,
            Class<? extends T> cls,
            boolean useContentSpecialization,
            Publisher... publisher
    ) {
        this.searchResolver = searchResolver;
        this.cls = cls;
        this.useContentSpecialization = useContentSpecialization;
        this.publisher = ImmutableSet.copyOf(publisher);
    }

    @Override
    public ScoredCandidates<T> generate(
            T content,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Exact Title Generator");

        if (Strings.isNullOrEmpty(content.getTitle())) {
            desc.appendText("subject has no title");
            return DefaultScoredCandidates.<T>fromSource(NAME).build();
        }

        // run if title contains non standard characters (eg cyrillic or chinese characters)
        if (containsNonStandardChars(content.getTitle())) {
            return searchForCandidates(content, desc);
        }

        // discard long titles
        if (content.getTitle().length() >= 4) {
            desc.appendText("subject title too long {}", content.getTitle().length());
            return DefaultScoredCandidates.<T>fromSource(NAME).build();
        }

        return searchForCandidates(content, desc);
    }

    @Override
    public EquivalenceGeneratorMetadata getMetadata() {
        return SourceLimitedEquivalenceGeneratorMetadata.create(
                this.getClass().getCanonicalName(),
                publisher
        );
    }

    private ScoredCandidates<T> searchForCandidates(T content, ResultDescription desc) {
        Application application = DefaultApplication.createWithReads(Lists.newArrayList(publisher));

        SearchQuery.Builder titleQuery = getSearchQueryBuilder(publisher, content.getTitle());

        if (useContentSpecialization && content.getSpecialization() != null) {
            titleQuery.withSpecializations(ImmutableSet.of(content.getSpecialization()));
        } else if (!useContentSpecialization) {
            titleQuery.withFakeSpecialization(true);
        }

        desc.appendText("query: %s, specialization: %s, publishers: %s",
                content.getTitle(),
                useContentSpecialization ? content.getSpecialization() : "no specialization",
                publisher
        );

        Iterable<? extends T> results = searchResolver.search(titleQuery.build(), application)
                .stream()
                .filter(cls::isInstance)
                .map(cls::cast)
                .collect(MoreCollectors.toImmutableList());

        DefaultScoredCandidates.Builder<T> scoredCandidates = DefaultScoredCandidates.fromSource(NAME);

        //Return actively published results, and not the subject itself, only keeping exact title matches
        StreamSupport.stream(results.spliterator(), false)
                .filter(Described::isActivelyPublished)
                .filter(input-> !Objects.equals(input.getId(), content.getId()))
                .filter(input -> input.getTitle().equals(content.getTitle()))
                .forEach(input -> scoredCandidates.addEquivalent(input, Score.ZERO));

        return scoredCandidates.build();
    }

    private SearchQuery.Builder getSearchQueryBuilder(Set<Publisher> publishers, String title) {
        return SearchQuery.builder(title)
                .withSelection(new Selection(0, SEARCH_LIMIT))
                .withPublishers(publishers);
    }

    private boolean containsNonStandardChars(String title) {
        Matcher matcher = STANDARD_CHARS.matcher(title);
        // inverse because matcher is true if every character is 'standard'
        return !matcher.matches();
    }

    @Override
    public String toString() {
        return "Exact Title-matching Generator";
    }

}
