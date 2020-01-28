package org.atlasapi.equiv.generators;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

public class RadioTimesFilmEquivalenceGenerator implements EquivalenceGenerator<Item> {

    private final Pattern rtFilmUriPattern = Pattern.compile("http://radiotimes.com/films/(\\d+)");
    private final String paFilmUriPrefix = "http://pressassociation.com/films/";
    private final Score scoreOnMatch;
    
    private final ContentResolver resolver;

    public RadioTimesFilmEquivalenceGenerator(ContentResolver resolver, Score score) {
        this.resolver = resolver;
        this.scoreOnMatch = score;
    }
    
    @Override
    public ScoredCandidates<Item> generate(
            Item content,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        checkArgument(content instanceof Film, "Content not Film:" + content.getCanonicalUri());

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Radio Times Film Equivalence Generator");
        
        Builder<Item> results = DefaultScoredCandidates.fromSource("RTtoPA");
        
        Matcher uriMatcher = rtFilmUriPattern.matcher(content.getCanonicalUri());
        if (uriMatcher.matches()) {
            String paUri = paFilmUriPrefix + uriMatcher.group(1);
            ResolvedContent resolvedByUris = resolver.findByUris(ImmutableSet.of(paUri));
            Maybe<Identified> resolvedContent = resolvedByUris.get(paUri);

            //Since PA ingester started using progId to generate film uris rather than RT film number,
            //new content will have different canonical uri from its alias,
            //thus not returning anything when requested by uri. In this case we try to substitute it with other content
            //that was found by that alias
            if (resolvedContent.isNothing() || !filmIsActivelyPublished(resolvedContent.requireValue())) {
                resolvedContent = resolvedByUris.filterContent(input ->
                        input.hasValue() && !input.requireValue().getCanonicalUri().equals(paUri))
                        .getFirstValue();
            }

            if (resolvedContent.hasValue()
                    && filmIsActivelyPublished(resolvedContent.requireValue())) {
                results.addEquivalent((Film)resolvedContent.requireValue(), scoreOnMatch);

                if (((Film) resolvedContent.requireValue()).getId() != null) {
                    generatorComponent.addComponentResult(
                            resolvedContent.requireValue().getId(),
                            scoreOnMatch.toString()
                    );
                }
            }
        }
        
        return results.build();
    }

    private boolean filmIsActivelyPublished(Identified resolvedContent) {
        return resolvedContent instanceof Film
                && ((Film) resolvedContent).isActivelyPublished();
    }

    @Override
    public String toString() {
        return "RT->PA Film Generator";
    }
}
