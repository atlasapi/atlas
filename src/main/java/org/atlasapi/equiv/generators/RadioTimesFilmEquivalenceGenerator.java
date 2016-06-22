package org.atlasapi.equiv.generators;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;

public class RadioTimesFilmEquivalenceGenerator implements EquivalenceGenerator<Item> {

    private final Pattern rtFilmUriPattern = Pattern.compile("http://radiotimes.com/films/(\\d+)");
    private final String paFilmUriPrefix = "http://pressassociation.com/films/";
    
    private final ContentResolver resolver;

    public RadioTimesFilmEquivalenceGenerator(ContentResolver resolver) {
        this.resolver = resolver;
    }
    
    @Override
    public ScoredCandidates<Item> generate(Item content, ResultDescription desc) {
        checkArgument(content instanceof Film, "Content not Film:" + content.getCanonicalUri());
        
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
                results.addEquivalent((Film)resolvedContent.requireValue(), Score.ONE);
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
