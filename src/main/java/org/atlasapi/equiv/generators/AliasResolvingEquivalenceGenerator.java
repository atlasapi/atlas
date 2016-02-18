package org.atlasapi.equiv.generators;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import com.google.common.collect.Iterables;

public class AliasResolvingEquivalenceGenerator<T extends Content> implements EquivalenceGenerator<T> {

    public static final <T extends Content> EquivalenceGenerator<T> aliasResolvingGenerator(ContentResolver resolver, Class<T> cls) {
        return new AliasResolvingEquivalenceGenerator<T>(resolver,cls);
    }
    
    private final ContentResolver resolver;
    private final Class<T> cls;

    public AliasResolvingEquivalenceGenerator(ContentResolver resolver, Class<T> cls) {
        this.resolver = resolver;
        this.cls = cls;
    }

    @Override
    public ScoredCandidates<T> generate(T content, ResultDescription desc) {
        Builder<T> equivalents = DefaultScoredCandidates.fromSource("Alias");
        desc.startStage("Resolving aliases:");
        // TODO new aliases
        for (String alias : content.getAliasUrls()) {
            desc.appendText(alias);
        }
        desc.finishStage();
        
        // TODO new alias
       ResolvedContent resolved = resolver.findByCanonicalUris(content.getAliasUrls());
       
       for (T identified : Iterables.filter(resolved.getAllResolvedResults(), cls)) {
           if (identified.isActivelyPublished()) {
               equivalents.addEquivalent(identified, Score.ONE);
               desc.appendText("Resolved %s", identified.getCanonicalUri());
           }
       }
       desc.appendText("Missed %s", resolved.getUnresolved().size());
        
        return equivalents.build();
    }
    
    @Override
    public String toString() {
        return "Alias Resolving Generator";
    }
}
