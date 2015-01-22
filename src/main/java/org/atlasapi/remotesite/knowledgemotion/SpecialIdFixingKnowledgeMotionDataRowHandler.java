package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.channels.Pipe.SourceChannel;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;

public class SpecialIdFixingKnowledgeMotionDataRowHandler implements KnowledgeMotionDataRowHandler {

    private static final String CORRECTED_BLOOMBERG_ID_ALIAS_NAMESPACE = "knowledgemotion:correctedBloombergId";

    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final KnowledgeMotionSourceConfig sourceConfig;

    public SpecialIdFixingKnowledgeMotionDataRowHandler(ContentResolver resolver, ContentWriter writer,
            KnowledgeMotionSourceConfig sourceConfig) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.sourceConfig = checkNotNull(sourceConfig);
    }

    @Override
    public Optional<Content> handle(KnowledgeMotionDataRow row) {
        Maybe<Identified> existing = resolve(sourceConfig.uri(row.getId()));
        if (existing.isNothing()) {
            return Optional.absent();
        }

        Item identified = (Item) existing.requireValue();
        identified.setAliases(ImmutableList.of(new Alias(CORRECTED_BLOOMBERG_ID_ALIAS_NAMESPACE, row.getAlternativeId())));
        write(identified);
        return Optional.of((Content) identified);
    }

    public void write(Content content) {
        Item item = (Item) content;
        writer.createOrUpdate(item);
    }

    private Maybe<Identified> resolve(String uri) {
        ImmutableSet<String> uris = ImmutableSet.of(uri);
        return resolver.findByCanonicalUris(uris).get(uri);
    }

}
