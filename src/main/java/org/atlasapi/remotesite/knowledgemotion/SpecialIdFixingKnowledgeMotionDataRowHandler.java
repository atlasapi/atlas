package org.atlasapi.remotesite.knowledgemotion;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;

public class SpecialIdFixingKnowledgeMotionDataRowHandler implements KnowledgeMotionDataRowHandler {

    private static final String CORRECTED_BLOOMBERG_ID_ALIAS_NAMESPACE = "knowledgemotion:correctedBloombergId";
    private final Splitter idSplitter = Splitter.on(":").omitEmptyStrings();

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
        if (!sourceConfig.rowHeader().equals(row.getSource())) {
            return Optional.absent();
        }

        String id = Iterables.getLast(idSplitter.split(row.getId()));

        Maybe<Identified> existing = resolve(sourceConfig.uri(id));
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
