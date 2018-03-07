package org.atlasapi.remotesite.channel4.pmlsd;

import com.google.common.base.Strings;
import com.metabroadcast.common.time.Clock;
import com.sun.syndication.feed.atom.Content;
import com.sun.syndication.feed.atom.Entry;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.remotesite.ContentExtractor;
import org.jdom.Attribute;
import org.jdom.Element;

import java.util.Map;

public abstract class BaseC4ItemExtractor<I extends Item> implements ContentExtractor<Entry, I> {

    protected final Clock clock;
    
    public BaseC4ItemExtractor(Clock clock) {
        this.clock = clock;
    }
    
    @Override
    public final I extract(Entry entry) {
        Map<String, String> lookup = C4AtomApi.foreignElementLookup(entry);
        I item = createItem(entry, lookup);

        String programmeId = lookup.get(C4AtomApi.DC_PROGRAMME_ID);
        if (programmeId != null) {
            item.addAlias(new Alias(C4AtomApi.ALIAS, programmeId));
            item.addAlias(new Alias(C4AtomApi.ALIAS_FOR_BARB, programmeId));
        }

        item.setLastUpdated(clock.now());
        item.setTitle(entry.getTitle());
        Content summary = entry.getSummary();
        if (summary != null) {
            item.setDescription(Strings.emptyToNull(summary.getValue()));
        }
        item.setMediaType(MediaType.VIDEO);
        item.setSpecialization(Specialization.TV);
        addImages(entry, item);
        return setAdditionalItemFields(entry, lookup, item);
    }


    protected abstract I createItem(Entry source, Map<String, String> lookup);
    protected abstract I setAdditionalItemFields(Entry source, Map<String, String> lookup, I item);

    private final void addImages(Entry source, Item item) {
        Element mediaGroup = getMedia(source);
        
        if (mediaGroup != null) {
            Element thumbnail = mediaGroup.getChild("thumbnail", C4AtomApi.NS_MEDIA_RSS);
            if (thumbnail != null) {
                Attribute thumbnailUri = thumbnail.getAttribute("url");
                C4AtomApi.addImages(item, thumbnailUri.getValue());
            }
        }
    }

    protected abstract Element getMedia(Entry source);

}
