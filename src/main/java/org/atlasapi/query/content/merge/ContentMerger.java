package org.atlasapi.query.content.merge;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;

import com.metabroadcast.common.base.Maybe;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import static com.google.gdata.util.common.base.Preconditions.checkNotNull;

public class ContentMerger {

    private final ItemMerger itemMerger;
    private final EpisodeMerger episodeMerger;

    private ContentMerger(
            ItemMerger itemMerger,
            EpisodeMerger episodeMerger
    ){
        this.itemMerger = checkNotNull(itemMerger);
        this.episodeMerger = checkNotNull(episodeMerger);
    }

    public static ContentMerger create(
            ItemMerger itemMerger,
            EpisodeMerger episodeMerger
    ){
        return new ContentMerger(
                itemMerger,
                episodeMerger
        );
    }

    public Content merge(
            Optional<Identified> possibleExisting,
            Content update,
            boolean merge,
            BroadcastMerger broadcastMerger
    ) {
        if (!possibleExisting.isPresent()) {
            return update;
        }
        Identified identifiedContent = possibleExisting.get();

        if(!(identifiedContent instanceof Content)){
            throw new IllegalStateException(
                    "Entity for " + update.getCanonicalUri() + " not Content"
            );
        }

        Content existingContent = (Content) identifiedContent;
        if (!existingContent.getClass().equals(update.getClass())){
            return mergeWithTypeUpdate(existingContent, update, merge, broadcastMerger);
        }
        return merge(existingContent, update, merge, broadcastMerger);
    }

    private Content mergeWithTypeUpdate(
            Content existing,
            Content update,
            boolean merge,
            BroadcastMerger broadcastMerger
    ){
        if (update instanceof Episode){
            return merge(copyToEpisode(existing, broadcastMerger), update, merge, broadcastMerger);
        }
        if (update instanceof Film){
            return merge(copyToFilm(existing, broadcastMerger), update, merge, broadcastMerger);
        }
        if (update instanceof Brand){
            return merge(copyToBrand(existing, broadcastMerger), update, merge, broadcastMerger);
        }
        if (update instanceof Series){
            return merge(copyToSeries(existing, broadcastMerger), update, merge, broadcastMerger);
        }
        if (update instanceof Item){
            return merge(copyToItem(existing, broadcastMerger), update, merge, broadcastMerger);
        }

        return merge(existing, update, merge, broadcastMerger);
    }

    private Content copyToBrand(Content existing, BroadcastMerger broadcastMerger){
        Brand brand = new Brand();
        Content.copyTo(existing, brand);
        return merge(brand, existing, false, broadcastMerger);
    }

    private Content copyToEpisode(Content existing, BroadcastMerger broadcastMerger){
        Episode episode = new Episode();
        Content.copyTo(existing, episode);
        return merge(episode, existing, false, broadcastMerger);
    }

    private Content copyToSeries(Content existing, BroadcastMerger broadcastMerger){
        Series series = new Series();
        Content.copyTo(existing, series);
        return merge(series, existing, false, broadcastMerger);
    }

    private Content copyToFilm(Content existing, BroadcastMerger broadcastMerger){
        Film film = new Film();
        Content.copyTo(existing, film);
        return merge(film, existing, false, broadcastMerger);
    }

    private Content copyToItem(Content existing, BroadcastMerger broadcastMerger){
        Item item = new Item();
        Content.copyTo(existing, item);
        return merge(item, existing, false, broadcastMerger);
    }

    private Content merge(
            Content existing,
            Content update,
            boolean merge,
            BroadcastMerger broadcastMerger
    ) {
        existing.setActivelyPublished(update.isActivelyPublished());

        existing.setEquivalentTo(
                merge
                ? merge(existing.getEquivalentTo(), update.getEquivalentTo())
                : update.getEquivalentTo()
        );
        existing.setLastUpdated(update.getLastUpdated());
        existing.setTitle(update.getTitle());
        existing.setShortDescription(update.getShortDescription());
        existing.setMediumDescription(update.getMediumDescription());
        existing.setLongDescription(update.getLongDescription());
        existing.setDescription(update.getDescription());
        existing.setImage(update.getImage());
        existing.setThumbnail(update.getThumbnail());
        existing.setMediaType(update.getMediaType());
        existing.setSpecialization(update.getSpecialization());
        existing.setRelatedLinks(
                merge
                ? merge(existing.getRelatedLinks(), update.getRelatedLinks())
                : update.getRelatedLinks()
        );
        existing.setAliases(
                merge
                ? merge(existing.getAliases(), update.getAliases())
                : update.getAliases()
        );
        existing.setTopicRefs(
                merge ?
                merge(existing.getTopicRefs(), update.getTopicRefs()) :
                update.getTopicRefs()
        );
        existing.setPeople(
                merge
                ? merge(existing.people(), update.people())
                : update.people()
        );
        existing.setKeyPhrases(update.getKeyPhrases());
        existing.setClips(
                merge
                ? merge(existing.getClips(), update.getClips())
                : update.getClips()
        );
        existing.setPriority(update.getPriority());
        existing.setEventRefs(
                merge
                ? merge(existing.events(), update.events())
                : update.events()
        );
        existing.setImages(
                merge
                ? merge(existing.getImages(), update.getImages())
                : update.getImages()
        );
        existing.setAwards(
                merge
                ? merge(existing.getAwards(), update.getAwards())
                : update.getAwards());
        existing.setPresentationChannel(update.getPresentationChannel());
        existing.setYear(update.getYear());

        if (existing instanceof Episode && update instanceof Episode) {
            episodeMerger.mergeEpisodes((Episode) existing, (Episode) update);
        }
        if (existing instanceof Item && update instanceof Item) {
            return itemMerger.mergeItems(
                    itemMerger.mergeReleaseDates((Item) existing, (Item) update, merge),
                    (Item) update,
                    merge,
                    broadcastMerger
            );
        }
        return existing;
    }

    private <T> Set<T> merge(Set<T> existing, Set<T> posted) {
        return ImmutableSet.copyOf(Iterables.concat(posted, existing));
    }

    private <T> List<T> merge(List<T> existing, List<T> posted) {
        return ImmutableSet.copyOf(Iterables.concat(posted, existing)).asList();
    }
}
