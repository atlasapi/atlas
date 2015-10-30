package org.atlasapi.remotesite.bbc.nitro;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.util.GroupLock;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityOption;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.common.scheduling.ScheduledTask;

public class OffScheduleContentIngestTask extends ScheduledTask {

    private final NitroContentAdapter contentAdapter;
    private final int pageSize;
    private final ContentWriter contentWriter;
    private final LocalOrRemoteNitroFetcher localOrRemoteFetcher;
    private final GroupLock<String> lock;

    public OffScheduleContentIngestTask(
            NitroContentAdapter contentAdapter, int pageSize,
            ContentWriter contentWriter, GroupLock<String> lock,
            LocalOrRemoteNitroFetcher localOrRemoteFetcher
    ) {
        this.localOrRemoteFetcher = checkNotNull(localOrRemoteFetcher);
        this.lock = checkNotNull(lock);
        this.contentWriter = checkNotNull(contentWriter);
        this.contentAdapter = checkNotNull(contentAdapter);
        this.pageSize = pageSize;
    }

    @Override
    protected void runTask() {
        ProgrammesQuery query = ProgrammesQuery
                .builder()
                .withMixins(ProgrammesMixin.TITLES, ProgrammesMixin.PEOPLE)
                .withAvailability(AvailabilityOption.AVAILABLE)
                .withPageSize(pageSize)
                .build();

        ImmutableSet<Item> items;
        try {
            items = contentAdapter.fetchEpisodes(query);

            for (Item item : items) {
                contentWriter.createOrUpdate(item);
            }
        } catch (NitroException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock("");
        }
    }
}
