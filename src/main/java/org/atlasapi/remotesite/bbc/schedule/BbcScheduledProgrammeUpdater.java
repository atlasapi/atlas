package org.atlasapi.remotesite.bbc.schedule;

import java.util.List;

import javax.xml.bind.JAXBException;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;
import org.atlasapi.persistence.system.Fetcher;
import org.atlasapi.persistence.system.RemoteSiteClient;
import org.atlasapi.query.uri.LocalOrRemoteFetcher;
import org.atlasapi.remotesite.bbc.schedule.ChannelSchedule.Programme;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Updater to download advance BBC schedules and get URIplay to load data for the programmes that they include
 *  
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class BbcScheduledProgrammeUpdater implements Runnable {

	private static final String SLASH_PROGRAMMES_BASE_URI = "http://www.bbc.co.uk/programmes/";

	private final RemoteSiteClient<ChannelSchedule> scheduleClient;
	private final Fetcher<Identified> fetcher;

	private final Iterable<String> uris;

	private final AdapterLog log;

	private final ContentWriter writer;

    private final ContentResolver localFetcher;
	
	public BbcScheduledProgrammeUpdater(ContentResolver localFetcher, Fetcher<Identified> remoteFetcher, ContentWriter writer, Iterable<String> uris, AdapterLog log) throws JAXBException {
		this(new BbcScheduleClient(), localFetcher, remoteFetcher, writer, uris, log);
	}
	
	BbcScheduledProgrammeUpdater(RemoteSiteClient<ChannelSchedule> scheduleClient, ContentResolver localFetcher, Fetcher<Identified> remoteFetcher, ContentWriter writer, Iterable<String> uris, AdapterLog log) {
		this.scheduleClient = scheduleClient;
        this.localFetcher = localFetcher;
		this.fetcher = remoteFetcher;
		this.writer = writer;
		this.uris = uris;
		this.log = log;
	}

	private void update(String uri) {
		try {
		    Fetcher<Identified> brandFetcher = new LocalOrRemoteFetcher(localFetcher, fetcher);
			ChannelSchedule schedule = scheduleClient.get(uri);
			List<Programme> programmes = schedule.programmes();
			for (Programme programme : programmes) {
				if (programme.isEpisode()) {
					Item fetchedItem = (Item) fetcher.fetch(SLASH_PROGRAMMES_BASE_URI + programme.pid());
					if(!(fetchedItem instanceof Episode)) {
                        writer.createOrUpdate(fetchedItem);
					} else {
                        Episode fetchedEpisode = (Episode) fetchedItem;
                        Brand brand = fetchedEpisode.getContainer();
                        if (brand == null || Strings.isNullOrEmpty(brand.getCanonicalUri())) {
                            writer.createOrUpdate(fetchedEpisode);
                        } else {
                            Brand fetchedBrand = (Brand) brandFetcher.fetch(brand.getCanonicalUri());
                            if (fetchedBrand != null) {
                                if (!fetchedBrand.getContents().contains(fetchedEpisode)) {
                                    fetchedBrand.addContents(fetchedEpisode);
                                } else {
                                    List<Episode> currentItems = Lists.newArrayList(fetchedBrand.getContents());
                                    currentItems.set(currentItems.indexOf(fetchedEpisode), fetchedEpisode);
                                    fetchedBrand.setContents(currentItems);
                                }
                                writer.createOrUpdate(fetchedBrand, true);
                            }
                        }
					}
				}
			}
		} catch (Exception e) {
			log.record(new AdapterLogEntry(Severity.WARN).withSource(getClass()).withCause(e).withDescription("Exception updating BBC URI " + uri));
		}
		
	}

	@Override
	public void run() {
		log.record(new AdapterLogEntry(Severity.INFO).withSource(getClass()).withDescription("BBC Schedule Updater started"));
		for (String uri : uris) {
			log.record(new AdapterLogEntry(Severity.DEBUG).withSource(getClass()).withDescription("Updating from schedule: " + uri));
			update(uri);
		}
		log.record(new AdapterLogEntry(Severity.INFO).withSource(getClass()).withDescription("BBC Schedule Updater finished"));
	}

}
