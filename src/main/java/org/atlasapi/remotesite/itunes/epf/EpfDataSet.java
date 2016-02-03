package org.atlasapi.remotesite.itunes.epf;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.Reader;
import java.util.Locale;

import org.atlasapi.remotesite.itunes.epf.model.EpfArtist;
import org.atlasapi.remotesite.itunes.epf.model.EpfArtistCollection;
import org.atlasapi.remotesite.itunes.epf.model.EpfCollection;
import org.atlasapi.remotesite.itunes.epf.model.EpfCollectionVideo;
import org.atlasapi.remotesite.itunes.epf.model.EpfPricing;
import org.atlasapi.remotesite.itunes.epf.model.EpfStorefront;
import org.atlasapi.remotesite.itunes.epf.model.EpfVideo;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.metabroadcast.common.intl.Country;

public class EpfDataSet {

    private static final String TAB_FIELD_SEPARATOR = "\t";
    private static final String EMPTY_ROW_SEPARATOR = null;
    private final File datasetDirectory;

    public EpfDataSet(File datasetDirectory) {
        checkArgument(checkNotNull(datasetDirectory).isDirectory());
        this.datasetDirectory = datasetDirectory;
    }

    public EpfTable<EpfArtist> getArtistTable() {
        return new EpfTable<EpfArtist>(readerSupplierFor("artist"), EpfArtist.FROM_ROW_PARTS);
    }
    
    public EpfTable<EpfArtistCollection> getArtistCollectionTable() {
        return new EpfTable<EpfArtistCollection>(readerSupplierFor("artist_collection"), EpfArtistCollection.FROM_ROW_PARTS);
    }
    
    public EpfTable<EpfCollection> getCollectionTable() {
        return new EpfTable<EpfCollection>(readerSupplierFor("collection"), EpfCollection.FROM_ROW_PARTS);
    }
    
    public EpfTable<EpfCollectionVideo> getCollectionVideoTable() {
        return new EpfTable<EpfCollectionVideo>(readerSupplierFor("collection_video"), EpfCollectionVideo.FROM_ROW_PARTS);
    }
    
    public EpfTable<EpfVideo> getVideoTable() {
        return new EpfTable<EpfVideo>(readerSupplierFor("video"), EpfVideo.FROM_ROW_PARTS);
    }
    
    public EpfTable<EpfPricing> getPricingTable() {
        return new EpfTable<EpfPricing>(readerSupplierFor("video_price"), EpfPricing.FROM_ROW_PARTS);
    }

    public EpfTable<EpfStorefront> getCountryCodes() {
        return new EpfTable<EpfStorefront>(readerSupplierFor("storefront"), EpfStorefront.FROM_ROW_PARTS);
    }

    private InputSupplier<? extends Reader> readerSupplierFor(String fileName) {
        return Files.newReaderSupplier(new File(datasetDirectory, fileName), Charsets.UTF_8);
    }
}
