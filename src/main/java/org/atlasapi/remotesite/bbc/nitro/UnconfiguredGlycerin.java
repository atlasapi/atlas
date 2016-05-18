package org.atlasapi.remotesite.bbc.nitro;

import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.MasterBrand;
import com.metabroadcast.atlas.glycerin.model.Programme;
import com.metabroadcast.atlas.glycerin.model.Service;
import com.metabroadcast.atlas.glycerin.model.Version;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityQuery;
import com.metabroadcast.atlas.glycerin.queries.BroadcastsQuery;
import com.metabroadcast.atlas.glycerin.queries.MasterBrandsQuery;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.atlas.glycerin.queries.ServicesQuery;
import com.metabroadcast.atlas.glycerin.queries.VersionsQuery;


public class UnconfiguredGlycerin implements Glycerin {

    private static final Glycerin instance = new UnconfiguredGlycerin();
    
    public static final Glycerin get() {
        return instance;
    }
    
    private UnconfiguredGlycerin() { }
    
    private UnsupportedOperationException unconfigured() {
        return new UnsupportedOperationException("not configured");
    }

    @Override
    public GlycerinResponse<Programme> execute(ProgrammesQuery query) throws GlycerinException {
        throw unconfigured();
    }

    @Override
    public GlycerinResponse<Availability> execute(AvailabilityQuery query) throws GlycerinException {
        throw unconfigured();
    }

    @Override
    public GlycerinResponse<Broadcast> execute(BroadcastsQuery query) throws GlycerinException {
        throw unconfigured();
    }

    @Override
    public GlycerinResponse<Service> execute(ServicesQuery query) throws GlycerinException {
        throw unconfigured();
    }

    @Override
    public GlycerinResponse<Version> execute(VersionsQuery query) throws GlycerinException {
        throw unconfigured();
    }

    @Override
    public GlycerinResponse<MasterBrand> execute(MasterBrandsQuery query) throws GlycerinException {
        throw unconfigured();
    }

}
