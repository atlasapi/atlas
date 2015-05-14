package org.atlasapi.remotesite;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.feeds.lakeview.LakeviewModule;
import org.atlasapi.remotesite.amazonunbox.AmazonUnboxModule;
import org.atlasapi.remotesite.archiveorg.ArchiveOrgModule;
import org.atlasapi.remotesite.bbc.BbcModule;
import org.atlasapi.remotesite.bbc.audience.AudienceModule;
import org.atlasapi.remotesite.bbc.products.BBCProductsModule;
import org.atlasapi.remotesite.bt.channels.BtChannelsModule;
import org.atlasapi.remotesite.bt.events.BtEventsModule;
import org.atlasapi.remotesite.btfeatured.BtFeaturedContentModule;
import org.atlasapi.remotesite.btvod.BtVodModule;
import org.atlasapi.remotesite.channel4.C4Module;
import org.atlasapi.remotesite.channel4.pmlsd.C4PmlsdModule;
import org.atlasapi.remotesite.five.FiveModule;
import org.atlasapi.remotesite.getty.GettyModule;
import org.atlasapi.remotesite.hulu.HuluModule;
import org.atlasapi.remotesite.itunes.ItunesModule;
import org.atlasapi.remotesite.itv.ItvModule;
import org.atlasapi.remotesite.itv.interlinking.ItvInterlinkingModule;
import org.atlasapi.remotesite.itv.whatson.ItvWhatsOnModule;
import org.atlasapi.remotesite.knowledgemotion.KnowledgeMotionModule;
import org.atlasapi.remotesite.lovefilm.LoveFilmModule;
import org.atlasapi.remotesite.metabroadcast.MetaBroadcastModule;
import org.atlasapi.remotesite.music.emipub.EmiPubModule;
import org.atlasapi.remotesite.music.musicbrainz.MusicBrainzModule;
import org.atlasapi.remotesite.netflix.NetflixModule;
import org.atlasapi.remotesite.opta.events.OptaEventsModule;
import org.atlasapi.remotesite.pa.PaModule;
import org.atlasapi.remotesite.preview.PreviewNetworksModule;
import org.atlasapi.remotesite.redux.ReduxModule;
import org.atlasapi.remotesite.rovi.RoviModule;
import org.atlasapi.remotesite.rte.RteModule;
import org.atlasapi.remotesite.space.TheSpaceModule;
import org.atlasapi.remotesite.talktalk.TalkTalkModule;
import org.atlasapi.remotesite.thesun.TheSunModule;
import org.atlasapi.remotesite.tvblob.TVBlobModule;
import org.atlasapi.remotesite.voila.VoilaModule;
import org.atlasapi.remotesite.wikipedia.WikipediaModule;
import org.atlasapi.remotesite.worldservice.WorldServicesModule;
import org.atlasapi.remotesite.youview.YouViewModule;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;

public class RemoteSiteModuleConfigurer {

    private static Log logger = LogFactory.getLog(RemoteSiteModuleConfigurer.class);

    private Map<String, Class<?>> moduleMap = ImmutableMap.<String, Class<?>>builder()
        .put("bbc",     BbcModule.class)
        .put("itv",     ItvModule.class)
        .put("c4",      C4Module.class)
        .put("c4pmlsd", C4PmlsdModule.class)
        .put("five",    FiveModule.class)
 //       .put("ict",     ICTomorrowModule.class)
        .put("aorg",    ArchiveOrgModule.class)
        .put("hulu",    HuluModule.class)
 //       .put("hbo",     HboModule.class)
        .put("itunes",  ItunesModule.class)
 //       .put("msn",     MsnVideoModule.class)
        .put("pa",      PaModule.class)
  //      .put("youtube", YouTubeModule.class)
        .put("tvblob",  TVBlobModule.class)
        .put("preview", PreviewNetworksModule.class)
        .put("itvinterlinking", ItvInterlinkingModule.class)
        .put("worldservice", WorldServicesModule.class)
        .put("lakeview", LakeviewModule.class)
        .put("redux", ReduxModule.class)
        .put("voila", VoilaModule.class)
        .put("lovefilm", LoveFilmModule.class)
        .put("unbox", AmazonUnboxModule.class)
        .put("netflix", NetflixModule.class)
        .put("youview", YouViewModule.class)
        .put("bbc-products", BBCProductsModule.class)
        .put("thespace", TheSpaceModule.class)
        .put("musicbrainz", MusicBrainzModule.class)
        .put("emipub", EmiPubModule.class)
        .put("metabroadcast", MetaBroadcastModule.class)
        .put("btfeatured", BtFeaturedContentModule.class)
        .put("talktalk", TalkTalkModule.class)
        .put("itvwhatson", ItvWhatsOnModule.class)
        .put("wikipedia", WikipediaModule.class)
        .put("thesuntvpicks", TheSunModule.class)
        .put("rovi", RoviModule.class)
        .put("rte", RteModule.class)
        .put("bt-channels", BtChannelsModule.class)
        .put("bbc-audience-data", AudienceModule.class)
        .put("btvod", BtVodModule.class)
        .put("knowledgemotion", KnowledgeMotionModule.class)
        .put("getty", GettyModule.class)
        .put("bt-events", BtEventsModule.class)
        .put("opta-events", OptaEventsModule.class)
    .build();

    public Iterable<? extends Class<?>> enabledModules() {
        if(Configurer.get("updaters.all.enabled").toBoolean()) {
            return moduleMap.values();
        }
        
        List<Class<?>> enabledModules = Lists.newArrayList();
        
        for (Entry<String,Class<?>> moduleEntry : moduleMap.entrySet()) {
            String moduleName = moduleEntry.getKey();
            
            Parameter moduleParam = Configurer.get("updaters."+moduleName+".enabled");
            
            if(moduleParam != null && moduleParam.toBoolean()) {
                enabledModules.add(moduleEntry.getValue());
                logger.info("Including module " + moduleName);
            } else {
                logger.info("Not including module " + moduleName);
            }
        }
        
        return enabledModules;
    }
 
    
}
