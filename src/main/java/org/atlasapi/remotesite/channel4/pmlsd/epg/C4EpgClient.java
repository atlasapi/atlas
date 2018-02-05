package org.atlasapi.remotesite.channel4.pmlsd.epg;

import java.util.List;

import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.NodeFactory;
import nu.xom.Nodes;
import nu.xom.XPathContext;

import org.atlasapi.persistence.system.RemoteSiteClient;
import org.atlasapi.remotesite.channel4.pmlsd.XmlClient;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4EpgEntry;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4EpgEntryElement;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4MediaContentElement;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4MediaGroupElement;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.http.SimpleHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C4EpgClient implements RemoteSiteClient<List<C4EpgEntry>>{

    Logger log = LoggerFactory.getLogger(C4EpgClient.class);

    private static final String ATOM_FEED_ATOM_ENTRY_XPATH = "//atom:feed/atom:entry";
    private static final XPathContext xPathContext = new XPathContext("atom", "http://www.w3.org/2005/Atom");

    private XmlClient client;

    public C4EpgClient(SimpleHttpClient client) {
        this.client = new XmlClient(client, new Builder(new C4EpgElementFactory()));
    }
    
    @Override
    public List<C4EpgEntry> get(String uri) throws Exception {
        Document d = client.get(uri);
        log.info("%s", d.toXML());
        return getEntries(client.get(uri));
    }

    private List<C4EpgEntry> getEntries(Document scheduleDocument) {
        Nodes entryNodes = scheduleDocument.query(ATOM_FEED_ATOM_ENTRY_XPATH, xPathContext);
        
        ImmutableList.Builder<C4EpgEntry> entries = ImmutableList.builder();
        
        for (int i = 0; i < entryNodes.size(); i++) {
            C4EpgEntry entry = C4EpgEntry.from((C4EpgEntryElement) entryNodes.get(i));
            entries.add(entry);
        }
        
        return entries.build();
    }
    
    private static class C4EpgElementFactory extends NodeFactory {

        @Override
        public Element startMakingElement(String name, String namespace) {
            if(name.equals("entry")) {
                return new C4EpgEntryElement(name, namespace);
            }
            if(name.equals("media:group")) {
                return new C4MediaGroupElement(name, namespace);
            }
            if(name.equals("media:content")) {
                return new C4MediaContentElement(name, namespace);
            }
            return super.startMakingElement(name, namespace);
        }
        
    }
}
