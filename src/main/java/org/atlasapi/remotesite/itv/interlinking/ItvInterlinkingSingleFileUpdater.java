package org.atlasapi.remotesite.itv.interlinking;

import static org.atlasapi.remotesite.itv.interlinking.ItvInterlinkingContentExtractor.ATOM_NS;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Elements;

import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.HttpClients;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpRequest;

public class ItvInterlinkingSingleFileUpdater {

    private static final String BASE_URL = "http://mercury.itv.com/linking/";
    private final DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyyMMdd");
    
    private final SimpleHttpClient webserviceClient = HttpClients.webserviceClient();
    private final ContentWriter contentWriter;
    private final ItvInterlinkingContentExtractor contentExtractor;
    
    public ItvInterlinkingSingleFileUpdater(ContentWriter contentWriter, ItvInterlinkingContentExtractor contentExtractor) {
        this.contentWriter = contentWriter;
        this.contentExtractor = contentExtractor;
    }
    
    public void processUri(String uri) {
        try {
            Document document = webserviceClient.get(new SimpleHttpRequest<Document>(uri, XomResponseTransformer.standard()));
            
            Elements entryElements = document.getRootElement().getChildElements("entry", ATOM_NS);
            
            ItvInterlinkingEntryProcessor entryProcessor = new ItvInterlinkingEntryProcessor(contentWriter, contentExtractor);
            
            for (int i = 0; i < entryElements.size(); i++) {
                Element entryElement = entryElements.get(i);
                entryProcessor.processEntry(entryElement);
            }
            
            entryProcessor.processAllEntries();
            
        } catch (HttpException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public String getUrlForDate(DateTime date) {
        return BASE_URL + dateFormat.print(date);
    }
}
