package org.atlasapi.equiv.handlers;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import com.google.common.collect.Multimap;

/**
 * Created by adam on 27/07/2016.
 */
public class PLEquivalenceResultHandler implements EquivalenceResultHandler {


    private String titleAddOn;
    private EquivalenceResultStore store;

    public PLEquivalenceResultHandler(String titleAddOn) {

        this.titleAddOn = titleAddOn;
        this.store = new PLFileEquivalenceResultStore(titleAddOn);
    }


    @Override
    public void handle(EquivalenceResult result) {

        store.store(result);

//        try {
//            convert(result);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    private void convert(EquivalenceResult<Item> result) throws IOException {
        String filename = "output/" + titleAddOn + "-" + result.subject().getId() + ".txt";
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        FileWriter writer = new FileWriter(filename);
        CSVPrinter printer = new CSVPrinter(writer, csvFileFormat);





        Item subject = result.subject();
        List<String> subjectList = new LinkedList();
        subjectList.add(subject.getTitle());
        subjectList.add(subject.getCanonicalUri());
        printer.printRecord(subjectList);

        List<String> header = new LinkedList<>();
        header.add("Title");
        header.add("Score");
        printer.printRecord(header);

        // get those with strong equivalence
//        Multimap<Publisher, ScoredCandidate<Item>> publisherCandidateMap = result.strongEquivalences();
//        List<Collection<ScoredCandidate<Item>>> list = new LinkedList();
//        for (Publisher key: publisherCandidateMap.keySet()) {
//            list.add(publisherCandidateMap.get(key));
//        }
//
//        for (Collection<ScoredCandidate<Item>> candidateCollection: list) {
//            for (ScoredCandidate<Item> candidate : candidateCollection) {
//                Item item = candidate.candidate();
//                Score score = candidate.score();
//                List<String> infoList = new LinkedList<>();
//                infoList.add(item.getTitle());
//                infoList.add(Double.valueOf(score.toString()).toString());
//
//                printer.printRecord(infoList);
//            }
//
//        }

        // get list of candidates and scores and print them to csv file
        List<ScoredCandidates<Item>> scoredCandidatesList = result.rawScores();
        for (ScoredCandidates<Item> scoredCandidates : scoredCandidatesList) {
            Map<Item, Score> scoredCandidatesMap = scoredCandidates.candidates();
            for (Item key: scoredCandidatesMap.keySet()) {
                List<String> printingList = new LinkedList<>();
                printingList.add(key.getTitle());
                printingList.add(Double.valueOf(scoredCandidatesMap.get(key).toString()).toString());
                printer.printRecord(printingList);
            }
        }


        writer.flush();
        writer.close();
        printer.close();


    }

}
