package server.ratis;

import org.apache.ratis.server.protocol.TermIndex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DIMetadata {

    private Map<String, Integer> wordFrequencyMap = new HashMap<>();

    private TermIndex latestAppliedTermIndex;

    public TermIndex getLatestAppliedTermIndex() {
        return latestAppliedTermIndex;
    }

    public synchronized int getSingleWordFrequency(String word) {
        return wordFrequencyMap.getOrDefault(word, 0);
    }

    public void insertDocumentAndUpdateTermIndex(String document, TermIndex termIndex) {
        String[] words = document.split(" ");

        for(String word : words) {
            wordFrequencyMap.put(word, 1 + wordFrequencyMap.getOrDefault(word, 0));
        }

        latestAppliedTermIndex = termIndex;
    }

    public void updateFrequencyMapAndTermIndex(Map<String, Integer> map, TermIndex termIndex) {
        wordFrequencyMap = map;
        latestAppliedTermIndex = termIndex;
    }

    List<String> getAllWordFrequencies() {
        List<String> res = new ArrayList<>();

        for(Map.Entry<String, Integer> entry : wordFrequencyMap.entrySet()) {
            res.add(entry.getKey() + "_" + entry.getValue());
        }

        return res;
    }
}
