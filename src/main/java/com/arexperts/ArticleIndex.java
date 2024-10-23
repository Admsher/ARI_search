package com.arexperts;

import java.util.*;
import java.io.Serializable;

public class ArticleIndex implements Serializable {
    private int n;
    private Map<Integer, List<Integer>> ngramTable;
    private Map<Integer, String> keyTable;
    private int articlesAdded = 0;
    private int maximumNumberOfNGrams;

    public ArticleIndex(int n, int maximumNumberOfNGrams) {
        this.n = n;
        this.ngramTable = new HashMap<>();
        this.keyTable = new HashMap<>();
        this.maximumNumberOfNGrams = maximumNumberOfNGrams;
    }


    /**
     * Return the number of articles added to the index.
    
     */
    public int NumberOfArticles() {
        return articlesAdded;
    }



     /**
      * Adds an article to the index.
      * @param s the text of the article to add
      * @param key the key to associate with the article
      */
     public void addArticle(String s, String key) {
        articlesAdded++;
        Set<Integer> grams = getNGrams(s, n);
        int h = key.hashCode();
        // Store the article key in the key table
        keyTable.put(h, key);
        // Add the article to the ngram table
        for (Integer g : grams) {
            // Create a new list for the ngram if it doesn't exist
            ngramTable.computeIfAbsent(g, k -> new ArrayList<>()).add(h);
        }
    }




    /**
     * Finds the best match for a given string in the index.
     * @param s the string to search for
     * @return an array of two strings. The first element is the key of the best match, and the second element is a string version of the score.
     */
    
    // Get the ngrams for the search string
    public String[] findMatch(String s) {        
        Set<Integer> grams = getNGrams(s, n);
        // Initialize lists to store the hits and scores
        List<Integer> hits = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        // Iterate over the ngrams
        for (Integer g : grams) {
            // Get the list of article keys associated with the ngram
            List<Integer> found = ngramTable.get(g);
            // If the list is not empty, add it to the hits and scores lists
            if (found != null && !found.isEmpty()) {
                hits.addAll(found);
                // Calculate the score
                double score = 1.0 / found.size();
                // Add the score to the scores list
                for (int i = 0; i < found.size(); i++) {
                    scores.add(score);
                }
            }
        }
        // If there were no hits, return an empty array
        if (hits.isEmpty()) {
            return new String[] { null, "0" };
        }
        // Calculate the total score for each article
        Map<Integer, Double> totals = new HashMap<>();
        for (int i = 0; i < hits.size(); i++) {
            int hit = hits.get(i);
            double score = scores.get(i);
            totals.put(hit, totals.getOrDefault(hit, 0.0) + score);
        }
        // Find the article with the highest score
        int maxKey = Collections.max(totals.entrySet(), Comparator.comparingDouble(Map.Entry::getValue)).getKey();
        double maxValue = totals.get(maxKey);
        // Return the key and score as an array
        return new String[] { keyTable.get(maxKey), String.valueOf(maxValue)};
    }



    /**
     * Generates a set of ngrams from a given string. The ngrams are generated
     * by taking all substrings of length n from the input string, and then
     * hashing them. The resulting set of ngrams is returned as a Set of
     * Integers.
     * 
     * @param s the string to generate ngrams from
     * @param n the length of the ngrams
     * @return a set of ngrams
     */
    public Set<Integer> getNGrams(String s, int n) {
        // Remove all non-letter characters and convert to lower case
        String s_letters_only = s.replaceAll("[^a-zA-Z0-9\\s++]", "").toLowerCase();
        // Split the string into words
        String[] words = s_letters_only.split("\\s+");
        // Create a set to store the ngrams
        Set<Integer> grams = new HashSet<>();
        // Initialize the number of ngrams found
        int foundNGrams = 0;
        // Iterate over the words
        for (int i = 0; i <= words.length - n; i++) {
            // Create a string buffer to build the ngram
            StringBuilder sb = new StringBuilder();
            // Iterate over the next n words
            for (int j = 0; j < n; j++) {
                // Append the word to the string buffer
                sb.append(words[i + j]);
                // If this is not the last word, append a space
                if (j < n - 1) {
                    sb.append(" ");
                }
            }
            // Generate the hash code for the ngram
            Integer hashCode = sb.toString().hashCode();
            // Add the ngram to the set
            grams.add(hashCode);
            // Increment the number of ngrams found
            foundNGrams++;
            // If the maximum number of ngrams has been reached, break
            if (foundNGrams >= maximumNumberOfNGrams) {
                break;
            }
        }
        // Return the set of ngrams
        return grams;
    }



    /**
     * Compares two strings by calculating the intersection of their ngrams.
     * Returns a string describing the result of the comparison.
     * 
     * @param s1 the first string
     * @param s2 the second string
     * @return a string describing the result of the comparison
     */
    public String validateMatch(String s1, String s2) {
        // Get the ngrams for the two strings
        Set<Integer> ng1 = getNGrams(s1, 5);
        Set<Integer> ng2 = getNGrams(s2, 5);
        // Calculate the intersection of the two sets of ngrams
        Set<Integer> intersection = new HashSet<>(ng1);
        intersection.retainAll(ng2);
        int inter = intersection.size();
        // Calculate the score for each string
        double score1 = (double) inter / ng1.size();
        double score2 = (double) inter / ng2.size();
        // Determine if the strings are a match
        boolean isMatch = Math.max(score1, score2) > 0.5 && Math.min(score1, score2) > 0.2;
        // Return a string describing the result of the comparison
        return "Matched with " + inter + " score with intersection of " + score1 + " and " + score2 + ". Verdict is "
                + isMatch;
    }

}
