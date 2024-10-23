package com.arexperts;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;


public class TestReadSpeed {

    // Starting time in nanoseconds for measuring elapsed time
    private static final double startTime = System.nanoTime();

    // Method to read a CSV file and process each record
    private static void doRead() {
        int row = 0;        
        File file = new File("test.csv");
        ArticleIndex articles = new ArticleIndex(5, 100);
        try (Reader reader = new FileReader(file.getAbsolutePath());
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {
        

            // Iterating through each record in the CSV file
            for (CSVRecord record : csvParser) {
                // Extract article text from the specified column and add to ArticleIndex

                String articleToAdd = record.get(6).trim();        
                articles.addArticle(articleToAdd, "row" + row++);
            }
            // Attempt to find a match for a specific text in the articles
            System.out.println(articles.findMatch("It is undoubtedly an opulent and striking pictorial spectacle that Cecil B. DeMille has wrought from Wilson Barrett's famous old stage work, \"\"The Sign of the Cross.\"\" It was offered last night at the Rialto before a brilliant audience, which revealed no little interest in its variety of handsome scenes, its battling gladiators, its orgies in Nero's court, its chanting Christians, its music and its screams.Throughout this really mammoth production the fine DeMillean hand is noticeable. Where there was a chance to touch up episodes it has been done. It is as though Nero were living in the twentieth century, with some of the lines and the squabbling in the Rome arena for places to see the big bill, which includes many combats between Nero's own subjects and scores of Christians and others marching to their deaths. The hungry lions rush up stone steps, eager to get to their human prey, and, just before that, a dying man is supposed to have his head trampled on by an elephant.The principal rôles are all well played, even though they are more or less in the modern manner. But the outstanding histrionic achievement comes from Charles Laughton, who shoulders the responsibility for Nero. He is a petulent Nero, a man who has no thought for other than himself, and when he is asked to grant the life of")[1]); 
        }
        catch(IOException ex) {
            // Handle exception for file reading errors
            System.err.println("File '" + file.getName() +  "' caught exception: " + ex.getLocalizedMessage());
        }
            // Handle exception for memory errors
        catch(OutOfMemoryError exMemoryError)
        {
            System.err.println("File '" + file.getName() +  "' caught exception: " + exMemoryError.getLocalizedMessage());
        }
    }

    public static void main(String[] args) {
        // Repeatedly perform the read operation 100 times
        for (int i = 0; i < 100; i++) {
            doRead();
        }
        // Output the total elapsed time for all operations
        System.out.println("Total elapsed time : " + getElapsedTime());               
    }

    // Calculate and return the elapsed time since startTime
       
    private static double getElapsedTime()
    {        
        return (System.nanoTime() - startTime) / 1_000_000_000.0; // Converts to seconds
    }    
}

