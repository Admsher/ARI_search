package com.arexperts;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

public class FileProcessor {


    public static ArticleIndex loadNGramsFromFiles(String directoryPath,  int columnIndex, int filesToProcess, int offsetFileNumber, int nGramLength, int maximumNumberOfNGrams){
        int fileCount = 0;
        int filesProcessed = 0;
        File[] files = getFileList(directoryPath);
        ArticleIndex article = new ArticleIndex(nGramLength, maximumNumberOfNGrams);
        double startTime = System.nanoTime()/1_000_000_000.0;

        for (File file : files) {
            fileCount++;
            double loopStartTime = System.nanoTime()/1_000_000_000.0;
            if (file.getName().toLowerCase().startsWith("._")) {
                System.out.println("Skipping ._ file:" + file.getName());
                continue;
            }
            if (fileCount <= offsetFileNumber) {
                System.out.println("Skipping offset file:" + file.getName() );
                continue;
            }
            if (filesProcessed >= filesToProcess)
            {
                System.out.println("All processed files done! " + (System.nanoTime()/1_000_000_000.0 - startTime));
                break;
            }
            String name = file.getName().toLowerCase();
            if (name.endsWith(".xlsx")){
                processExcelFile(article,file,columnIndex, fileCount);
                filesProcessed++;
            } else if (name.endsWith(".csv")) {
                processCSVFile(article,file,columnIndex, fileCount);
                filesProcessed++;
            } else {
                System.out.println("Skipping : " + file.getName() + " " + (System.nanoTime()/1_000_000_000.0 - startTime));
            }

            System.out.println("File #" + filesProcessed + ". Processed " + file.getName() + " in " +   (System.nanoTime()/1_000_000_000.0 - loopStartTime));
        }

        System.out.println("End:" + (System.nanoTime()/1_000_000_000.0 - startTime));
        return article;
    }

    private static void processCSVFile(ArticleIndex article, File file, int columnIndex, int fileCount){
        try (Reader reader = new FileReader(file.getAbsolutePath());
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {

            for (CSVRecord record : csvParser) {
                if (record.size() <= columnIndex) {
                    continue;
                }
                String articleToAdd = record.get(columnIndex).trim();
                article.addArticle(articleToAdd, file.getName().toLowerCase());
            }
        }
        catch(IOException ex) {
            System.err.println("File '" + file.getName() +  "' caught exception: " + ex.getLocalizedMessage());
        }
        catch(OutOfMemoryError exMemoryError)
        {
            System.err.println("File '" + file.getName() +  "' caught exception: " + exMemoryError.getLocalizedMessage() + " after " + fileCount + " files.");
        }
    }

    private static void processExcelFile(ArticleIndex article, File file, int columnIndex, int fileCount){
        try (XSSFWorkbook workbook = new XSSFWorkbook(file)){
            XSSFSheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.rowIterator();
            do{
                Row row = rowIterator.next();
                //1 cell is 1 column I believe
                if (row.getPhysicalNumberOfCells() <= columnIndex){
                    continue;
                }
                //Read cell data from specified column
                Cell cell = row.getCell(columnIndex);
                DataFormatter formatter = new DataFormatter();
                String articleData = formatter.formatCellValue(cell);
                article.addArticle(articleData,file.getName().toLowerCase());
            }while (rowIterator.hasNext());
        }catch (IOException | InvalidFormatException ex){
            System.err.println("File '" + file.getName() +  "' caught exception: " + ex.getLocalizedMessage());
        }catch (OutOfMemoryError exMemoryError){
            System.err.println("File '" + file.getName() +  "' caught exception: " + exMemoryError.getLocalizedMessage() + " after " + fileCount + " files.");
        }
    }



    public static File[] getFileList(String directoryPath)
    {
        File directory = new File(directoryPath);
        if (!directory.exists()){
            System.err.println("Given path '" + directoryPath + "' does not exist.");
            return null;
        }
        if (!directory.isDirectory()) {
            System.err.println("Given path '" + directoryPath + "' is not a directory.");
            return null;
        }
        File[] files = directory.listFiles();
        if (files == null) {
            System.err.println("No files found in directory");
            return null;
        }

        return files;
    }
}
