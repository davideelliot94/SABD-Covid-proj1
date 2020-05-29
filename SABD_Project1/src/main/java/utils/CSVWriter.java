package utils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CSVWriter {

    public static void writeCSVFile(List<String> collect, String outputPath, String[] newHeader) throws IOException {

        FileWriter fw = new FileWriter(outputPath);

        fw.write(Arrays.toString(newHeader).replace("[","").replace("]","")+"\n");


        for (String c : collect
        ) {

            String newLine = c +"\n";
            fw.write(newLine);
        }
        fw.close();

    }
}