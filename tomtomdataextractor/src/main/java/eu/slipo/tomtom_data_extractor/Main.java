package eu.slipo.tomtom_data_extractor;

public class Main {

    public static void main(String[] args) {
        System.out.println("TomTom Data Extractor");
        TomTomExtractor tte = new TomTomExtractor();
        // String inputFile = "/home/ubuntu16/Documents/slipo/sample_tomtom_pois.nt";
        String inputFile = "/home/ubuntu16/Documents/slipo/tomtom_pois_austria_v0.3_unique.nt";
        // String outputFile = "/home/ubuntu16/Documents/slipo/sample_tomtom_pois.csv";
        String outputFile = "/home/ubuntu16/Documents/slipo/tomtom_pois_austria_v0.3_unique.csv";
        String categoriesFile = "/home/ubuntu16/Documents/slipo/categories.txt";
        tte.processTomTomData(inputFile, outputFile, categoriesFile);
    }

}
