package kissmydisc.repricer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class InventoryLoader {
    public static void main(String[] args) throws Exception {
        String file = "C:\\Users\\Sridhar\\Desktop\\Elance\\RepricerEngine\\test\\InventoryLoaderSamples\\AJP Inventory Loader Sample.csv";

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));

        String line = reader.readLine();

        String[] header = line.split("\t");

        Map<Integer, String> map = new HashMap<Integer, String>();

        for (int i = 0; i < header.length; i++) {
            map.put(i, header[i]);
        }

        while (true) {
            line = reader.readLine();
            if (line == null)
                break;
            String[] parts = line.split("\t");
            for (int i = 0; i < parts.length; i++) {
                if (map.size() > i) {
                    System.out.println(map.get(i) + ": " + parts[i]);
                } else {
                    System.out.println("None: " + parts[i]);
                }
            }
            System.out.println("\n\n");
        }

    }
}
