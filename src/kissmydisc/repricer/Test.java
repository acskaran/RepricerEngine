package kissmydisc.repricer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.Character.UnicodeBlock;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Test {

    public static void main(String[] args) throws Exception {

        String file = "C:\\Users\\Sridhar\\Desktop\\Sample Afr Inv.txt";

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "ISO8859-1"));

        String line = reader.readLine();

        String[] parts = line.split("\t");

        String sellersku = "";

        int i = 0;
        for (String part : parts) {
            if (part.equals("r\u00e9f-produit"))
                System.out.println("PART equal");
            if (part.equals("quantit\u00e9"))
                System.out.println(part + " equal");
            System.out.println(i++ + " " + part);
        }
        line = reader.readLine();
        parts = line.split("\t");
        i = 0;
        for (String part : parts) {
            if (sellersku.equals(part))
                System.out.println("SKU");
            System.out.println(i++ + "  " + part);
        }

        System.out.println(Charset.availableCharsets());
        System.out.println(Charset.isSupported("Windows-932"));
        System.out.println(Charset.isSupported("Shift_JIS"));
        System.out.println(Charset.isSupported("ISO8859-1"));

    }
}
