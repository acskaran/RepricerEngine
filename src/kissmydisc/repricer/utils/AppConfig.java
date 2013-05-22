package kissmydisc.repricer.utils;

import java.io.FileInputStream;
import java.util.Properties;

public class AppConfig {

    private static final Properties properties = new Properties();

    public static void initialize(String propertiesFile) throws Exception {
        if (propertiesFile != null) {
            FileInputStream props = new FileInputStream(propertiesFile);
            try {
                properties.load(props);
            } finally {
                if (props != null) {
                    props.close();
                }
            }
        }
    }

    public static String getString(String key) {
        return properties.getProperty(key);
    }

    public static int getInteger(String key, int defaultVal) {
        if (properties.containsKey(key)) {
            return Integer.parseInt(properties.getProperty(key));
        }
        return defaultVal;
    }

    public static boolean getBoolean(String key, boolean defaultVal) {
        if (properties.containsKey(key)) {
            return Boolean.valueOf(properties.getProperty(key));
        }
        return defaultVal;
    }

    public static double getBoolean(String key, double defaultVal) {
        if (properties.containsKey(key)) {
            return Double.valueOf(properties.getProperty(key));
        }
        return defaultVal;
    }

    public static void main(String[] args) throws Exception {
        initialize("Repricer.properties");
        System.out.println(getString("DatabaseName"));
        System.out.println(getInteger("DatabasePort", 1));
        System.out.println(getBoolean("ShouldSubmitFeedToAmazon", false));
    }

}