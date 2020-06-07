import com.github.javafaker.Faker;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class Generator {
    static Faker faker = new Faker();

    static int maxTax = 1000000;
    static int minTax = 2500;
    static int minYear = 2000;
    static int maxYear = 2020;
    static List<String> zipcodes = new ArrayList<>();

    static void setCsvFile(File csvFile) throws IOException {
        if (csvFile.exists()) {
            String csv = FileUtils.readFileToString(csvFile, "UTF-8");
            String[] rows = csv.split("\n");
            for (String row : rows) {
                zipcodes.add(row.split(",")[0].trim());
            }
        } else {
            throw new IllegalArgumentException("Please provide absolute path to csv file");
        }
    }

    public static String getRandomdata() {
        return getId() + "," + getName() + "," + getTaxAmount() + "," + getYear() + "," + zipCode() + "," + getNumberOfHouseHold();
    }

    static int getNumberOfHouseHold() {
        Random random = new Random(2000);
        return random.nextInt((15000 - 1500) + 1) + 1500;
    }

    static String zipCode() {
        Random r = new Random();
        int index = r.nextInt(zipcodes.size() + 1000);
        return index >= zipcodes.size() ? "" : zipcodes.get(index);
    }

    static String getYear() {
        Random r = new Random();
        int random = r.nextInt((maxYear - minYear) + 1) + minYear;
        return random > maxYear || random < minYear ? "" : String.valueOf(random);
    }

    static String getId() {
        return UUID.randomUUID().toString();
    }

    static String getName() {
        return faker.gameOfThrones().character();
    }

    static String getTaxAmount() {
        Random random = new Random(2000);
        int rand = random.nextInt((maxTax + 10000 - minTax) + 1) + minTax;
        return rand > maxTax || rand < minTax ? "" : String.valueOf(rand);
    }
}
