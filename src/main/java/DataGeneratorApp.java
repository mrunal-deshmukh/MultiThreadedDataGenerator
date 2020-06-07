import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

public class DataGeneratorApp {


    private static BlockingDeque<String> blockingDeque = new LinkedBlockingDeque<String>();
    private static AtomicLong counter = new AtomicLong();

    public static class DataCreator implements Runnable {
        private CountDownLatch latch;

        public DataCreator(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            while (latch.getCount() > 0) {
                counter.incrementAndGet();
                blockingDeque.add(Generator.getRandomdata());
            }
        }
    }

    public static class DataFileWriter implements Runnable {
        private final int index;
        private File outputDir;
        private int maxSize;
        private CountDownLatch latch;
        private final File f;
        private List<String> dataList;

        DataFileWriter(int index, String outputDir, CountDownLatch latch) throws IOException {

            this.index = index;
            this.outputDir = new File(outputDir);
            this.latch = latch;
            f = new File(outputDir + "/" + index + ".csv");
            FileUtils.touch(f);
            dataList = new ArrayList<>();
        }

        @Override
        public void run() {
            while (latch.getCount() > 0) {
                String dataRow = blockingDeque.poll();
                if (dataRow != null) {
                    dataList.add(dataRow);
                    if (dataList.size() > 10000) {
                        try {
                            FileUtils.writeLines(f, dataList, true);
                            dataList.clear();
                        } catch (IOException e) {
                            System.out.println("exception occured while writing to file" + f.getAbsolutePath());
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("o", true, "output directory, will be created if not present");
        options.addOption("s", true, "provide size in GB to generate data");
        options.addOption("c", true, "provide csv file uszips.csv");
        // create the parser
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            String outDir = getOutPutDir(line);
            int dataSizeInGB = getDataSize(line);
            String csvFilepath = getCsv(line);
            Generator.setCsvFile(new File(csvFilepath));
            CountDownLatch latch = new CountDownLatch(1);
            for (int i = 0; i < dataSizeInGB * 20; i++) {
                new Thread(new DataFileWriter(i, outDir, latch)).start();
                new Thread(new DataCreator(latch)).start();
            }
            TimerTask task = getTimer(outDir, latch, dataSizeInGB);
            Timer timer = new Timer("fileSizeTimer");
            long delay = 10000L;
            timer.scheduleAtFixedRate(task, 0L, delay);
            while (latch.getCount() > 0) {
                blockingDeque.add(Generator.getRandomdata());
            }
            timer.cancel();
            System.out.println("Generation Complete");
        } catch (ParseException | IOException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
        }
    }

    /**
     * 1073741824 bytes is 1 GB.
     *
     * @param outDir
     * @param latch
     * @param totalSizeRequiredInGB
     * @return
     */
    private static TimerTask getTimer(String outDir, CountDownLatch latch, int totalSizeRequiredInGB) {
        final long requiredSizeInBytes = 1073741824L * totalSizeRequiredInGB;
        final long now = System.currentTimeMillis();
        TimerTask task = new TimerTask() {
            public void run() {
                try {
                    long dirSize = getDirSize(outDir);
                    System.out.println("Current Data Generated(in Bytes):" + dirSize);
                    Double minutes = (System.currentTimeMillis() - now) / 60000d;
                    System.out.println("Total Time Elapsed(in Minutes)" + BigDecimal.valueOf(minutes).setScale(2, RoundingMode.CEILING));
                    if (dirSize >= requiredSizeInBytes) {
                        latch.countDown();
                    }
                } catch (IOException e) {
                    latch.countDown();
                    e.printStackTrace();
                }

            }
        };
        return task;
    }

    private static String getCsv(CommandLine line) {
        if (line.hasOption("c") && new File(line.getOptionValue("c")).exists()) {
            return line.getOptionValue("c");
        } else {
            throw new IllegalArgumentException("Please provide uszip files.");
        }
    }

    private static int getDataSize(CommandLine line) {
        if (line.hasOption("s") && NumberUtils.isParsable(line.getOptionValue("s"))) {
            return Integer.parseInt(line.getOptionValue("s"));
        }
        return 10;
    }

    private static String getOutPutDir(CommandLine line) throws IOException {
        if (line.hasOption("o")) {
            String outPutDirectory = line.getOptionValue("o");
            File file = new File(outPutDirectory);
            if (!file.exists()) {
                FileUtils.forceMkdir(file);
                System.out.println("Creating Directory:" + outPutDirectory);
            } else {
                System.out.println("Cleaning files from " + outPutDirectory);
                FileUtils.cleanDirectory(file);
            }
            return outPutDirectory;
        }
        throw new IllegalArgumentException("Please provide output directory absolute path");
    }

    private static long getDirSize(String outDir) throws IOException {
        Path folder = Paths.get(outDir);
        return Files.walk(folder)
                .filter(p -> p.toFile().isFile())
                .mapToLong(p -> p.toFile().length())
                .sum();
    }
}
