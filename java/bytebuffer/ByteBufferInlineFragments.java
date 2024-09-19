package bytebuffer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Reads the file into a set of ByteByffers
 * Use byte arrays instead of Strings for the City names.
 * Embed fragments in the ListOfCities to avoid using the ConcurrentHashMaps
 */
public class ByteBufferInlineFragments {

    ExecutorService threadPoolExecutor;

    String file = "measurements.txt";

    final int threads = Runtime.getRuntime().availableProcessors();
    RowFragments rf = new RowFragments();
    static final int BUFFERSIZE = 512 * 1024;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();
        new ByteBufferInlineFragments().go();
        long endTime = System.currentTimeMillis();
        System.out.println("Took " + (endTime - startTime) / 1000 + " s");
    }

    private void go() throws IOException, ExecutionException, InterruptedException {
        System.out.println("Using " + threads + " cores");
        threadPoolExecutor = Executors.newFixedThreadPool(threads);
        Future<?>[] runningThreads = new Future<?>[threads];

        // Keep a spare buffer at the end to load into in the background
        ByteBuffer[] buffers = new ByteBuffer[threads + 1];

        try (RandomAccessFile aFile = new RandomAccessFile(file, "r");
             FileChannel channel = aFile.getChannel()) {

            launchInitialProcesses(buffers, channel, runningThreads);

            // Load the next block in advance, ready for when a thread finishes.
            buffers[threads] = ByteBuffer.allocate(BUFFERSIZE);
            boolean doWeStillHaveData = loadNextBlock(buffers[threads], channel);
            System.out.println("Loaded data");

            int blockNumber = threads; // keep track of which block we are processing
            ListOfCities overallResults = new ListOfCities(0);
            while (doWeStillHaveData) {

                // go through the executors, check if any have finished and
                // re-fill any which have
                for (int i = 0; i < threads; i++) {
                    if (runningThreads[i].isDone()) {
                        // thread finished, collect result and re-fill buffer.
                        mergeAndStoreResults((ListOfCities) runningThreads[i].get(), overallResults);

                        doWeStillHaveData = processNextBlock(buffers, i, blockNumber, runningThreads, channel);
                        blockNumber++;
                        if (!doWeStillHaveData) {
                            break;
                        }
                    }
                } // end for
            } // end while

            waitForThreads(runningThreads, overallResults);
            threadPoolExecutor.close();

            processFragments(overallResults);
            sortAndDisplay(overallResults);
        }
    }

    private static void sortAndDisplay(ListOfCities overallResults) {
        TreeMap<String, Station> sortedCities = new TreeMap<>();
        for (Integer hash : overallResults.keySet()) {
            Station c = overallResults.get(hash);
            sortedCities.put(
                    new String(c.name, StandardCharsets.UTF_8),
                    c);
        }
        for (Station city : sortedCities.values()) {
            System.out.printf("%s=%.1f/%.1f/%.1f\n",
                    new String(city.name),
                    city.minT,
                    city.total / city.measurements,
                    city.maxT);
        }
    }

    private void processFragments(ListOfCities overallResults) {
        Set<Integer> allFragments = new HashSet<>(rf.lineStarts.keySet());
        allFragments.addAll(rf.lineEnds.keySet());
        System.out.println("Fragments: " + allFragments.size());
        for (Integer f : allFragments) {
            String line = rf.getJoinedFragments(f);
            overallResults.addCity(line);
        }
    }

    private boolean processNextBlock(ByteBuffer[] buffers, int i, int blockNumber, Future<?>[] runningThreads, FileChannel channel) throws IOException {
        boolean doWeStillHaveData;
        // last buffer should have pre-loaded data, swap that into the current slot
        // and refill the previous one.
        ByteBuffer b = buffers[threads];
        buffers[threads] = buffers[i];
        buffers[i] = b;
        ProcessData p = new ProcessData(b, blockNumber);
        runningThreads[i] = threadPoolExecutor.submit(p::process);
        doWeStillHaveData = loadNextBlock(buffers[threads], channel);
        return doWeStillHaveData;
    }

    private void waitForThreads(Future<?>[] runningThreads, ListOfCities overallResults) throws InterruptedException, ExecutionException {
        for (int i = 0; i < threads; i++) {
            mergeAndStoreResults((ListOfCities) runningThreads[i].get(), overallResults);
        }
    }


    private void mergeAndStoreResults(ListOfCities result, ListOfCities overallResults) {
        if (result.endFragment != null) {
            rf.addStart(result.blockNumber + 1, result.endFragment);
        }
        if (result.startFragment != null) {
            rf.addEnd(result.blockNumber, result.startFragment);
        }
        for (Integer hash : result.keySet()) {
            overallResults.mergeCity(result.get(hash));
        }
    }

    private void launchInitialProcesses(ByteBuffer[] buffers, FileChannel channel, Future<?>[] whatsRunning) throws IOException {
        for (int i = 0; i < threads; i++) {
            ByteBuffer b = ByteBuffer.allocate(BUFFERSIZE);
            buffers[i] = b;
            loadNextBlock(b, channel);
            ProcessData p = new ProcessData(b, i);
            whatsRunning[i] = threadPoolExecutor.submit(p::process);
        }
    }

    boolean loadNextBlock(ByteBuffer buffer, FileChannel channel) throws IOException {
        if (channel.read(buffer) == -1) {
            return false;
        }
        buffer.flip();
        return true;
    }

    class ProcessData {

        ByteBuffer buffer;
        int blockNumber;

        public ProcessData(ByteBuffer buffer, int blockNumber) {
            this.buffer = buffer;
            this.blockNumber = blockNumber;
        }

        ListOfCities process() {
            ListOfCities results = new ListOfCities(blockNumber);
            // Read up to the first newline and store that as an 'end of line' fragment
            AppendableByteArray sb = new AppendableByteArray();
            int start = readUpToLineEnd(sb);
            results.startFragment = sb;

            // Main loop through block, read until we get to the delimiter and the newline
            AppendableByteArray name = new AppendableByteArray();
            AppendableByteArray value = new AppendableByteArray();
            boolean readingName = true;
            int size = buffer.limit();
            for (int i = start; i < size; i++) {
                byte b = buffer.get();
                if (b == ';') {
                    readingName = false;
                } else if (b != '\n') {
                    (readingName ? name : value).addByte(b);
                } else {
                    results.addCity(name.getBuffer(), fastParseDouble(value.buffer, value.length));
                    name.rewind();
                    value.rewind();
                    readingName = true;
                }
            }
            // If we get to the end and there is still data left, add it to the fragments as the start of the next block
            if (name.length > 0) {
                AppendableByteArray fragment = new AppendableByteArray();
                fragment.appendArray(name);
                if (!readingName) {
                    fragment.addByte((byte) ';');
                    if (value.length > 0) {
                        fragment.appendArray(value);
                    }
                }
                results.endFragment = fragment;
            }
            buffer.clear();
            return results;
        }

        private int readUpToLineEnd(AppendableByteArray sb) {
            byte b = buffer.get();
            int start = 1;
            while (b != '\n') {
                sb.addByte(b);
                b = buffer.get();
                start++;
            }
            return start;
        }

        /**
         * Parse a byte array into a double without having to go through a String first
         * All the numbers are [-]d+.d so can take shortcuts with location of decimal place etc.
         */
        static double fastParseDouble(byte[] b, int length) {
            double number = (b[length - 1] - '0') / 10.0;
            // skip the decimal place
            double multiplier = 1;
            if (b[0] == '-') {
                for (int i = length - 3; i >= 1; i--) {
                    number += (b[i] - '0') * multiplier;
                    multiplier *= 10;
                }
                return -number;
            } else {
                for (int i = length - 3; i >= 0; i--) {
                    number += (b[i] - '0') * multiplier;
                    multiplier *= 10;
                }
                return number;
            }
        }
    }

    static class RowFragments {
        // Holds line fragments at the start and end of each block
        Map<Integer, AppendableByteArray> lineStarts = new HashMap<>();
        Map<Integer, AppendableByteArray> lineEnds = new HashMap<>();

        // spare characters at the end of a block will be the start of a row in the next block.
        // Called with 'block+1' so this is where it needs to be
        void addStart(Integer blockNumber, AppendableByteArray s) {
            lineStarts.put(blockNumber, s);
        }

        // characters before the first newline may be part of the previous block
        void addEnd(Integer blockNumber, AppendableByteArray s) {
            lineEnds.put(blockNumber, s);
        }

        String getJoinedFragments(int hashCode) {
            AppendableByteArray start = lineStarts.get(hashCode);
            AppendableByteArray end = lineEnds.get(hashCode);
            if (start == null) {
                return end.asString();
            } else if (end == null) {
                return start.asString();
            } else {
                start.appendArray(end);
                return start.asString();
            }
        }
    }

    class Station {
        public final byte[] name;
        public int measurements = 1;
        public double total;
        public double maxT = Double.MIN_VALUE;
        public double minT = Double.MAX_VALUE;
        public final int hashCode;

        Station(byte[] name, int hash, double temp) {
            this.name = name;
            this.hashCode = hash;
            this.total = temp;
        }

        public void add_measurement(double temp) {
            total += temp;
            measurements++;
            if (temp > maxT) {
                maxT = temp;
            }
            if (temp < minT) {
                minT = temp;
            }
        }

        public void combine_results(Station city) {
            measurements += city.measurements;
            total += city.total;
            if (city.maxT > maxT) {
                maxT = city.maxT;
            }
            if (city.minT < minT) {
                minT = city.minT;
            }
        }
    }

    // class which takes City entries and stores/updates them
    class ListOfCities extends HashMap<Integer, Station> {

        // startFragment is at the start of the block (or the end of the previous block)
        public AppendableByteArray startFragment;
        public AppendableByteArray endFragment;
        public int blockNumber;

        public ListOfCities(int blockNumber) {
            super();
            this.blockNumber = blockNumber;
        }

        // Only called at the end on the line fragments - doesn't need to be as optimised
        void addCity(String line) {
            String[] bits = line.split(";");
            addCity(bits[0].getBytes(StandardCharsets.UTF_8), Double.parseDouble(bits[1]));
        }

        void addCity(byte[] name, double temperature) {
            // inlined hashcode for tiny speed increase
            int h = 0;
            for (byte b : name) {
                h = 31 * h + b;
            }
            Station city = this.get(h);
            if (city != null) {
                city.add_measurement(temperature);
            } else {
                this.put(h, new Station(name, h, temperature));
            }
        }

        void mergeCity(Station c1) {
            // combine two sets of measurements for a city
            int h = c1.hashCode;
            Station c2 = this.get(h);
            if (c2 != null) {
                c2.combine_results(c1);
            } else {
                this.put(h, c1);
            }
        }
    }
}

/**
 * Holds a byte array along with methods to add bytes and concatenate arrays.
 */
class AppendableByteArray {
    int length;
    byte[] buffer;
    static int INITIAL_BUFF_SIZE = 40;

    /*
     40 bytes should be enough for anyone, right? The longest place name in the world
     (Taumatawhakatangihangakoauauotamateaturipukakapikimaungahoronukupokaiwhenuakitanatahu)
     is 85 characters and I hope that doesn't appear in the test data.
     And that place in Wales (Llanfairpwllgwyngyllgogerychwyrndrobwllllantysiliogogogoch)
     if 58 but I don't think that's in the data either.
     */
    AppendableByteArray() {
        length = 0;
        buffer = new byte[INITIAL_BUFF_SIZE];
    }

    byte[] getBuffer() {
        return Arrays.copyOfRange(buffer, 0, length);
    }

    void addByte(byte b) {
        buffer[length++] = b;
    }

    void appendArray(AppendableByteArray toAppend) {
        System.arraycopy(toAppend.buffer, 0, buffer, length, toAppend.length);
        length += toAppend.length;
    }

    void rewind() {
        length = 0;
    }

    public String asString() {
        return new String(buffer, 0, length, StandardCharsets.UTF_8);
    }
}