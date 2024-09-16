
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * Reads the file into a set of ByteByffers
 * Use byte arrays instead of Strings for the City names.
 */
public class CalculateByteBufferCharArray {

    ExecutorService threadPoolExecutor;

    String file = "../measurements.txt";
    final int threads = Runtime.getRuntime().availableProcessors();
    RowFragments rf = new RowFragments();
    static final int BUFFERSIZE = 256 * 1024;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();
        new CalculateByteBufferCharArray().go();
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
            ListOfCities overallResults = new ListOfCities();
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
        TreeMap<String, CityB> sortedCities = new TreeMap<>();
        for (Long hash : overallResults.keySet()) {
            CityB c = overallResults.get(hash);
            sortedCities.put(
                    new String(c.name, StandardCharsets.UTF_8),
                    c);
        }
        for (CityB city : sortedCities.values()) {
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
            CityAndTemp c = CityB.processRow(line);
            overallResults.addCity(c);
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
        for (Long hash : result.keySet()) {
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
            ListOfCities results = new ListOfCities();
            // Read up to the first newline and store that as an 'end of line' fragment
            AppendableByteArray sb = new AppendableByteArray();
            int start = readUpToLineEnd(sb);
            rf.addEnd(blockNumber, sb);

            buffer.get(0);

            // Main loop through block, read until we get to the delimiter and the newline
            AppendableByteArray name = new AppendableByteArray();
            AppendableByteArray value = new AppendableByteArray();
            boolean readingName = true;
            for (int i = start; i < buffer.limit(); i++) {
                byte b = buffer.get();
                if (b == ';') {
                    readingName = false;
                } else if (b != '\n') {
                    if (readingName) {
                        name.addByte(b);
                    } else {
                        value.addByte(b);
                    }
                } else {
                    CityAndTemp c = new CityAndTemp(name.getBuffer(), fastParseDouble(value.getBuffer()));
                    results.addCity(c);
                    name.rewind();
                    value.rewind();
                    readingName = true;
                }
            }
            // If we get to the end and there is still data left, add it to the fragments as the start of the next block
            AppendableByteArray fragment = new AppendableByteArray();
            if (name.length > 0) {
                fragment.appendArray(name);
                if (!readingName) {
                    fragment.addByte((byte) ';');
                    if (value.length > 0) {
                        fragment.appendArray(value);
                    }
                }
                rf.addStart(blockNumber + 1, fragment);
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

        // Parse a byte array into a double without having to go through a String first
        static double fastParseDouble(byte[] b) {
            int whole = 0;
            double divisor = 1.0; // the value to divide by to get the result
            int start = 0;
            if (b[0] == '-') {
                divisor = -1.0;
                start = 1;
            }
            boolean seenDP = false;
            for (int i = start; i < b.length; i++) {
                if (b[i] == '.') {
                    seenDP = true;
                    continue;
                }
                int digit = b[i] - '0';
                whole = whole * 10 + digit;
                if (seenDP) {
                    divisor *= 10;
                }
            }
            return whole / divisor;
        }
    }

    class RowFragments {
        // Holds line fragments at the start and end of each block
        ConcurrentHashMap<Integer, AppendableByteArray> lineStarts = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, AppendableByteArray> lineEnds = new ConcurrentHashMap<>();

        // spare characters at the end of a block will be the start of a row in the next block.
        // Called with 'block+1' so this is where it needs to be
        void addStart(Integer blockNumber, AppendableByteArray s) {
            lineStarts.put(blockNumber, s);
        }

        // characters before the first newline may be part of the previous block
        void addEnd(Integer blockNumber, AppendableByteArray s) {
            lineEnds.put(blockNumber, s);
        }

        String getJoinedFragments(int number) {
            AppendableByteArray start = lineStarts.get(number);
            AppendableByteArray end = lineEnds.get(number);
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

    record CityAndTemp(byte[] name, Double temperature) {
    }




    // class which takes City entries and stores/updates them
    class ListOfCities extends HashMap<Long, CityB> {

        void addCity(CityAndTemp c) {
            long h = Arrays.hashCode(c.name);
            CityB city = this.get(h);
            if (city != null) {
                city.add_measurement(c.temperature);
            } else {
                CityB city1 = new CityB(c.name, h);
                city1.add_measurement(c.temperature);
                this.put(h, city1);
            }
        }

        void mergeCity(CityB c1) {
            // combine two sets of measurements for a city
            long h = c1.hashCode;
            CityB c2 = this.get(h);
            if (c2 != null) {
                c2.combine_results(c1);
            } else {
                this.put(h, c1);
            }

        }
    }
}

