package unsafebuffer;

import sun.misc.Unsafe;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
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
 * Use byte arrays instead of Strings for the streams.City names.
 */
public class CalculateUnsafeByteBuffer {

    ExecutorService threadPoolExecutor;

    String file = "measurements.txt";

    final int threads = Runtime.getRuntime().availableProcessors();
    RowFragments rf = new RowFragments();
    static final int BUFFERSIZE = 512 * 1024;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();
        new CalculateUnsafeByteBuffer().go();
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
                for (int thread = 0; thread < threads && doWeStillHaveData; thread++) {
                    if (runningThreads[thread].isDone()) {
                        // thread finished, collect result and re-fill buffer.
                        mergeAndStoreResults((ListOfCities) runningThreads[thread].get(), overallResults);

                        doWeStillHaveData = processNextBlock(buffers, thread, blockNumber++, runningThreads, channel);
                    }
                } // end for threads
            } // end while

            waitForThreads(runningThreads, overallResults);
            threadPoolExecutor.close();

            processFragments(overallResults);
            sortAndDisplay(overallResults);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
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
        for (Map.Entry<String, Station> e : sortedCities.entrySet()) {
            Station city = e.getValue();
            AppendableByteArray output = new AppendableByteArray();
            output.addByte((byte) ';').
                    appendArray(fastNumberToString(city.minT)).
                    addByte((byte) ';').
                    // todo: sort out average calculation rounding errors using ints..
                    appendArray(String.format("%.1f;", city.total / (city.measurements * 10.0)).getBytes(StandardCharsets.UTF_8)).
                    appendArray(fastNumberToString(city.maxT));
//            System.out.printf("%s=%.1f/%.1f/%.1f\n",
//                    e.getKey(),
//                    city.minT/10.0,
//                    city.total / (city.measurements * 10.0),
//                    city.maxT/10.0);
            System.out.print(e.getKey());
            System.out.println(output.asString());
        }
    }

    static byte[] fastNumberToString(int number) {
        int length;
        byte[] bytes;
        if (number <0 ){ // -9 (-0.9), -99 (-9.9), -999 (-99.9)
            number = -number;  // negative
            if (number >= 100) {
                length = 5;
                bytes = new byte[5]; // 3 digits, 5 characters
            }
            else {
                length = 4;
                bytes = new byte[4];
            }
            bytes[0] = (byte) '-';
        } else { // positive
            length = number >= 100 ? 4 : 3;
            bytes = new byte[length];
        }
        bytes[length-1] = (byte) ('0' + (number % 10));
        number /= 10;
        bytes[length-2] = '.';
        bytes[length-3] = (byte) ('0' + (number % 10));
        if (length >= 4) {
            number /= 10;
            bytes[length-4] = (byte) ('0' + (number % 10));
        }
        return bytes;
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

    private boolean processNextBlock(ByteBuffer[] buffers, int i, int blockNumber, Future<?>[] runningThreads, FileChannel channel) throws IOException, NoSuchFieldException, IllegalAccessException {
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

    private void launchInitialProcesses(ByteBuffer[] buffers, FileChannel channel, Future<?>[] whatsRunning) throws IOException, NoSuchFieldException, IllegalAccessException {
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

        static Unsafe unsafe;
        int blockNumber;
        ByteBuffer innerBuffer;
        int bufferPosition;
        int limit;
        byte[] array;

        static {
            try {
                Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                unsafe = (Unsafe) unsafeField.get(null);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        public ProcessData(ByteBuffer buffer, int blockNumber) {
            this.innerBuffer = buffer;
            this.limit = buffer.limit();
            this.array = buffer.array();
            this.bufferPosition = unsafe.arrayBaseOffset(byte[].class);
            this.blockNumber = blockNumber;
        }

        ListOfCities process() {
            ListOfCities results = new ListOfCities(blockNumber);
            // Read up to the first newline and add it as a fragment (potential end of previous block)
            AppendableByteArray sb = new AppendableByteArray();
            byte b = unsafe.getByte(array, bufferPosition++);
            int start = 1;
            while (b != '\n') {
                sb.addByte(b);
                b = unsafe.getByte(array, bufferPosition++);
                start++;
            }
            results.startFragment = sb;

            // Main loop through block, read until we get to the delimiter and the newline
            AppendableByteArray name = new AppendableByteArray();
            AppendableByteArray value = new AppendableByteArray();
            boolean readingName = true;
            for (int i = start; i < limit; i++) {
                b = unsafe.getByte(array, bufferPosition++);
                if (b == ';') {
                    readingName = false;
                } else if (b != '\n') {
                    if (readingName) {
                        name.addByte(b);
                    } else {
                        value.addByte(b);
                    }
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
            innerBuffer.clear();
            return results;
        }
    }

    /**
     * Parse a byte array into a double without having to go through a String first
     * All the numbers are [-]d{1,2}.d so can take shortcuts with location of decimal place etc.
     * Returns 10* the actual number
     */
    static int fastParseDouble(byte[] b, int length) {
        if (b[0] == '-') {
            if (length == 4) {
                return (b[1] - '0') * -10 - (b[3] - '0');
            } else {
                return (b[1] - '0') * -100 - (b[2] - '0') * 10 - (b[4] - '0');
            }
        } else {
            if (length == 3) {
                return (b[0] - '0') * 10 + (b[2] - '0');
            } else {
                return (b[0] - '0') * 100 + (b[1] - '0') * 10 + (b[3] - '0');
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

    class Station {
        public final byte[] name;
        public int measurements;
        public int total;
        public int maxT = Integer.MIN_VALUE;
        public int minT = Integer.MAX_VALUE;
        public final int hashCode;

        Station(byte[] name, int hash, int temp) {
            this.name = name;
            this.hashCode = hash;
            this.total = temp;
            this.measurements = 1;
        }

        public void add_measurement(int temp) {
            total += temp;
            measurements++;
            if (temp > maxT) {
                maxT = temp;
            } else if (temp < minT) {
                minT = temp;
            }
        }

        public void combine_results(Station city) {
            measurements += city.measurements;
            total += city.total;
            if (city.maxT > maxT) {
                maxT = city.maxT;
            } else if (city.minT < minT) {
                minT = city.minT;
            }
        }
    }

    // class which takes streams.City entries and stores/updates them
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
            byte[] number = bits[1].getBytes(StandardCharsets.UTF_8);
            addCity(bits[0].getBytes(StandardCharsets.UTF_8),
                    fastParseDouble(number, number.length));
        }

        void addCity(byte[] name, int temperature) {
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

    AppendableByteArray addByte(byte b) {
        buffer[length++] = b;
        return this;
    }

    void appendArray(AppendableByteArray toAppend) {
        System.arraycopy(toAppend.buffer, 0, buffer, length, toAppend.length);
        length += toAppend.length;
    }

    AppendableByteArray appendArray(byte[] bytes) {
        System.arraycopy(bytes, 0, buffer, length, bytes.length);
        length += bytes.length;
        return this;
    }


    void rewind() {
        length = 0;
    }

    public String asString() {
        return new String(buffer, 0, length, StandardCharsets.UTF_8);
    }

}
