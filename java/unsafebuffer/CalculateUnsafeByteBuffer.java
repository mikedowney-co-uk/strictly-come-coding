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
 * Use byte arrays instead of Strings for the City names.
 */
public class CalculateUnsafeByteBuffer {

    ExecutorService threadPoolExecutor;

    static final String file = "measurements.txt";

    final int threads = Runtime.getRuntime().availableProcessors();
    RowFragments rf = new RowFragments();
    static final int BUFFERSIZE = 256 * 1024;
    ProcessData[] processors;

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
        processors = new ProcessData[threads];

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
        int count = 0;
        Station city = null;
        for (Map.Entry<String, Station> e : sortedCities.entrySet()) {
            city = e.getValue();
            AppendableByteArray output = new AppendableByteArray();
            output.addByte((byte) ';');
            output.appendArray(fastNumberToString(city.minT));
            output.addByte((byte) ';');
            // todo: sort out average calculation rounding errors using ints..
            output.appendArray(String.format("%.1f;", city.total / (city.measurements * 10.0)).getBytes(StandardCharsets.UTF_8));
            output.appendArray(fastNumberToString(city.maxT));
//            System.out.printf("%s=%.1f/%.1f/%.1f\n",
//                    e.getKey(),
//                    city.minT/10.0,
//                    city.total / (city.measurements * 10.0),
//                    city.maxT/10.0);
            System.out.print(e.getKey());
//            System.out.print(" ("+city.measurements+") ");
            System.out.println(output.asString());
        }
    }

    static byte[] fastNumberToString(int number) {
        int length;
        byte[] bytes;
        if (number < 0) { // -9 (-0.9), -99 (-9.9), -999 (-99.9)
            number = -number;  // negative
            if (number >= 100) {
                length = 5;
                bytes = new byte[5]; // 3 digits, 5 characters
            } else {
                length = 4;
                bytes = new byte[4];
            }
            bytes[0] = (byte) '-';
        } else { // positive
            length = number >= 100 ? 4 : 3;
            bytes = new byte[length];
        }
        bytes[length - 1] = (byte) ('0' + (number % 10));
        number /= 10;
        bytes[length - 2] = '.';
        bytes[length - 3] = (byte) ('0' + (number % 10));
        if (number >= 10) {
            number /= 10;
            bytes[length - 4] = (byte) ('0' + (number % 10));
        }
        return bytes;
    }

    private void processFragments(ListOfCities overallResults) {
        Set<Integer> allFragments = new HashSet<>(rf.lineStarts.keySet());
        allFragments.addAll(rf.lineEnds.keySet());
        System.out.println("Fragments: " + allFragments.size());
        for (Integer f : allFragments) {
            String line = rf.getJoinedFragments(f);
            if (!line.isEmpty()) {
                overallResults.addCity(line);
            }
        }
    }

    private boolean processNextBlock(ByteBuffer[] buffers, int i, int blockNumber, Future<?>[] runningThreads, FileChannel channel) throws IOException, NoSuchFieldException, IllegalAccessException {
        // last buffer should have pre-loaded data, swap that into the current slot
        // and refill the previous one.
        ByteBuffer b = buffers[threads];
        buffers[threads] = buffers[i];
        buffers[i] = b;
        ProcessData p = processors[i];
        p.reset(b, blockNumber);

        runningThreads[i] = threadPoolExecutor.submit(p::process);
        // load data into the spare buffer ready for the next thread
        return loadNextBlock(buffers[threads], channel);
    }

    private void waitForThreads(Future<?>[] runningThreads, ListOfCities overallResults) throws InterruptedException, ExecutionException {
        for (int i = 0; i < threads; i++) {
            mergeAndStoreResults((ListOfCities) runningThreads[i].get(), overallResults);
        }
    }

    private void mergeAndStoreResults(ListOfCities resultToAdd, ListOfCities overallResults) {
        if (resultToAdd.endFragment != null) {
            rf.addStart(resultToAdd.blockNumber + 1, resultToAdd.endFragment);
        }
        if (resultToAdd.startFragment != null) {
            rf.addEnd(resultToAdd.blockNumber, resultToAdd.startFragment);
        }
        for (Integer hash : resultToAdd.keySet()) {
            overallResults.mergeCity(resultToAdd.get(hash));
        }
    }

    private void launchInitialProcesses(ByteBuffer[] buffers, FileChannel channel, Future<?>[] whatsRunning) throws IOException, NoSuchFieldException, IllegalAccessException {
        for (int i = 0; i < threads; i++) {
            ByteBuffer b = ByteBuffer.allocate(BUFFERSIZE);
            buffers[i] = b;
            loadNextBlock(b, channel);
            ProcessData p = new ProcessData(b, i);
            processors[i] = p;
            whatsRunning[i] = threadPoolExecutor.submit(p::process);
        }
    }

    boolean loadNextBlock(ByteBuffer burger, FileChannel channel) throws IOException {
        if (channel.read(burger) == -1) {
            return false;
        }
        burger.flip();
        return true;
    }

    class ProcessData {

        int blockNumber;
        ByteBuffer innerBuffer;
        int bufferPosition;
        int limit;
        byte[] array;

        static Unsafe unsafe;
        static final int arrayOffset;

        static {
            try {
                Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                unsafe = (Unsafe) unsafeField.get(null);
                arrayOffset = unsafe.arrayBaseOffset(byte[].class);
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

        /**
         * Re-use the ProcessData class by calling this instead of instantiating a fresh
         * one each time.
         *
         * @param buffer
         * @param blockNumber
         */
        public void reset(ByteBuffer buffer, int blockNumber) {
            this.blockNumber = blockNumber;
            this.innerBuffer = buffer;
            this.limit = buffer.limit();
            this.array = buffer.array();
            this.bufferPosition = unsafe.arrayBaseOffset(byte[].class);
//            results.startFragment = null;
//            results.endFragment = null;
//            results.blockNumber = blockNumber;
        }


        ListOfCities process() {
            ListOfCities results = new ListOfCities(blockNumber);
            // Read up to the first newline and add it as a fragment (potential end of previous block)
//            AppendableByteArray sb = new AppendableByteArray();
            ByteArrayWindow baw = new ByteArrayWindow(array, bufferPosition);
            byte b = unsafe.getByte(array, bufferPosition++);
            int start = 0;
            while (b != '\n') {
                baw.incrementEnd();
                b = unsafe.getByte(array, bufferPosition++);
                start++;
            }
            results.startFragment = baw.getArray();

            // Main loop through block, read until we get to the delimiter and the newline
            ByteArrayWindow name = new ByteArrayWindow(array, bufferPosition);
            ByteArrayWindow value = new ByteArrayWindow(array, bufferPosition);
            boolean readingName = true;
            int h = 0;
            for (int i = start; i < limit; i++) {
                b = unsafe.getByte(array, bufferPosition++);
                if (b == ';') {
                    readingName = false;
                    name.endIndex = bufferPosition - 1;
                    value.startIndex = bufferPosition;
                } else if (b != '\n') {
                    if (readingName) {
                        name.incrementEnd();
                        h = 31 * h + b;
                    } else {
                        value.incrementEnd();
                    }
                } else {
                    value.endIndex = bufferPosition - 1;
                    results.addCity(name, h, fastParseDouble(value.getArray().array, value.length()));
                    name.rewind(bufferPosition);
                    value.rewind(bufferPosition);
                    readingName = true;
                    h = 0;
                }
            } // end for
            // If we get to the end and there is still data left, add it to the fragments as the start of the next block
            if (name.length() > 0) {
                ByteArrayWindow fragment = new ByteArrayWindow(array,
                        name.startIndex);
                fragment.endIndex = bufferPosition - 1;
                results.endFragment = fragment.getArray();
            }
            innerBuffer.clear();
//            System.out.println(blockNumber + ": " + new String(baw.array) + " , " + new String(results.endFragment.array));
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
                if (length != 4){
                    System.out.println("OOPS:"+new String(b));
                }
                return (b[0] - '0') * 100 + (b[1] - '0') * 10 + (b[3] - '0');
            }
        }
    }

    static class RowFragments {
        // Holds line fragments at the start and end of each block
        Map<Integer, ByteArrayWindow> lineStarts = new HashMap<>();
        Map<Integer, ByteArrayWindow> lineEnds = new HashMap<>();

        // spare characters at the end of a block will be the start of a row in the next block.
        void addStart(Integer blockNumber, ByteArrayWindow s) {
            lineStarts.put(blockNumber, s);
        }

        void addEnd(Integer blockNumber, ByteArrayWindow s) {
            lineEnds.put(blockNumber, s);
        }

        String getJoinedFragments(int number) {
            ByteArrayWindow start = lineStarts.get(number);
            ByteArrayWindow end = lineEnds.get(number);
            if (start == null) {
                return new String(end.array, StandardCharsets.UTF_8);
            } else if (end == null) {
                return new String(start.array, StandardCharsets.UTF_8);
            } else {
                AppendableByteArray combined = new AppendableByteArray();
                combined.appendArray(start.array);
                combined.appendArray(end.array);
                return combined.asString();
            }
        }
    }

    class Station {
        public final byte[] name;
        public int measurements;
        public int total;
        public int maxT;
        public int minT;
        public final int hashCode;

        Station(byte[] name, int hash, int temp) {
            this.name = name;
            this.hashCode = hash;
            this.total = temp;
            this.measurements = 1;
            this.minT = temp;
            this.maxT = temp;
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

    // class which takes City entries and stores/updates them
    class ListOfCities extends HashMap<Integer, Station> {

        // startFragment is at the start of the block (or the end of the previous block)
        public ByteArrayWindow startFragment;
        public ByteArrayWindow endFragment;
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

        // use the hash pre-calculated in the loop
        // Reduces the number of array copies.
        void addCity(ByteArrayWindow name, int h, int temperature) {
            Station city = this.get(h);
            if (city != null) {
                city.add_measurement(temperature);
            } else {
                this.put(h, new Station(name.getArray().array, h, temperature));
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
 * Holds the start and end addresses of a substring within a byte array.
 * Has methods to retrieve copies of the substring where required.
 * (values are the addresses used in 'unsafe' so can only be used in
 * the memory access or copy functions there).
 */
class ByteArrayWindow {
    final byte[] buffer;
    int startIndex;
    int endIndex;
    byte[] array; // only populated at the very end

    static final Unsafe unsafe;
    static final int bufferStart;

    static {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = (Unsafe) unsafeField.get(null);
            bufferStart = unsafe.arrayBaseOffset(byte[].class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public ByteArrayWindow(byte[] buffer, int startIndex) {
        this.buffer = buffer;
        this.startIndex = startIndex;
        this.endIndex = startIndex;
    }

    void incrementEnd() {
        endIndex++;
    }

    ByteArrayWindow getArray() {
        array = new byte[endIndex - startIndex];
        unsafe.copyMemory(buffer, startIndex, array, bufferStart, endIndex - startIndex);
        return this;
    }

    public void rewind(int bufferPosition) {
        startIndex = bufferPosition;
        endIndex = bufferPosition;
    }

    public int length() {
        return endIndex - startIndex;
    }
}

/**
 * Holds a byte array along with methods to add bytes and concatenate arrays.
 * Not using it in the main loop any more but still currently using it to build up
 * the output.
 */
class AppendableByteArray {
    static final Unsafe unsafe;
    static final int bufferStart;

    static {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = (Unsafe) unsafeField.get(null);
            bufferStart = unsafe.arrayBaseOffset(byte[].class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    int length;
    byte[] buffer;
    static final int INITIAL_BUFF_SIZE = 40;

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

    void addByte(byte b) {
        unsafe.putByte(buffer, bufferStart + length, b);
        length++;
    }

    void appendArray(byte[] bytes) {
        unsafe.copyMemory(bytes, bufferStart, buffer, bufferStart + length, bytes.length);
        length += bytes.length;
    }

    public String asString() {
        return new String(buffer, 0, length, StandardCharsets.UTF_8);
    }

}
