package unsafebuffer;

import sun.misc.Unsafe;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Reads the file into a set of ByteBuffers
 * Use byte arrays instead of Strings for the City names.
 */
public class CalculateUnsafeByteBuffer {

    ExecutorService threadPoolExecutor;

    static final String file = "measurements.txt";

    final int threads = Runtime.getRuntime().availableProcessors();
    RowFragments rf;
    static final int BUFFERSIZE = 1024 * 1024;
    ProcessData[] processors;

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        new CalculateUnsafeByteBuffer().go();
        long endTime = System.currentTimeMillis();
        System.out.printf("Took %.2f s\n", (endTime - startTime) / 1000.0);
    }

    private void go() throws Exception {
        System.out.println("Using " + threads + " cores");
        threadPoolExecutor = Executors.newFixedThreadPool(threads);
        Future<?>[] runningThreads = new Future<?>[threads];
        processors = new ProcessData[threads];
        rf = new RowFragments();

        for (int i = 0; i < threads; i++) {
            processors[i] = new ProcessData(ByteBuffer.allocate(BUFFERSIZE), i);
        }
        boolean doWeStillHaveData = true;
        int blockNumber = 0;

        while (doWeStillHaveData) {
            for (int thread = 0; thread < threads && doWeStillHaveData; thread++) {
                if (runningThreads[thread] == null) {
                    ProcessData p = processors[thread];
                    p.blockNumber = blockNumber++;
                    runningThreads[thread] = threadPoolExecutor.submit(p::process);
                } else if (runningThreads[thread].isDone()) {
                    // thread finished, collect result
                    ListOfCities resultToAdd = (ListOfCities) runningThreads[thread].get();
                    if (resultToAdd != null) {
                        storeFragments(resultToAdd);
                    } else {
                        doWeStillHaveData = false;
                    }
                    runningThreads[thread] = null;
                }
            } // end for threads
        } // end while

        ListOfCities overallResults = new ListOfCities(0);
        waitForThreads(runningThreads, overallResults);

        processFragments(overallResults);
        sortAndDisplay(overallResults);
        threadPoolExecutor.shutdown();
        threadPoolExecutor.close();
    }

    private static void sortAndDisplay(ListOfCities overallResults) {
        TreeMap<String, Station> sortedCities = new TreeMap<>();
        for (ListOfCities.MapEntry m : overallResults.records) {
            if (m != null) {
                Station c = m.value;
                sortedCities.put(
                        new String(c.name, StandardCharsets.UTF_8),
                        c);
            }
        }
        Station city = null;
        for (Map.Entry<String, Station> e : sortedCities.entrySet()) {
            city = e.getValue();
            AppendableByteArray output = new AppendableByteArray();
            output.addDelimiter();
            output.appendArray(fastNumberToString(city.minT));
            output.addDelimiter();
            // todo: sort out average calculation rounding errors using ints..
            output.appendArray(String.format("%.1f;", city.total / (city.measurements * 10.0)).getBytes(StandardCharsets.UTF_8));
            output.appendArray(fastNumberToString(city.maxT));
            System.out.print(e.getKey());
//            System.out.printf(" (%d)", city.measurements);
            System.out.println(output.asString());
        }
        System.out.println("length = " + sortedCities.size());
        assert (sortedCities.size() == 413);
        assert city.minT == -290;
        assert city.maxT == 675;
        assert city.measurements == 2420468;
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
        for (int f = 0; f < NUM_BLOCKS; f++) {
            String line = rf.getJoinedFragments(f);
            if (!line.isEmpty()) {
                overallResults.addCity(line);
            }
        }
    }

    private void waitForThreads(Future<?>[] runningThreads, ListOfCities overallResults) throws Exception {
        for (int i = 0; i < threads; i++) {
            if (runningThreads[i] != null) {
                mergeAndStoreResults((ListOfCities) runningThreads[i].get(), overallResults);
            }
            processors[i].close();
        }
    }

    private void storeFragments(ListOfCities resultToAdd) {
        if (resultToAdd.endFragment != null) {
            rf.addStart(resultToAdd.blockNumber + 1, resultToAdd.endFragment);
        }
        if (resultToAdd.startFragment != null) {
            rf.addEnd(resultToAdd.blockNumber, resultToAdd.startFragment);
        }
    }

    private void mergeAndStoreResults(ListOfCities resultToAdd, ListOfCities overallResults) {
        if (resultToAdd.endFragment != null) {
            rf.addStart(resultToAdd.blockNumber + 1, resultToAdd.endFragment);
        }
        if (resultToAdd.startFragment != null) {
            rf.addEnd(resultToAdd.blockNumber, resultToAdd.startFragment);
        }
        for (ListOfCities.MapEntry m : resultToAdd.records) {
            if (m != null) {
                overallResults.mergeCity(m.value);
            }
        }
    }

    static class ProcessData {

        ByteBuffer innerBuffer;
        RandomAccessFile raFile;
        FileChannel channel;

        int blockNumber;
        int bufferPosition;
        int limit;
        byte[] array;

        static Unsafe unsafe;
        static final int arrayOffset;
        ListOfCities results = new ListOfCities(blockNumber);

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

        public ProcessData(ByteBuffer buffer, int blockNumber) throws FileNotFoundException {
            this.innerBuffer = buffer;
            this.limit = buffer.limit();
            this.array = buffer.array();
            this.bufferPosition = unsafe.arrayBaseOffset(byte[].class);
            this.blockNumber = blockNumber;
            this.raFile = new RandomAccessFile(file, "r");
            this.channel = raFile.getChannel();
        }

        public void close() throws IOException {
            channel.close();
            raFile.close();
        }

        ListOfCities process() throws IOException {
            channel.position((long) blockNumber * BUFFERSIZE);
            int status = channel.read(innerBuffer);
            if (status == -1) {
                return null;
            }
            innerBuffer.flip();

            this.limit = innerBuffer.limit();
            this.array = innerBuffer.array();
            this.bufferPosition = unsafe.arrayBaseOffset(byte[].class);
            this.results.blockNumber = blockNumber;
            this.results.startFragment = null;
            this.results.endFragment = null;

            // Read up to the first newline and add it as a fragment (potential end of previous block)
            ByteArrayWindow baw = new ByteArrayWindow(array, bufferPosition);
            byte b = unsafe.getByte(array, bufferPosition++);
            int start = 0;
            while (b != '\n') {
                baw.endIndex++;
                b = unsafe.getByte(array, bufferPosition++);
                start++;
            }
            results.startFragment = baw.getArray().array;

            // Main loop through block
            ByteArrayWindow name = new ByteArrayWindow(array, bufferPosition);
            ByteArrayWindow value = new ByteArrayWindow(array, bufferPosition);
            boolean readingName = true;
            int h = 0;
            for (int i = start; i < limit; i++) {
                b = unsafe.getByte(array, bufferPosition++);
                // read until we get to the delimiter and the newline
                if (b == ';') {
                    readingName = false;
                    name.endIndex = bufferPosition - 1;
                    value.startIndex = bufferPosition;
                } else if (readingName) {
                    name.endIndex++;
                    h = 31 * h + b;
                } else if (b != '\n') {
                    value.endIndex++;
                } else {    // end of line
                    value.endIndex = bufferPosition - 1;
                    results.addOrMerge(h, name, fastParseDouble(value));
                    name.rewind(bufferPosition);
                    value.rewind(bufferPosition);
                    readingName = true;
                    h = 0;
                }
            } // end for
            // If we get to the end and there is still data left, add it to the fragments as the start of the next block
            if (name.length() > 0) {
                ByteArrayWindow fragment = new ByteArrayWindow(array, name.startIndex);
                fragment.endIndex = bufferPosition - 1;
                results.endFragment = fragment.getArray().array;
            }
            innerBuffer.clear();
            return results;
        }
    }

    /**
     * Parse a byte array into a number without having to go through a String first
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

    static int fastParseDouble(ByteArrayWindow baw) {
        if (baw.getByte(0) == '-') {
            if (baw.length() == 4) {
                return (baw.getByte(1) - '0') * -10 - (baw.getByte(3) - '0');
            } else {
                return (baw.getByte(1) - '0') * -100 - (baw.getByte(2) - '0') * 10 - (baw.getByte(4) - '0');
            }
        } else {
            if (baw.length() == 3) {
                return (baw.getByte(0) - '0') * 10 + (baw.getByte(2) - '0');
            } else {
                return (baw.getByte(0) - '0') * 100 + (baw.getByte(1) - '0') * 10 + (baw.getByte(3) - '0');
            }
        }
    }

    // text file is >13Gb
    static final int NUM_BLOCKS = (int) (14_000_000_000L / BUFFERSIZE);

    static class RowFragments {
        byte[][] lineEnds = new byte[NUM_BLOCKS][];
        byte[][] lineStarts = new byte[NUM_BLOCKS][];

        // spare characters at the end of a block will be the start of a row in the next block.
        void addStart(Integer blockNumber, byte[] s) {
            lineStarts[blockNumber] = s;
        }

        void addEnd(Integer blockNumber, byte[] s) {
            lineEnds[blockNumber] = s;
        }

        String getJoinedFragments(int number) {
            byte[] start = lineStarts[number];
            byte[] end = lineEnds[number];
            if (start == null && end == null) {
                return "";
            } else if (start == null) {
                return new String(end, StandardCharsets.UTF_8);
            } else if (end == null) {
                return new String(start, StandardCharsets.UTF_8);
            } else {
                AppendableByteArray combined = new AppendableByteArray();
                combined.appendArray(start);
                combined.appendArray(end);
                return combined.asString();
            }
        }
    }

    static class Station {
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
    static class ListOfCities {
        public record MapEntry(int hash, Station value) {
        }

        static final int HASH_SPACE = 8192;
        static final int COLLISION = 2;
        public MapEntry[] records = new MapEntry[HASH_SPACE + COLLISION];

        public Station get(int key) {
            int hash = key & (HASH_SPACE - 1);
            // unrolled loop looking for the entry
            MapEntry entry = records[hash];
            if (entry != null && entry.hash == key) {
                return entry.value;
            }
            if (entry == null) {
                return null;
            }

            entry = records[++hash];
            if (entry == null) {
                return null;
            }
            if (entry.hash == key) {
                return entry.value;
            }

            entry = records[++hash];
            if (entry == null) {
                return null;
            }
            if (entry.hash == key) {
                return entry.value;
            }
            throw new RuntimeException("Map Collision Error (get)");
        }

        public void put(int key, Station value) {
            int hash = key & (HASH_SPACE - 1);
            // Search forwards checking for gaps. We never replace existing values.
            if (records[hash] == null) {
                records[hash] = new MapEntry(key, value);
                return;
            }
            if (records[++hash] == null) {
                records[hash] = new MapEntry(key, value);
                return;
            }
            if (records[++hash] == null) {
                records[hash] = new MapEntry(key, value);
                return;
            }
            throw new RuntimeException("Map Collision Error (put)");
        }

        // startFragment is at the start of the block (or the end of the previous block)
        public byte[] startFragment;
        public byte[] endFragment;
        public int blockNumber;

        public ListOfCities(int blockNumber) {
            this.blockNumber = blockNumber;
        }

        // Only called at the end on the line fragments - doesn't need to be as optimised
        void addCity(String line) {
            String[] bits = line.split(";");
            byte[] number = bits[1].getBytes(StandardCharsets.UTF_8);
            byte[] name = bits[0].getBytes(StandardCharsets.UTF_8);
            int temperature = fastParseDouble(number, number.length);

            // inlined hashcode for tiny speed increase
            int h = 0;
            for (byte b : name) {
                h = 31 * h + b;
            }
            int hash = h & (HASH_SPACE - 1);
            // Search forwards checking for gaps or replacements
            for (int i = hash; i < hash + COLLISION; i++) {
                if (records[i] == null) {
                    records[i] = new MapEntry(h, new Station(name, h, temperature));
                    return;
                }
                // merge existing data
                MapEntry entry = records[i];
                if (entry.hash == h) {
                    entry.value.add_measurement(temperature);
                    return;
                }
            }
        }

        void addOrMerge(int key, ByteArrayWindow name, int temperature) {
            int hash = key & (HASH_SPACE - 1);
            // Search forwards checking for gaps or replacements
            if (records[hash] == null) {
                records[hash] = new MapEntry(key, new Station(name.getArray().array, key, temperature));
                return;
            }
            MapEntry entry = records[hash];
            if (entry.hash == key) {
                entry.value.add_measurement(temperature);
                return;
            }

            if (records[++hash] == null) {
                records[hash] = new MapEntry(key, new Station(name.getArray().array, key, temperature));
                return;
            }

            entry = records[hash];
            if (entry.hash == key) {
                entry.value.add_measurement(temperature);
                return;
            }

            if (records[++hash] == null) {
                records[hash] = new MapEntry(key, new Station(name.getArray().array, key, temperature));
                return;
            }

            entry = records[hash];
            if (entry.hash == key) {
                entry.value.add_measurement(temperature);
            }
            throw new RuntimeException("Map Collision Error (merge)");
        }


        void mergeCity(Station city) {
            // combine two sets of measurements for  city
            int h = city.hashCode;
            Station c2 = get(h);
            if (c2 != null) {
                c2.combine_results(city);
            } else {
                put(h, city);
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

    public byte getByte(int offset) {
        return unsafe.getByte(buffer, startIndex + offset);
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
    static final int INITIAL_BUFF_SIZE = 32;

    /*
     32 bytes should be enough for anyone, right? The longest place name in the world
     (Taumatawhakatangihangakoauauotamateaturipukakapikimaungahoronukupokaiwhenuakitanatahu)
     is 85 characters and I hope that doesn't appear in the test data.
     And that place in Wales (Llanfairpwllgwyngyllgogerychwyrndrobwllllantysiliogogogoch)
     if 58 but I don't think that's in the data either.
     */
    AppendableByteArray() {
        length = 0;
        buffer = new byte[INITIAL_BUFF_SIZE];
    }

    void addDelimiter() {
        unsafe.putByte(buffer, bufferStart + length, (byte) ';');
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
