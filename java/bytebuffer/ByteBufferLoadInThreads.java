package bytebuffer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Reads the file into a set of ByteBuffers
 * Use byte arrays instead of Strings for the City names.
 * Based on the version using Unsafe (which had all the optimisations in the data structures)
 * but is back to using array reads instead.
 * <p>
 * Add -ea to VM options to enable the asserts.
 */
public class ByteBufferLoadInThreads {

    ExecutorService threadPoolExecutor;

    static final String file = "measurements.txt";

    final int threads = Runtime.getRuntime().availableProcessors();
    RowFragments rf;
    static final int BUFFERSIZE = 1024 * 1024;
    ProcessData[] processors;

    // text file is >13Gb
    static final int NUM_BLOCKS = (int) (14_000_000_000L / BUFFERSIZE);


    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        new ByteBufferLoadInThreads().go();
        long endTime = System.currentTimeMillis();
        System.out.printf("Took %.2f s\n", (endTime - startTime) / 1000.0);
    }

    private void go() throws Exception {
//        System.out.println("Using " + threads + " cores");
//        System.out.println("Estimated number of blocks: " + NUM_BLOCKS);
        threadPoolExecutor = Executors.newFixedThreadPool(threads);
        Future<?>[] runningThreads = new Future<?>[threads];
        processors = new ProcessData[threads];
        rf = new RowFragments();

        for (int i = 0; i < threads; i++) {
            processors[i] = new ProcessData(ByteBuffer.allocate(BUFFERSIZE), i);
        }
        boolean weStillHaveData = true;
        int blockNumber = 0;

        while (weStillHaveData) {
            for (int thread = 0; thread < threads && weStillHaveData; thread++) {
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
                        weStillHaveData = false;
                    }
                    runningThreads[thread] = null;
                }
            } // end for threads
        } // end while
//        System.out.println("blockNumber = " + blockNumber);
        ListOfCities overallResults = new ListOfCities(0);
        waitForThreads(runningThreads, overallResults);

        processFragments(overallResults);
        sortAndDisplay(overallResults);
        threadPoolExecutor.shutdown();
        threadPoolExecutor.close();
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
                ListOfCities resultToAdd = (ListOfCities) runningThreads[i].get();
                if (resultToAdd != null) {
                    storeFragments(resultToAdd);
                }
            }
            ProcessData p = processors[i];
            mergeAndStoreResults(p.results, overallResults);

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

        Station city; // = new Station("Dummy".getBytes(), -1, 0);
//        int count = 0;
        for (Map.Entry<String, Station> e : sortedCities.entrySet()) {
            city = e.getValue();
            AppendableByteArray output = new AppendableByteArray();
            output.addDelimiter();
            output.appendArray(numberToString(city.minT));
            output.addDelimiter();
            // todo: sort out average calculation rounding errors using ints..
            output.appendArray(String.format("%.1f;", city.total / (city.measurements * 10.0)).getBytes(StandardCharsets.UTF_8));
            output.appendArray(numberToString(city.maxT));
            System.out.print(e.getKey());
//            System.out.printf(" (%d)", city.measurements);
            System.out.println(output.asString());
//            count += city.measurements;
        }
//        System.out.println("length = " + sortedCities.size());
//        System.out.println("count = " + count);
//        assert (sortedCities.size() == 413);
//        assert count == 1_000_000_000;
    }

    static byte[] numberToString(int number) {
        int length;
        byte[] bytes;
        if (number < 0) { // eg. -9 (-0.9), -99 (-9.9), -999 (-99.9)
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


    static class ProcessData {

        ByteBuffer innerBuffer;
        RandomAccessFile raFile;
        FileChannel channel;

        int blockNumber;
        int limit;
        byte[] array;

        ListOfCities results = new ListOfCities(blockNumber);

        public ProcessData(ByteBuffer buffer, int blockNumber) throws FileNotFoundException {
            this.innerBuffer = buffer;
            this.limit = buffer.limit();
            this.array = buffer.array();
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
            results.blockNumber = blockNumber;
            results.startFragment = null;
            results.endFragment = null;

            // Read up to the first newline and add it as a fragment (potential end of previous block)
            int bufferPosition = 0;
            ByteArrayWindow baw = new ByteArrayWindow(array, bufferPosition);
            byte b = array[bufferPosition++];
            while (b != '\n') {
                baw.endIndex++;
                b = array[bufferPosition++];
            }
            results.startFragment = baw.getArray();

            // Main loop through block
            ByteArrayWindow name = new ByteArrayWindow(array, bufferPosition);
            ByteArrayWindow value = new ByteArrayWindow(array, bufferPosition);
            boolean readingName = true;
            int h = 0;
            for (; bufferPosition < limit; bufferPosition++) {
                b = array[bufferPosition];
                // read until we get to the delimiter and the newline
                if (b == ';') {
                    readingName = false;
                    name.endIndex = bufferPosition;
                    value.rewind(bufferPosition + 1);
                } else if (readingName) {
                    name.endIndex++;
                    h = 31 * h + b;
                } else if (b != '\n') { // only consider chr=13 as newline while reading numbers
                    value.endIndex++;
                } else {    // end of line
                    results.addOrMerge(h, name, value.charsToDouble());
                    name.rewind(bufferPosition + 1);
                    readingName = true;
                    h = 0;
                }
            } // end for
            // If we get to the end and there is still data left, add it to the fragments as the start of the next block
            if (name.length() > 0) {
                ByteArrayWindow fragment = new ByteArrayWindow(array, name.startIndex);
                fragment.endIndex = bufferPosition;
                results.endFragment = fragment.getArray();
            }
            innerBuffer.clear();
            return results;
        }
    }


    static class RowFragments {
        byte[][] lineEnds = new byte[NUM_BLOCKS][];
        byte[][] lineStarts = new byte[NUM_BLOCKS][];

        // spare characters at the end of a block will be the start of a row in the next block.
        void addStart(int blockNumber, byte[] s) {
            lineStarts[blockNumber] = s;
        }

        void addEnd(int blockNumber, byte[] s) {
            lineEnds[blockNumber] = s;
        }

        String getJoinedFragments(int number) {
            // turn the start and end fragments into a string
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
    static class ListOfCities {
        public record MapEntry(int hash, Station value) {
        }

        static final int HASH_SPACE = 8192;
        static final int COLLISION = 2;
        public MapEntry[] records = new MapEntry[HASH_SPACE + COLLISION];

        // startFragment is at the start of the block (or the end of the previous block)
        public byte[] startFragment;
        public byte[] endFragment;
        public int blockNumber;

        public ListOfCities(int blockNumber) {
            this.blockNumber = blockNumber;
        }

        // Only called at the end on the line fragments.
        void addCity(String line) {
            byte[] array = line.getBytes(StandardCharsets.UTF_8);
            ByteArrayWindow name = new ByteArrayWindow(array, 0);
            ByteArrayWindow temp = null;
            boolean readingName = true;
            for (int i=0; i<array.length; i++) {
                byte b = array[i];
                if (b == ';') {
                    readingName = false;
                    name.endIndex = i;
                    temp = new ByteArrayWindow(array, i + 1);
                    temp.endIndex = array.length;
                } else if (readingName) {
                    name.endIndex++;
                } else {
                    break;
                }
            }

            int temperature = temp.charsToDouble();

            // inlined hashcode for tiny speed increase
            int h = 0;
            byte[] nameArray = name.getArray();
            for (byte b : nameArray) {
                h = 31 * h + b;
            }
            Station city = new Station(nameArray, h, temperature);
            mergeCity(city);
        }

        void addOrMerge(int key, ByteArrayWindow name, int temperature) {
            int hash = key & (HASH_SPACE - 1);
            // Search forwards search for the entry or a gap
            MapEntry entry = records[hash];
            if (entry == null) {
                records[hash] = new MapEntry(key, new Station(name.getArray(), key, temperature));
                return;
            }
            if (entry.hash == key) {
                entry.value.add_measurement(temperature);
                return;
            }

            entry = records[++hash];
            if (entry == null) {
                records[hash] = new MapEntry(key, new Station(name.getArray(), key, temperature));
                return;
            }

            if (entry.hash == key) {
                entry.value.add_measurement(temperature);
                return;
            }

            entry = records[++hash];
            if (entry == null) {
                records[hash] = new MapEntry(key, new Station(name.getArray(), key, temperature));
                return;
            }

            if (entry.hash == key) {
                entry.value.add_measurement(temperature);
            }
            throw new RuntimeException("Map Collision Error (merge)");
        }


        void mergeCity(Station city) {
            // add a city, or if already present combine two sets of measurements
            int h = city.hashCode;
            int hash = h & (HASH_SPACE - 1);

            MapEntry entry = records[hash];
            if (entry == null) {
                records[hash] = new MapEntry(h, city);
                return;
            }
            // Search forward looking for the city, merge if we find it, add it if we find a null
            if (entry.hash == h) {
                entry.value.combine_results(city);
                return;
            }
            entry = records[++hash];
            if (entry == null) {
                records[hash] = new MapEntry(h, city);
                return;
            }
            if (entry.hash == h) {
                entry.value.combine_results(city);
                return;
            }
            entry = records[++hash];
            if (entry == null) {
                records[hash] = new MapEntry(h, city);
                return;
            }
            if (entry.hash == h) {
                entry.value.combine_results(city);
                return;
            }
            throw new RuntimeException("Map Collision Error (merge/put)");
        }
    }
}


/**
 * Holds the start and end addresses of a substring within a byte array.
 * Has methods to retrieve copies of the substring where required.
 */
class ByteArrayWindow {
    final byte[] buffer;
    int startIndex;
    int endIndex;

    public ByteArrayWindow(byte[] buffer, int startIndex) {
        this.buffer = buffer;
        this.startIndex = startIndex;
        this.endIndex = startIndex;
    }

    /**
     * Parse a byte array into a number without having to go through a String first
     * All the numbers are [-]d{1,2}.d so can take shortcuts with location of decimal place etc.
     * Returns 10* the actual number
     */
    int charsToDouble() {
        if (buffer[startIndex] == '-') {
            if (length() == 4) {
                return -buffer[startIndex + 1] * 10 - buffer[startIndex + 3] + 528;
            } else {
                return -buffer[startIndex + 1] * 100 - buffer[startIndex + 2] * 10 - buffer[startIndex + 4] + 5328;
            }
        } else {
            if (length() == 3) {
                return buffer[startIndex] * 10 + buffer[startIndex + 2] - 528;
            } else {
                return buffer[startIndex] * 100 + buffer[startIndex + 1] * 10 + buffer[startIndex + 3] - 5328;
            }
        }
    }

    byte[] getArray() {
        return Arrays.copyOfRange(buffer, startIndex, endIndex);
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
 */
class AppendableByteArray {
    int length;
    byte[] buffer;
    static int INITIAL_BUFF_SIZE = 32;

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

    public void addDelimiter() {
        buffer[length++] = ';';
    }

    public void appendArray(byte[] bytes) {
        System.arraycopy(bytes, 0, buffer, length, bytes.length);
        length += bytes.length;
    }
}