package bytebuffer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;

/**
 * Reads the file into a set of ByteBuffers
 * Use byte arrays instead of Strings for the City names.
 * Used the array-backed map
 * <p>
 * Add -ea to VM options to enable the asserts.
 */
public class ByteBufferLoadInThreads {


    static final String file = "measurements.txt";
    static final int BUFFERSIZE = 1024 * 1024;

    static int NUM_BLOCKS;


    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        long size = Files.size(Paths.get(file));
        NUM_BLOCKS = 1 + (int) (size / BUFFERSIZE);
        new ByteBufferLoadInThreads().go();
        long endTime = System.currentTimeMillis();
        System.out.printf("Took %.2f s\n", (endTime - startTime) / 1000.0);
    }

    // All the blocks of code which were separate methods have now been inlined.
    // That wasn't to save time of itself but it meant I could move from instance
    // variables to local variables, which is noticeably faster.
    private void go() throws Exception {
        final int threads = Runtime.getRuntime().availableProcessors();
//        System.out.println("Using " + threads + " cores");

//        System.out.println("Estimated number of blocks: " + NUM_BLOCKS);
        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(threads);
        Future<?>[] runningThreads = new Future<?>[threads];
        ProcessData[] processors = new ProcessData[threads];
        RowFragments fragmentStore = new RowFragments();

        for (int i = 0; i < threads; i++) {
            processors[i] = new ProcessData(ByteBuffer.allocate(BUFFERSIZE), i, fragmentStore);
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
                    // thread finished, handle result.
                    ProcessData resultToAdd = (ProcessData) runningThreads[thread].get();
                    // Note: we re-use the ListOfCities, 1 per processor, so only collect at the end
                    weStillHaveData = (resultToAdd != null);

                    runningThreads[thread] = null;
                }
            } // end for threads
        } // end while - no more data.
//        System.out.println("blockNumber = " + blockNumber);
        ProcessData overallResults = null;
        // wait for threads to end and combine the results
        for (int i = 0; i < threads; i++) {
//            if (runningThreads[i] != null) {
//                ProcessData resultToAdd = (ProcessData) runningThreads[i].get();
//                if (resultToAdd != null) {
//                    fragmentStore.storeFragments(resultToAdd);
//                }
//            }
            if (overallResults == null) {
                overallResults = processors[i];
            } else {
                ProcessData resultsToAdd = processors[i];
                // Merges a result set into the final ListOfCities.
                for (Station s : resultsToAdd.records) {
                    if (s != null) {
                        overallResults.mergeCity(s);
                    }
                }
            }
            processors[i].close();
        }

        // add the fragments into the results now.
        for (int f = 0; f < NUM_BLOCKS; f++) {
            byte[] line = fragmentStore.getJoinedFragments(f);
            if (line != null) {
                overallResults.addCity(line);
            }
        }
        sortAndDisplay(overallResults);
        threadPoolExecutor.shutdown();
        threadPoolExecutor.close();
    }

    private void sortAndDisplay(ProcessData overallResults) {
        TreeMap<String, Station> sortedCities = new TreeMap<>();
        for (Station s : overallResults.records) {
            if (s != null) {
                sortedCities.put(new String(s.name, StandardCharsets.UTF_8), s);
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
            output.appendArray(String.format("%.1f;", city.total / (city.measurements * 10.0)).getBytes(StandardCharsets.UTF_8));
//            System.out.printf("%s;%s;%.1f;%s\n",
//                    e.getKey(), new String(numberToString(city.minT), StandardCharsets.UTF_8),
//                    city.total / (city.measurements * 10.0),
//                    new String(numberToString(city.maxT), StandardCharsets.UTF_8));

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

    // Only used in the final display
    static byte[] numberToString(int number) {
        int length;
        byte[] bytes;
        if (number < 0) {
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

        ByteBuffer buffer;
        RandomAccessFile raFile;
        FileChannel channel;

        int blockNumber;

        // larger values have fewer collisions but the increased array size takes longer to traverse
        static final int HASH_SPACE = 8192;
        static final int COLLISION = 2; // number of extra spaces needed for hash collisions
        // decreasing the hash array size below 8192 means this needs increasing to at least 6
        Station[] records = new Station[HASH_SPACE + COLLISION];

        // startFragment is at the start of the block (or the end of the previous block)
//        public byte[] startFragments;
//        public byte[] endFragments;
        RowFragments fragmentStore;

        public ProcessData(ByteBuffer buffer, int blockNumber, RowFragments fragmentStore) throws FileNotFoundException {
            this.buffer = buffer;
            this.blockNumber = blockNumber;
            this.raFile = new RandomAccessFile(file, "r");
            this.channel = raFile.getChannel();
            this.fragmentStore = fragmentStore;
        }

        public void close() throws IOException {
            channel.close();
            raFile.close();
        }

        ProcessData process() throws IOException {
            channel.position((long) blockNumber * BUFFERSIZE);
            int status = channel.read(buffer);
            if (status == -1) {
                return null;
            }
            buffer.flip();
            byte[] array = buffer.array();
            int limit = buffer.limit();


            // Read up to the first newline and add it as a fragment (potential end of previous block)
            int bufferPosition = -1;
            byte b = array[++bufferPosition];
            while (b != '\n') {
                b = array[++bufferPosition];
            }
            fragmentStore.lineEnds[blockNumber] = Arrays.copyOfRange(array, 0, bufferPosition);
// lineEnds[resultToAdd.blockNumber] = resultToAdd.startFragment;
            // Main loop through block
            int nameStart = ++bufferPosition;
            int nameEnd = bufferPosition;
            boolean readingName = true;
            int h = 0;
            // inlining the decimal conversion
            int sign = 1;
            int temperature = 0;

            while (bufferPosition < limit) {
                b = array[bufferPosition++];
                // read until we get to the delimiter and the newline
                if (b == ';') {
                    readingName = false;
                    nameEnd = bufferPosition - 1;
                } else if (readingName) {
                    h = 31 * h + b; // calculate the hash of the name
                } else if (b != '\n') { // only consider chr=13 as newline while reading numbers
                    if (b == '-') {
                        sign = -1;
                    } else if (b != '.') {
                        temperature = temperature * 10 + (b - '0');
                    }
                } else {    // end of line
                    addOrMerge(h, array, nameStart, nameEnd, sign * temperature);
                    temperature = 0;
                    sign = 1;
                    nameStart = bufferPosition;
                    nameEnd = bufferPosition;
                    readingName = true;
                    h = 0;
                }
            } // end loop
            // If we get to the end and there is still data left, add it to the fragments as the start of the next block
// lineStarts[resultToAdd.blockNumber + 1] = resultToAdd.endFragment;
            if (nameStart < limit) {
                fragmentStore.lineStarts[blockNumber + 1] = Arrays.copyOfRange(array, nameStart, limit);
            }
            buffer.clear();
            return this;
        }


        // Only called at the end on the line fragments.
        void addCity(byte[] array) {
            int tempStart = 0;
            int tempEnd = 0;
            // Split the line into name and temperature
            int hashCode = 0;
            for (int i = 0; i < array.length; i++) {
                byte b = array[i];
                if (b == ';') {
                    tempStart = i + 1;
                    tempEnd = array.length;
                    break;
                } else {
                    hashCode = 31 * hashCode + b;
                }
            }

            /*
             * Parse a byte array into a number without having to go through a String first
             * All the numbers are [-]d{1,2}.d so can take shortcuts with location of decimal place etc.
             * Returns 10* the actual number
             */
            int temp;
            if (array[tempStart] == '-') {
                if (tempEnd - tempStart == 4) {
                    temp = -array[tempStart + 1] * 10 - array[tempStart + 3] + 528;
                } else {
                    temp = -array[tempStart + 1] * 100 - array[tempStart + 2] * 10 - array[tempStart + 4] + 5328;
                }
            } else {
                if (tempEnd - tempStart == 3) {
                    temp = array[tempStart] * 10 + array[tempStart + 2] - 528;
                } else {
                    temp = array[tempStart] * 100 + array[tempStart + 1] * 10 + array[tempStart + 3] - 5328;
                }
            }

            // gamble - we will already have seen all the station codes during the block.
            // use a shortcut merge which only considers hashCode and expects the values to be there
            int hash = hashCode & (HASH_SPACE - 1);
            Station entry = records[hash];
            if (entry.hash == hashCode) {
                records[hash].add_measurement(temp);
                return;
            }
            entry = records[++hash];
            if (entry.hash == hashCode) {
                records[hash].add_measurement(temp);
                return;
            }
            entry = records[++hash];
            if (entry.hash == hashCode) {
                records[hash].add_measurement(temp);
                return;
            }
            throw new RuntimeException("Hash code not present in array");
        }

        // Called during the main processing loop
        void addOrMerge(int key, byte[] buffer, int startIndex, int endIndex, int temperature) {
            int hash = key & (HASH_SPACE - 1);
            // Search forwards search for the entry or a gap
            Station[] r = records;
            Station entry = r[hash];
            if (entry == null) {
                byte[] nameArray = Arrays.copyOfRange(buffer, startIndex, endIndex);
                r[hash] = new Station(nameArray, key, temperature);
                return;
            }
            if (entry.hash == key) {
                entry.add_measurement(temperature);
                return;
            }

            entry = r[++hash];
            if (entry == null) {
                byte[] nameArray = Arrays.copyOfRange(buffer, startIndex, endIndex);
                r[hash] = new Station(nameArray, key, temperature);
                return;
            }

            if (entry.hash == key) {
                entry.add_measurement(temperature);
                return;
            }

            entry = r[++hash];
            if (entry == null) {
                byte[] nameArray = Arrays.copyOfRange(buffer, startIndex, endIndex);
                r[hash] = new Station(nameArray, key, temperature);
                return;
            }

            if (entry.hash == key) {
                entry.add_measurement(temperature);
                return;
            }
            // don't fail silently, fail kicking and screaming if we can't store the value.
            throw new RuntimeException("Map Collision Error (merge)");
        }


        // Called during the final data merging and during the fragment processing
        void mergeCity(Station city) {
            // add a city, or if already present combine two sets of measurements
            int h = city.hash;
            int hash = h & (HASH_SPACE - 1);
            Station[] r = records;
            Station entry = r[hash];
            // Search forward looking for the city, merge if we find it, add it if we find a null
            if (entry == null) {
                r[hash] = city;
                return;
            }
            if (entry.hash == h) {
                entry.combine_results(city);
                return;
            }
            entry = r[++hash];
            if (entry == null) {
                r[hash] = city;
                return;
            }
            if (entry.hash == h) {
                entry.combine_results(city);
                return;
            }
            entry = r[++hash];
            if (entry == null) {
                r[hash] = city;
                return;
            }
            if (entry.hash == h) {
                entry.combine_results(city);
                return;
            }
            throw new RuntimeException("Map Collision Error (merge/put)");
        }
    }


    static class RowFragments {
        byte[][] lineEnds = new byte[NUM_BLOCKS][];
        byte[][] lineStarts = new byte[NUM_BLOCKS][];

        byte[] getJoinedFragments(int number) {
            // join the start and end fragments together
            byte[] start = lineStarts[number];
            byte[] end = lineEnds[number];
            if (start == null && end == null) {
                return null;
            } else if (start == null) {
                return end;
            } else if (end == null) {
                return start;
            } else {
                byte[] bufferToBuild = new byte[start.length + end.length];
                System.arraycopy(start, 0, bufferToBuild, 0, start.length);
                System.arraycopy(end, 0, bufferToBuild, start.length, end.length);
                return bufferToBuild;
            }
        }

        // spare characters at the end of a block will be the start of a row in the next block.
//        public void storeFragments(ProcessData resultToAdd) {
//            if (resultToAdd.endFragment != null) {
//                lineStarts[resultToAdd.blockNumber + 1] = resultToAdd.endFragment;
//            }
//            if (resultToAdd.startFragment != null) {
//                lineEnds[resultToAdd.blockNumber] = resultToAdd.startFragment;
//            }
//        }
    }

    static class Station {
        public final byte[] name;
        public int measurements;
        public int total;
        public int maxT;
        public int minT;
        public final int hash;

        Station(byte[] name, int hash, int temp) {
            this.name = name;
            this.hash = hash;
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
}

/**
 * Holds a byte array along with methods to add bytes and concatenate arrays.
 * Only used at the end, joining fragments and preparing output.
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
     is 58 but I don't think that's in the data either.
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
