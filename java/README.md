# Java solutions

This directory holds my Java based attempts at the billion row challenge. It has been a few years since I have
done any Java programming so I did a bit of research to look up different ways of loading in files and tried
a few different ways of doing the calculations.

## Loading the files

I tried several different ways of loading the data:

- Scanner was the slowest, taking over 6 minutes just to load:
```
Scanner s = new Scanner(new File(file));

while(s.hasNext()){
   String line = s.nextLine();
    count++;
}
```
- `Files.lines` was the second slowest, taking 1m35s:
``` 
    try (Stream<String> lines = Files.lines(Path.of(file))) {
        lines.forEach(x -> count++);
    }
```
- `BufferedReader` was only slightly quicker at 1m30:
```
    try(BufferedReader br = new BufferedReader(new FileReader(file))) {
        String line = "";
        while (line != null) {
            line = br.readLine();
            count++;
        }
    }
```
- `ByteBuffer` was a lot quicker, taking 30s. For my timing test, it read the input a byte at a time, counting the newlines.
```
    ByteBuffer buffer = ByteBuffer.allocate(BUFFERSIZE);
    try (RandomAccessFile aFile = new RandomAccessFile(file, "r");
         FileChannel channel = aFile.getChannel()) {
         while (true) {
             if (channel.read(buffer) == -1) {
                break; // end of file
             }
             // do any processing here
             buffer.flip();
         } // end while
    }
```
- A multi-threaded `ByteBuffer` could read the file and count the lines in as little as 5s.

## The Attempts
### 1. Sensible Java
This was actually the second attempt since I started off looking at the
`ByteBuffer` method but I'm listing it first because it's a more concise
version and more closely resembles good programming practice.

The file is read in using `Files.lines` and processed using parallel streams and a
custom collector to group the entries by city name. This is the first time I've needed
to write a custom Collector, to access the methods in the `City` class to sum together
the temperature readings so we can take the average.

This method doesn't use any special optimisations and runs in about 1.5 minutes.

### 2. The crazy bodged method filled with workarounds for edge cases

This method uses a `ByteBuffer` to hold the data and reads through it a byte at a time.
It processes the main body of the buffer but there will be bytes at the start which
should be part of the last line of the previous file, and vice-versa. These fragments are
stored and processed separately at the end.

This version takes 20 seconds to run.

### 3. A version of the above but using sun.misc.unsafe

This is based on the `ByteBuffer` version but uses _sun.misc.Unsafe_ to directly read the bytes.
There are additional optimisations where methods are inlined or references to buffer
slices are used instead of making copies of arrays.

After several rounds of profiling and optimising, this runs in less than 12 seconds.

### 4.  Using Arrays for (almost) Everything

After finding that _Unsafe_ isn't actually faster than using byte arrays, this version
reverted back to using standard array reading. It also replaces HashMap with a custom array-backed
map implementation. Further optimisations mean this can run in under 8 seconds.
