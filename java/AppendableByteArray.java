import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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

    AppendableByteArray(byte[] array) {
        length = array.length;
        buffer = array;
    }

    byte[] getBuffer() {
        return Arrays.copyOfRange(buffer, 0, length);
    }

    void addByte(byte b) {
        buffer[length++] = b;
//        if (length > buffer.length) {
//            System.err.println("Too many characters in " + new String(buffer, StandardCharsets.UTF_8));
//            throw new BufferOverflowException();
//            // TODO: increase buffer size if we really want to be flexible
//        }
    }

    void appendArray(AppendableByteArray toAppend) {
        System.arraycopy(toAppend.buffer, 0, buffer, length, toAppend.length);
        length += toAppend.length;
    }

    void appendArray(byte[] toAppend) {
        System.arraycopy(toAppend, 0, buffer, length, toAppend.length);
        length += toAppend.length;
    }

    void rewind() {
        length = 0;
    }

    public String asString() {
        return new String(buffer, 0, length, StandardCharsets.UTF_8);
    }
}
