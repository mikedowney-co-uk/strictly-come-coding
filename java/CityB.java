import java.nio.charset.StandardCharsets;

/**
 * A version of City which uses a byte array instead of a String
 */
class CityB {
    public byte[] name;
    public int measurements = 0;
    public double total = 0;
    public double maxT = Double.MIN_VALUE;
    public double minT = Double.MAX_VALUE;
    public long hashCode;

    CityB(byte[] name, long hash) {
        this.name = name;
        this.hashCode = hash;
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

    public void combine_results(CityB city) {
        measurements += city.measurements;
        total += city.total;
//            maxT = Math.max(maxT, city.maxT);
//            minT = Math.min(minT, city.minT);
        if (city.maxT > maxT) {
            maxT = city.maxT;
        }
        if (city.minT < minT) {
            minT = city.minT;
        }
    }

    static CalculateByteBufferCharArray.CityAndTemp processRow(String s) {
        String[] bits = s.split(";");
        return new CalculateByteBufferCharArray.CityAndTemp(bits[0].getBytes(StandardCharsets.UTF_8), Double.parseDouble(bits[1]));
    }
}