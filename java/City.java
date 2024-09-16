
class City implements Comparable<City> {
    public String name;
    public int measurements = 0;
    public double total = 0;
    public double maxT = -Double.MAX_VALUE;
    public double minT = Double.MAX_VALUE;

    static City newCity(String line) {
        String[] bits = line.split(";");
        return new City(bits[0], Double.parseDouble(bits[1]));
    }

    public City() {
    }

    public City(String name, double value) {
        this.name = name;
        this.total = value;
        this.measurements = 1;
        this.maxT = value;
        this.minT = value;
    }

    String getName() {
        return name;
    }

    City merge(City other) {
        this.name = other.name;
        this.total += other.total;
        this.measurements += other.measurements;
        this.maxT = Math.max(this.maxT, other.maxT);
        this.minT = Math.min(this.minT, other.minT);
        return this;
    }

    static void combine(City c1, City c2) {
        c1.merge(c2);
    }

    @Override
    public int compareTo(City other) {
        return this.name.compareTo(other.name);
    }
}
