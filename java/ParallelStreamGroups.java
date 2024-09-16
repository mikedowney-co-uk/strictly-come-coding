
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ParallelStreamGroups {

    String file = "../measurements.txt";

    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();
        new ParallelStreamGroups().go();
        long endTime = System.currentTimeMillis();
        System.out.println("Took " + (endTime - startTime) / 1000 + " s");
    }
    private void go() throws IOException {
        try (Stream<String> lines = Files.lines(Path.of(file))) {
//        try(Stream<String> lines = new BufferedReader(new FileReader(file)).lines()) {
            Set<City> overallResults =
                    new TreeSet<>(lines.parallel().
                            map(City::newCity).collect(
                                    Collectors.groupingBy(
                                            City::getName,
                                            CityCollector.toCityList())
                            ).values());
            for (City city : overallResults) {
                System.out.printf("%s=%.1f/%.1f/%.1f\n",
                        city.name,
                        city.minT,
                        city.total / city.measurements,
                        city.maxT);
            }
        }
    }
}

class CityCollector implements Collector<City, City, City> {

    public static CityCollector toCityList() {
        return new CityCollector();
    }

    /**
     * @return
     */
    @Override
    public Supplier<City> supplier() {
        return City::new;
    }

    @Override
    public BiConsumer<City, City> accumulator() {
        return City::combine;
    }

    @Override
    public BinaryOperator<City> combiner() {
        return City::merge; // not used?
    }

    @Override
    public Function<City, City> finisher() {
        return (city -> city);
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of();
    }
}
