package streams;


import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

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
