package kissmydisc.repricer.utils;

public class Pair<K, V> {

    @Override
    public String toString() {
        return "Pair [first=" + first + ", second=" + second + "]";
    }

    private K first;
    private V second;

    public Pair(K first, V second) {
        this.first = first;
        this.second = second;
    }

    public K getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }

    public void setFirst(K k) {
        this.first = k;
    }

    public void setSecond(V v) {
        this.second = v;
    }

}
