public class ISTSingleNode<V> extends ISTNode<V>{
    Integer key;
    V value;
    boolean isEmpty;

    ISTSingleNode(Integer key, V value, boolean isEmpty){
       this.key = key;
       this.value = value;
       this.isEmpty = isEmpty;
       this.minKey = key;
       this.maxKey = key;
    }

    @Override
    public String toString() {
        return key.toString();
    }
}



