public class ISTSingleNode<V> extends ISTNode<V>{
    Integer key;
    V value;
    boolean isEmpty;

    ISTSingleNode(Integer key, V value, boolean isEmpty){
       this.key = key;
       this.value = value;
       this.isEmpty = isEmpty;
    }

}



