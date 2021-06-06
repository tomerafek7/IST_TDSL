public class ISTSingleNode<V> extends ISTNode<V>{
    Integer key;
    V value;
    boolean isEmpty;

    ISTSingleNode(Integer key, V value){
       key = key;
       value = value;
       isEmpty = false;
    }

}



