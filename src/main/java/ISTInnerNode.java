import java.util.ArrayList;

public class ISTInnerNode<K,V> extends  ISTNode<K,V>{
    int numOfChildren;
    int numOfLeaves;
    ArrayList<K> keys;
    ArrayList<ISTNode<K,V>> children;
    int updateCount;
    int activeTX;
    int waitQueueIndex;

}
