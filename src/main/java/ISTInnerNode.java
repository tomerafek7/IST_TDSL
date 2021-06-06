import java.util.ArrayList;

public class ISTInnerNode<V> extends  ISTNode<V>{
    int numOfChildren;
    int numOfLeaves;
    ArrayList<Integer> keys;
    ArrayList<ISTNode<V>> children;
    int updateCount;
    int activeTX;
    int waitQueueIndex;

}
