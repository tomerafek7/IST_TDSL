import java.util.ArrayList;

public class ISTInnerNode<V> extends  ISTNode<V>{
    int numOfChildren;
    ArrayList<Integer> keys;
    ArrayList<ISTNode<V>> children;
    int updateCount;
    int activeTX;
    int waitQueueIndex;
    int numOfLeaves; // used only in rebuild
    boolean finishedCount; // used only in rebuild
    boolean rebuildFlag;
    ISTRebuildObject<V> rebuildObject;

    public ISTInnerNode(ArrayList<ISTSingleNode<V>> childrenList, int leaves){
        numOfLeaves = leaves;
        finishedCount = false;
        updateCount = 0;
        activeTX = 0;
        waitQueueIndex = 0;
        numOfChildren = childrenList.size();
        children = new ArrayList<ISTNode<V>>(childrenList);
        rebuildFlag = false;
        keys = new ArrayList<>();
        //children = childrenList; TODO: maybe this is valid and better

        for (int i=1; i<childrenList.size(); i++){
            keys.add(childrenList.get(i).key);

        }
        minKey = keys.get(0);
        maxKey = keys.get(keys.size()-1);
    }

    public ISTInnerNode(int numOfChildrenReceived, int leaves) {
        minKey = 0;
        maxKey = 0;
        numOfLeaves = leaves;
        finishedCount = false;
        updateCount = 0;
        activeTX = 0;
        waitQueueIndex = 0;
        numOfChildren = numOfChildrenReceived;
        rebuildFlag = false;
        keys = new ArrayList<>();
        children = new ArrayList<ISTNode<V>> ();
    }

}
