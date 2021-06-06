import java.util.ArrayList;

public class ISTInnerNode<V> extends  ISTNode<V>{
    int numOfChildren;
    int numOfLeaves; // used only in rebuild
    ArrayList<Integer> keys;
    ArrayList<ISTNode<V>> children;
    int updateCount;
    int activeTX;
    int waitQueueIndex;
    ISTInnerNode(ArrayList<ISTSingleNode<V>> childrenList){
        numOfLeaves = 0;
        updateCount = 0;
        activeTX = 0;
        waitQueueIndex = 0;
        numOfChildren = childrenList.size();
        children = new ArrayList<ISTNode<V>>(childrenList);
        //children = childrenList;

        for (int i=1; i<childrenList.size(); i++){
            keys.add(childrenList.get(i).key);

        }
        minKey = keys.get(0);
        maxKey = keys.get(keys.size()-1);
    }

    public ISTInnerNode(int numOfChildrenReceived) {
        numOfLeaves = 0;
        updateCount = 0;
        activeTX = 0;
        waitQueueIndex = 0;
        numOfChildren = numOfChildrenReceived;
        keys = new ArrayList<Integer> ();
        children = new ArrayList<ISTNode<V>> ();
    }
}
