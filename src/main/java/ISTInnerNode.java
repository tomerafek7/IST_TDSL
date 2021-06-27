import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ISTInnerNode {
    int numOfChildren;
    ArrayList<Integer> keys;
    ArrayList<ISTNode> children;
    int updateCount;
    AtomicInteger activeTX; // rebuildFlag == (activeTX = -1)
    AtomicInteger waitQueueIndex;
    int numOfLeaves; // used only in rebuild
    boolean finishedCount; // used only in rebuild
    ISTRebuildObject rebuildObject;
    //boolean rebuildFlag; rebuildFlag == (activeTX = -1)
    int debugNumOfLeaves; // used only in debug
    // TODO: add lock list for commit-time locking


    public ISTInnerNode(List<ISTNode> childrenList, int leaves){
        numOfLeaves = 0;
        finishedCount = false;
        updateCount = 0;
        activeTX = new AtomicInteger();
        waitQueueIndex = new AtomicInteger();
        numOfChildren = childrenList.size();
        children = new ArrayList<>(childrenList);
        //rebuildFlag = false;
        debugNumOfLeaves = 0;
        keys = new ArrayList<>(numOfChildren -1);
//        children = childrenList; TODO: maybe this is valid and better

        for (int i=1; i<childrenList.size(); i++){
            keys.add(childrenList.get(i).single.key);
        }

    }

    public ISTInnerNode(int numOfChildrenReceived, int leaves) {
        numOfLeaves = leaves;
        finishedCount = false;
        updateCount = 0;
        activeTX = new AtomicInteger();
        waitQueueIndex = new AtomicInteger();
        numOfChildren = numOfChildrenReceived;
//        rebuildFlag = false;
        debugNumOfLeaves = 0;
        keys = new ArrayList<>();
        children = new ArrayList<ISTNode> (numOfChildrenReceived);
        for (int i = 0; i<numOfChildrenReceived;i++){//we need to intitialize all the objects in the list
            children.add(null);
            if (i != 1){
                keys.add(null);
            }
        }
    }

}
