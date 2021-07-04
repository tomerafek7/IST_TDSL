import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

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
    AtomicReference<ISTRebuildObject> rebuildObjectAtomicReference;
    HashSet<Long> activeThreadsSet;
    //boolean rebuildFlag; rebuildFlag == (activeTX = -1)
    int debugNumOfLeaves; // used only in debug

    public ISTInnerNode(List<ISTNode> childrenList, int leaves){
        numOfLeaves = 0;
        finishedCount = false;
        updateCount = 0;
        activeTX = new AtomicInteger();
        waitQueueIndex = new AtomicInteger();
        numOfChildren = childrenList.size();
        children = new ArrayList<>(numOfChildren);
//        children = new ArrayList<>(childrenList);

        //rebuildFlag = false;
        debugNumOfLeaves = 0;
        keys = new ArrayList<>(numOfChildren -1);
        rebuildObjectAtomicReference = new AtomicReference<>(null);
        activeThreadsSet = new HashSet<>();
//        children = childrenList; TODO: maybe this is valid and better

        for (int i=0; i<childrenList.size(); i++){
            if(childrenList.get(i).single == null){
                int x = 1;
            }
            children.add(new ISTNode(childrenList.get(i).single.key, childrenList.get(i).single.value, childrenList.get(i).single.isEmpty));
            if(i>0) keys.add(childrenList.get(i).single.key);
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
        activeThreadsSet = new HashSet<>();
        rebuildObjectAtomicReference = new AtomicReference<>(null);
        children = new ArrayList<ISTNode> (numOfChildrenReceived);
        for (int i = 0; i<numOfChildrenReceived;i++){//we need to intitialize all the objects in the list
            children.add(null);
            if (i != 1){
                keys.add(null);
            }
        }
    }

}
