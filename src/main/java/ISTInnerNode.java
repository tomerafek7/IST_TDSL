import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class ISTInnerNode {
    int numOfChildren;
    ArrayList<Integer> keys;
    ArrayList<ISTNode> children;
    int updateCount;
    long needRebuildVersion; // for synchronizing the start of the rebuild operation
    AtomicInteger activeTX; // rebuildFlag == (activeTX = ACTIVE_REBUILD_MARK) (const defined in IST)
    AtomicInteger waitQueueIndexCount; // for rebuild
    AtomicInteger waitQueueIndexRebuild; // for rebuild
    int numOfLeaves; // used only in rebuild
    boolean finishedCount; // used only in rebuild
    ISTRebuildObject rebuildObject;
    AtomicReference<ISTRebuildObject> rebuildObjectAtomicReference;
    Set<Long> activeThreadsSet; // holds all of the active TXs in this node's subtree
    int debugNumOfLeaves; // used only in debug
    ReentrantLock debugLock; // debug

    public ISTInnerNode(List<ISTNode> childrenList, int leaves){
        numOfLeaves = 0;
        finishedCount = false;
        updateCount = 0;
        needRebuildVersion = -1L;
        activeTX = new AtomicInteger();
        waitQueueIndexCount = new AtomicInteger();
        waitQueueIndexRebuild = new AtomicInteger();
        numOfChildren = childrenList.size();
        children = new ArrayList<>(numOfChildren);
        debugNumOfLeaves = 0;
        keys = new ArrayList<>(numOfChildren -1);
        rebuildObjectAtomicReference = new AtomicReference<>(null);
        activeThreadsSet = ConcurrentHashMap.newKeySet();

        debugLock = new ReentrantLock(); // debug
        for (int i=0; i<childrenList.size(); i++){
            children.add(new ISTNode(childrenList.get(i).single.key, childrenList.get(i).single.value, childrenList.get(i).single.isEmpty));
            if(i>0) keys.add(childrenList.get(i).single.key);
        }
    }

    public ISTInnerNode(int numOfChildrenReceived, int leaves) {
        numOfLeaves = leaves;
        finishedCount = false;
        updateCount = 0;
        needRebuildVersion = -1L;
        activeTX = new AtomicInteger();
        waitQueueIndexCount = new AtomicInteger();
        waitQueueIndexRebuild = new AtomicInteger();
        numOfChildren = numOfChildrenReceived;
        debugNumOfLeaves = 0;
        keys = new ArrayList<>();
        activeThreadsSet = ConcurrentHashMap.newKeySet();
        rebuildObjectAtomicReference = new AtomicReference<>(null);
        children = new ArrayList<ISTNode> (numOfChildrenReceived);
        debugLock = new ReentrantLock(); // debug
        for (int i = 0; i<numOfChildrenReceived;i++){//we need to intitialize all the objects in the list
            children.add(null);
            if (i != 1){
                keys.add(null);
            }
        }
    }

}
