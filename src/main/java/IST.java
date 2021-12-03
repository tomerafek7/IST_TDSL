import com.sun.tools.javac.util.Pair;
import java.util.ArrayList;

public class IST {

    ISTNode root;
    final static int INIT_SIZE = 1;
    final static int ACTIVE_REBUILD_MARK = -10; // arbitrarily chose -10. could be any negative number.
    static int rebuildMinTreeLeafSize;
    static int rebuildCollaborationThreshold;
    static double rebuildUpdatesRatioThreshold;
    static int rebuildMinUpdatesThreshold;

    IST(int rebuildMinTreeLeafSize, int rebuildCollaborationThreshold,
        double rebuildUpdatesRatioThreshold, int rebuildMinUpdatesThreshold){
        this.root = new ISTNode(INIT_SIZE, 0);
        this.root.minKey = Integer.MAX_VALUE;
        this.root.maxKey = Integer.MAX_VALUE;
        //this.root.keys.add(Integer.MAX_VALUE);
        this.root.inner.children.set(0,new ISTNode(0, null, true));
        this.root.inner.keys.set(0, Integer.MAX_VALUE);
        IST.rebuildMinTreeLeafSize = rebuildMinTreeLeafSize;
        IST.rebuildCollaborationThreshold = rebuildCollaborationThreshold;
        IST.rebuildUpdatesRatioThreshold = rebuildUpdatesRatioThreshold;
        IST.rebuildMinUpdatesThreshold = rebuildMinUpdatesThreshold;
    }

    public int interpolate(ISTNode node, Integer key){
        Integer minKey = node.minKey;
        Integer maxKey = node.maxKey;
        int numKeys = node.inner.numOfChildren - 1;

        // check boundaries:
        if (key < minKey) return 0;
        if (key >= maxKey) return numKeys;

        // estimate index:
        long enumerator = numKeys * ((long)key - (long)minKey);
        long denominator = (long)maxKey - (long)minKey;
        int index = (int)(enumerator/denominator);
        Integer indexKey = node.inner.keys.get(index);
        // check estimation:
        // TODO: enhancement: recursive interpolation ?
        if (key < indexKey){
            // going left in the key list
            for(int i = index - 1; i >= 0; i--){
                if (key >= node.inner.keys.get(i)){
                    return i+1; // the +1 is to return the idx in the children list
                }
            }
        } else if (key >= indexKey) {
            // going right in the key list
            for (int i = index + 1; i < numKeys; i++) {
                if (key < node.inner.keys.get(i)) {
                    return i;
                }
            }
        }

        // if we got here it's a bug (because if the key is >max or <min we should exit above)
        assert(false); return -1;

    }

    // traverse the *global* (public) part of the tree
    // returns the node in the location of <key> (even if the returned node has a different key)
    private ISTNode traverseTree(Integer key, ArrayList<Pair<ISTNode, Integer>> path, LocalStorage localStorage){
        ISTNode curNode = root;
        ISTNode parentNode = null;
        while (true){
            if (TX.DEBUG_MODE_IST) {
                if (curNode.inner != null && curNode.inner.rebuildObject != null && curNode.inner.activeTX.get() != -1) {
                    TX.print("ERROR!!! Path: " + path.toString());
                }
            }
            parentNode =  curNode;
            int idx = interpolate( curNode, key);
            path.add(new Pair<>(curNode, idx));
            curNode = parentNode.inner.children.get(idx);
            if ( curNode == null || curNode.isLocked()){ //TODO: why do we have null here? mystery!
                localStorage.earlyAbort = true;
                TX.print("middle abort - IST");
                TXLibExceptions excep = new TXLibExceptions();
                throw excep.new AbortException();
            }
            if(!curNode.isInner){ // reached a single
                localStorage.ISTPutIntoReadSet(curNode);
                // traverse at the local tree to see if there's more nodes to go to.
                ISTNode localNode = traverseLocalTree(curNode, key, localStorage);
                if (localNode == curNode){
                    return curNode;
                } else { // TODO: here we don't need to use write set (we are local), can optimize
                    return localNode;
                }
            } else { // still an inner
                if(localStorage.ISTReadSet.contains(curNode)){ //TODO : check performance
                    // aborting because someone have committed the node which is in our read set
                    localStorage.earlyAbort = true;
                    TX.print("middle abort - IST");
                    TXLibExceptions excep = new TXLibExceptions();
                    throw excep.new AbortException();
                }
                curNode = checkAndHelpRebuild(curNode, parentNode, idx, localStorage);
                if(!curNode.isInner) { // corner case of a rebuild with all empty leaves
                    return curNode;
                }
            }
        }
    }

    // traverse a *local* part of the tree (local means only this thread can see it)
    private ISTNode traverseLocalTree(ISTNode curNode, Integer key, LocalStorage localStorage) {
        ISTNode fakeNode = localStorage.ISTGetUpdatedNodeFromWriteSet(curNode);
        if (curNode == fakeNode){//debug purposes
            return curNode;
        }
        if (fakeNode.isInner){ // inner
            int idx = interpolate(fakeNode, key);
            ISTNode newNode = fakeNode.inner.children.get(idx);
            return traverseLocalTree(newNode, key, localStorage);
        } else { // single
            return traverseLocalTree(fakeNode, key, localStorage); // lookup/insert/remove will handle the changes from write-set
        }
    }

    // lookup operation
    // in case <key> exists in the tree - returns the corresponding value
    // otherwise - returns null
    public Object lookup(Integer key){
        LocalStorage localStorage = TX.lStorage.get();
        ArrayList<Pair<ISTNode, Integer>> path = new ArrayList<>();
        // traverse the tree
        ISTNode leaf = traverseTree(key, path, localStorage); // returns a single, not sure if an updated one.
        ISTSingleNode single = localStorage.ISTGetUpdatedNodeFromWriteSet(leaf).single; // brings us the updated one (maybe the same)
            if (!single.isEmpty && single.key.equals(key)){
            return single.value;
        } else {
            return null;
        }
    }

    // insert operation
    // in case <key> exists in the tree - replace its value with <value>
    // otherwise - creates a new node with <key>,<value>
    public void insert(Integer key, Object value){
        LocalStorage localStorage = TX.lStorage.get();
        if (TX.DEBUG_MODE_IST){ TX.print("(TID = " + localStorage.tid + "). INSERT BEGIN");}
        ArrayList<Pair<ISTNode, Integer>> path = new ArrayList<>();

        // traverse the tree
        ISTNode leaf = traverseTree(key, path, localStorage); // returns a single, not sure if an updated one.
        ISTSingleNode single = leaf.single;
        // get parent + idx from the last element in path (path is updated in traverseTree)
        ISTNode parentNode = path.get(path.size()-1).fst;
        Integer idx = path.get(path.size()-1).snd;
        if (!single.isEmpty){ // non-empty leaf
            if(single.key.equals(key)){ // same key --> just replace value.
                localStorage.ISTPutIntoWriteSet(leaf, false,null,key,value,false);
            } else { // different key --> split into 2 singles
                ArrayList<ISTNode> childrenList = new ArrayList<>();
                ISTNode newSingle1 = new ISTNode(single.key, single.value, false);
                ISTNode newSingle2 = new ISTNode(key, value, false);
                // choose the children order: (the first to insert must be the smaller one)
                if (newSingle1.single.key < newSingle2.single.key) {
                    childrenList.add(newSingle1);
                    childrenList.add(newSingle2);
                } else {
                    childrenList.add(newSingle2);
                    childrenList.add(newSingle1);
                }
                assert parentNode != null;
                assert idx != -1;
                localStorage.ISTPutIntoWriteSet(leaf,true, childrenList,null,null,true);
            }
        } else { // empty leaf
            localStorage.ISTPutIntoWriteSet(leaf,false, null, key, value, false);
        }

        // increment updates counters
        for (Pair<ISTNode, Integer> pair: path) {
            localStorage.incUpdateList.add(pair.fst);
        }

    }

    // remove operation
    // in case <key> exists in the tree - removes the corresponding node and returns true
    // otherwise - returns false
    // remove meaning only marking the node as "empty"
    public boolean remove(Integer key){
        LocalStorage localStorage = TX.lStorage.get();
        ArrayList<Pair<ISTNode, Integer>> path = new ArrayList<>();
        // traverse the tree
        ISTNode leaf = traverseTree(key, path, localStorage); // returns a single, not sure if an updated one.
        ISTSingleNode single = localStorage.ISTGetUpdatedNodeFromWriteSet(leaf).single; // brings us the updated one (maybe the same)
        if (!single.isEmpty){ // non-empty leaf
            if(single.key.equals(key)){ // same key --> DELETE.
                localStorage.ISTPutIntoWriteSet(leaf, false, null,null,null,true);
            } else { // different key --> ERROR
                return false;
            }
        } else { // empty leaf --> ERROR
            return false;
        }
        // increment updates counters
        for (Pair<ISTNode, Integer> pair: path) {
            localStorage.incUpdateList.add(pair.fst);
        }
        return true;
    }

    // rebuild operation - called from checkAndHelpRebuild
    // initiates the rebuildObject if needed and call helpRebuild
    void rebuild(ISTNode rebuildRoot,ISTNode parent, int index){
        if (TX.DEBUG_MODE_IST) {
            LocalStorage localStorage = TX.lStorage.get(); // for debug only
            TX.print("(TID =" + localStorage.tid + ") rebuild. Node = " + rebuildRoot);
        }
        // try to change the atomic reference to rebuild object:
        rebuildRoot.inner.rebuildObjectAtomicReference.compareAndSet(null, new ISTRebuildObject(rebuildRoot,index,parent));
        rebuildRoot.inner.rebuildObject = rebuildRoot.inner.rebuildObjectAtomicReference.get(); // at this point someone must have changed the reference.
        rebuildRoot.inner.rebuildObject.helpRebuild();
        TX.print("Finished Rebuild");
    }

    // initiating rebuild operation if needed and possible
    // called from traverseTree each time we reach a new node
    ISTNode checkAndHelpRebuild(ISTNode root, ISTNode parent, int index, LocalStorage localStorage) {

        /* ******* CORNER CASES ******* */
        if (TX.DEBUG_MODE_IST) {
            assert !localStorage.ISTWriteSet.containsKey(root); // we assume that if we got here, it's with a global part of the tree.
        }
        if (!root.isInner) {
            return root; // corner case - rebuild is done with 0 leaves (all are empty) - so the new root is single
        }
        if (localStorage.decActiveList.contains(root)) { // free pass - in case this TX is already inside this node
            if (TX.DEBUG_MODE_IST) {
                assert (root.inner.activeTX.get() != ACTIVE_REBUILD_MARK) : "ERROR: (TID = " + localStorage.tid + ") ActiveTX shouldn't be = -1. threadSet = " + root.inner.activeThreadsSet.toString();
            }
            // no one else cannot start rebuild because this thread is inside the node, just to make sure..
            return root;
        }

        /* ******* NORMAL CASES ******* */

        // join rebuild if it's already started
        if (root.inner.activeTX.get() == ACTIVE_REBUILD_MARK) { // if (rebuild_flag)
            rebuild(root, parent, index); // to get the rebuild object (or create it in case it's null), and then call help rebuild
            if (TX.DEBUG_MODE_IST) {
                assert (parent.inner.children.get(index).inner.activeTX.get() != -1) : "ERROR: AFTER REBUILD: (TID = " + localStorage.tid + ") ActiveTX shouldn't be = -1";
            }
            return checkAndHelpRebuild(parent.inner.children.get(index), parent, index, localStorage); // call again, to make sure we hold the updated sub-tree root
        }

        // initiate rebuild if it's needed and possible to do now
        if (root.inner.needRebuildVersion >= 0L && localStorage.TxNum > root.inner.needRebuildVersion) { // enter this only if rebuild is needed AND this TX is younger than when rebuild was needed (to prevent deadlocks)
            TX.print("need rebuild");
            localStorage.stopwatchWaiting.start();
            boolean result = false;
            while (root.inner.activeTX.get() != ACTIVE_REBUILD_MARK) {// we wait for all transactions in sub-tree to be over and than rebuild should catch the sub-tree

                if (TX.DEBUG_MODE_IST) {
                    TX.print("(TID = " + localStorage.tid + ") Waiting for ActiveTX to be 0. activeTX = " + root.inner.activeTX.get() + ". Node = " + root + " . ThreadSet = [???]. needRebuildVersion = " + root.inner.needRebuildVersion + ". TxNum = " + localStorage.TxNum);
                }
                result = root.inner.activeTX.compareAndSet(0, ACTIVE_REBUILD_MARK);
            }
            localStorage.stopwatchWaiting.stop();
            if (result) {
                rebuild(root, parent, index); // to get the rebuild object (or create it in case it's null), and then call help rebuild
                return checkAndHelpRebuild(parent.inner.children.get(index), parent, index, localStorage); // call again, to make sure we hold the updated sub-tree root
            } else {
                return checkAndHelpRebuild(parent.inner.children.get(index), parent, index, localStorage); // call again, to make sure we hold the updated sub-tree root
            }
        }

        // increment the active counter and proceed traversing the tree
        while (true) {
            int active = root.inner.activeTX.get();
            boolean result = false;
            if (active == ACTIVE_REBUILD_MARK) { // rebuild has started after we passed the check above, go help
                return checkAndHelpRebuild(parent.inner.children.get(index), parent, index, localStorage);
            } else { // try to inc the active counter
                result = root.inner.activeTX.compareAndSet(active, active + 1);
                if (result) {
                    if (TX.DEBUG_MODE_IST) {
                        assert !root.inner.activeThreadsSet.contains(localStorage.tid) : "ERROR: Thread is already inside the activeThreadsSet";
                        assert !localStorage.decActiveList.contains(root) : "ERROR: node is already inside the decActiveList";
                    }

                    localStorage.decActiveList.add(root);
                    root.inner.activeThreadsSet.add(localStorage.tid); // adding the TID to the set so we could know that this thread has a free pass
                    return root; // return the updated sub-tree root, so the operation will continue with the updated tree // SUCCESS
                }
            }
        }

    }


/** *********** DEBUG FUNCTIONS *********** **/

    public void print(){
        System.out.println("IST: \n");
        _printIST(root, "");
        System.out.println("\n\n");
    }

    public void _printIST(ISTNode curNode, String indent) {
        if (!curNode.isInner) {
            String key = curNode.single.isEmpty ? "X" : curNode.single.key.toString();
            System.out.println(indent + key);
        }
        else {
            ISTNode curNodeInner = curNode;
            for(int i=curNodeInner.inner.numOfChildren-1; i>=0; i--){
                if(i < curNodeInner.inner.numOfChildren-1){
                    System.out.println(indent + curNodeInner.inner.keys.get(i));
                }
                _printIST(curNodeInner.inner.children.get(i), indent + "      ");
            }
        }
    }

    public void debugCheckRebuild(){
        TX.print("Debug Check Rebuild");

        ArrayList<Integer> numNeedRebuild= new ArrayList<>(100);
        for (int i = 0; i<100; i++){
            numNeedRebuild.add(i,0);
        }
        _debugCheckRebuild(root,1, numNeedRebuild);
        for (int i = 1; i<100; i++){
            if (numNeedRebuild.get(i) == 0){continue;}
            TX.print( "level :" + i + " numNeedRebuild : " + numNeedRebuild.get(i));
        }
    }
    public void _debugCheckRebuild(ISTNode curNode,Integer level ,ArrayList<Integer> numNeedRebuild) {
        if (curNode.isInner) {
            numNeedRebuild.set(level, numNeedRebuild.get(level) + curNode.inner.updateCount > -1L ? 0 : 1);
            for (int i = curNode.inner.numOfChildren - 1; i >= 0; i--) {
                _debugCheckRebuild(curNode.inner.children.get(i), level + 1, numNeedRebuild);
            }
        }
    }

    public void checkLevels(){
        ArrayList<Integer> numSingles= new ArrayList<>(100);
        ArrayList<Integer> numInners= new ArrayList<>(100);
        for (int i = 0; i<100; i++){
            numSingles.add(i, 0);
            numInners.add(i,0);
        }
        _checkLevels(root,1,numSingles,numInners);
        int countSingles = 0;
        for (int i = 1; i<100; i++){
            if (numSingles.get(i) == 0 && numInners.get(i) == 0){continue;}
            countSingles += numSingles.get(i);
            System.out.println( "level :" + i + "  singles:" + numSingles.get(i) + "  inners : " + numInners.get(i));
        }
        System.out.println("Num Of Singles = " + countSingles);
    }
    public void  _checkLevels(ISTNode curNode,Integer level ,ArrayList<Integer> numSingles,ArrayList<Integer> numInners) {
        if (!curNode.isInner) {
            if(!curNode.single.isEmpty) {
                numSingles.set(level, numSingles.get(level) + 1);
            }
        } else {
            numInners.set(level, numInners.get(level) + 1);
            for (int i = curNode.inner.numOfChildren - 1; i >= 0; i--) {


                _checkLevels(curNode.inner.children.get(i), level + 1, numSingles, numInners);
            }
        }
    }

    // check that the tree structure is correct
    public static boolean rebuildCheckRep(ISTNode node){
        return _checkRep(node, new ArrayList<>());
    }

    // check that the tree structure is correct
    public boolean checkRep(){
       return  _checkRep(root, new ArrayList<>());
    }

    public static boolean _checkRep(ISTNode curNode, ArrayList<Pair<Integer, String>> path) {

        if (!curNode.isInner) { // validate path
            if (!curNode.single.isEmpty) {
                Integer key = curNode.single.key;
                for (Pair<Integer, String> pair : path) {
                    if (pair.snd.equals("right")) {//assert key >= pair.fst : "key: " + key + ", Path: " + path.toString();
                        if (!(key >= pair.fst)) {
                            TX.print("key: " + key + ", Path: " + path.toString());
                            return false;
                        } else if (pair.snd.equals("left")) {
                            //assert key < pair.fst : "key: " + key + ", Path: " + path.toString();
                            if (!(key < pair.fst)) {
                                TX.print("key: " + key + ", Path: " + path.toString());
                                return false;
                            }
                        }
                    }
                }
            }
        } else { // inner node
            int i = 0;
            for (ISTNode child : curNode.inner.children) {
                ArrayList<Pair<Integer, String>> newPath = new ArrayList<>(path);
                if (i == 0) {
                    newPath.add(new Pair<>(curNode.inner.keys.get(i), "left"));
                } else {
                    newPath.add(new Pair<>(curNode.inner.keys.get(i - 1), "right"));
                }
               if  (!_checkRep(child, newPath)){
                   return false;
               }
                i++;
            }
        }
        return true;
    }


    public static int debugSubTreeCount(ISTNode curNode){

        if (!curNode.isInner){
            return curNode.single.isEmpty ? 0 : 1;
        }
        // here node is inner
        int keyCount = 0;
        for (ISTNode child : curNode.inner.children){
            if (!child.isInner){
                keyCount +=  child.single.isEmpty ? 0 : 1;
            } else { // inner
                keyCount += IST.debugSubTreeCount(child);
            }
        }
        curNode.inner.debugNumOfLeaves = keyCount;

        return keyCount;
    }

    public static void debugPrintNumLeaves (ISTNode curNode) {
        IST.debugSubTreeCount(curNode); // update debugNumOfLeaves field for all nodes in subtree
        // now, print only the desired data: (subtree root + sons)
        System.out.println("SubTree root # Leaves is: " + curNode.inner.debugNumOfLeaves);
        for (int i = 0; i <  curNode.inner.numOfChildren; i++) {
            System.out.println("Son #" + i + " # Leaves is: " + curNode.inner.children.get(i).inner.debugNumOfLeaves);
        }
    }

    public static ArrayList<ISTNode> debugCreateSortedList(ArrayList<ISTNode> list,
                                                ISTNode currentNode) {
        for (int i = 0; i < currentNode.inner.numOfChildren; i++) {
            ISTNode child = currentNode.inner.children.get(i);
            if (!child.isInner) { //child is single
                if (!child.single.isEmpty) { // child is not empty
                    //here we found a single node that should be added.
                    list.add(child);
                }
            } else { //child is inner node
                list = debugCreateSortedList(list, (child));
            }
        }
        return list;
    }
}

