import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import com.google.common.base.Stopwatch;

public class ISTWorkLoad {

    IST tree;
    List<Integer> localList;
    Random random;
    int numThreads;
    int numTX;
    int numOpsPerTX;
    double readRatio;
    double writeRatio;
    double deleteRatio;
    int maxKey;
    int startAmountOfKeys;
    int rebuildCollaborationThreshold;
    int rebuildMinTreeLeafSize;

    ISTWorkLoad(HashMap<String, String> config){
        localList = new ArrayList<>();
        random = new Random(0);
        numThreads = Integer.parseInt(config.get("numThreads"));
        numTX = Integer.parseInt(config.get("numTX"));
        numOpsPerTX = Integer.parseInt(config.get("numOpsPerTX"));
        readRatio = Double.parseDouble(config.get("readRatio"));
        writeRatio = Double.parseDouble(config.get("writeRatio"));
        deleteRatio = Double.parseDouble(config.get("deleteRatio"));
        maxKey = Integer.parseInt(config.get("maxKey"));
        startAmountOfKeys = Integer.parseInt(config.get("startAmountOfKeys"));
        rebuildMinTreeLeafSize = Integer.parseInt(config.get("rebuildMinTreeLeafSize"));
        rebuildCollaborationThreshold = Integer.parseInt(config.get("rebuildCollaborationThreshold"));

        tree = new IST(rebuildMinTreeLeafSize, rebuildCollaborationThreshold);

    }

    public static HashMap<String, String> parseConfig(String configFile){
        String line = "";
        String splitBy = ",";
        HashMap<String, String> configMap = new HashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(configFile));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {
                String[] param = line.split(splitBy);    // use comma as separator
                assert param.length == 2: "Config CSV file is not in the right format!";
                configMap.put(param[0], param[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return configMap;
    }

    public List<ISTTask> createTasks(){
        List<ISTTask> tasksList = new ArrayList<>();
        for(int i=0; i<numTX; i++){
            List<ISTOperation> opList = new ArrayList<>();
            opList.addAll(addInserts((int) (numOpsPerTX * writeRatio)));
            opList.addAll(addRemoves((int) (numOpsPerTX * deleteRatio)));
            opList.addAll(addLookups((int) (numOpsPerTX * readRatio)));
            tasksList.add(new ISTTask(opList, tree));
        }
        return tasksList;
    }

    public List<ISTOperation> addInserts(int num){
        List<ISTOperation> tmpList = new ArrayList<>();
        for (int i = 0; i<num; i++){
            int key = random.nextInt(maxKey);
            int value = random.nextInt();
            tmpList.add(new ISTOperation(key,value,"insert"));
            localList.add(key);
        }
        return tmpList;
    }

    public List<ISTOperation> addRemoves(int num){
        List<ISTOperation> tmpList = new ArrayList<>();
        for (int i = 0; i<num; i++){
            int index = random.nextInt(localList.size());
            int key = localList.get(index);
            tmpList.add(new ISTOperation(key,0,"remove"));
            localList.remove(index);
        }
        return tmpList;
    }

    public List<ISTOperation> addLookups(int num){
        List<ISTOperation> tmpList = new ArrayList<>();
        for (int i = 0; i<num; i++){
            tmpList.add(new ISTOperation(localList.get(random.nextInt(localList.size())),0,"lookup"));
        }
        return tmpList;
    }

    public void warmUpTree(){
        List<ISTOperation> opList = addInserts(startAmountOfKeys);
        Thread thread = new Thread(new ISTTask(opList, tree));
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // after inserting all keys - lookup each one in order to force rebuild
        List<ISTOperation> lookupList = new ArrayList<>();
        for(ISTOperation op : opList){
            lookupList.add(new ISTOperation(op.key, 0, "lookup"));
        }
        thread = new Thread(new ISTTask(lookupList, tree));
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        tree.rebuild(tree.root.inner.children.get(0),tree.root,0);
        if(TX.DEBUG_MODE_IST) {
            tree.checkLevels();
        }
//        tree.debugCheckRebuild();
    }


    public void executeTasks(List<ISTTask> tasksList){
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        for(ISTTask task : tasksList){
            pool.execute(task);
        }
        pool.shutdown();
        try {
            pool.awaitTermination(30, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        System.out.println("Starting IST WorkLoad Test");
        ISTWorkLoad workLoad = new ISTWorkLoad(parseConfig(args[0]));
        workLoad.warmUpTree(); // first, "warm-up" the tree
        List<ISTTask> tasksList = workLoad.createTasks(); // create all tasks
        Stopwatch stopwatch = Stopwatch.createStarted();// measure time
        workLoad.executeTasks(tasksList); // now execute all tasks
        long millis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("Finished IST WorkLoad after " + millis + " [ms]");
        System.out.println("Num Of Aborts = " + TX.abortCount);
    }

}



