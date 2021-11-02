import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ISTWorkLoad {

    int numThreads;
    int numTX;
    int numOpsPerTX;
    double readRatio;
    double writeRatio;
    double deleteRatio;
    int maxKey;
    int startAmountOfKeys;
    List<Integer> localList;

    ISTWorkLoad(HashMap<String, String> config){
        numThreads = Integer.parseInt(config.get("numThreads"));
        numTX = Integer.parseInt(config.get("numTX"));
        numOpsPerTX = Integer.parseInt(config.get("numOpsPerTX"));
        readRatio = Double.parseDouble(config.get("readRatio"));
        writeRatio = Double.parseDouble(config.get("writeRatio"));
        deleteRatio = Double.parseDouble(config.get("deleteRatio"));
        maxKey = Integer.parseInt(config.get("maxKey"));
        startAmountOfKeys = Integer.parseInt(config.get("startAmountOfKeys"));
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

    public List<ISTOperation> addInserts(int num){
        Random random = new Random(1);
        List<ISTOperation> tmpList = new ArrayList<>();
        for (int i = 0; i<num; i++){
            int key = random.nextInt(maxKey);
            int value = random.nextInt();
            tmpList.add(new ISTOperation(key,value,"insert"));
            localList.add()
        }
        return tmpList;
    }

    public List<ISTOperation> addRemoves(int num){
        List<Integer> localList = new ArrayList<>();
        Random random = new Random(1);
        List<ISTOperation> tmpList= new ArrayList<>();
        for (int i = 0; i<num; i++){
            tmpList.add(new ISTOperation(localList.get(random.nextInt(localList.size())),0,"remove"));
        }
    }

    public List<ISTOperation> addLookups(int num){
        List<Integer> localList = new ArrayList<>();
        Random random = new Random(1);
        List<ISTOperation> tmpList= new ArrayList<>();
        for (int i = 0; i<num; i++){
            tmpList.add(new ISTOperation(localList.get(random.nextInt(localList.size())),0,"lookup"));
        }
    }

    public static void main(String[] args){
        ISTWorkLoad workLoad = new ISTWorkLoad(parseConfig(args[0]));
        System.out.println("numThreads = " + workLoad.numThreads);
        System.out.println("numTX = " + workLoad.numTX);
        System.out.println("numOpsPerTX = " + workLoad.numOpsPerTX);
        System.out.println("readRatio = " + workLoad.readRatio);
        System.out.println("writeRatio = " + workLoad.writeRatio);
        System.out.println("deleteRatio = " + workLoad.deleteRatio);
        System.out.println("maxKey = " + workLoad.maxKey);
        System.out.println("startAmountOfKeys = " + workLoad.startAmountOfKeys);
    }

}



