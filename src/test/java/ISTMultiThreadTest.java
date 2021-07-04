
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ISTMultiThreadTest
{
    @Test
    public void Test1()
    {
        Integer threadCounter = 0;
        BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<Runnable>(10000);

        CustomThreadPoolExecutor executor = new CustomThreadPoolExecutor(10,
                20, 5000, TimeUnit.MILLISECONDS, blockingQueue);

        executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r,
                                          ThreadPoolExecutor executor) {
//                System.out.println("DemoTask Rejected : "
//                        + ((ISTComplexRun) r).getName());
                System.out.println("Waiting for a second !!");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                System.out.println("Lets add another time : "
//                        + ((ISTComplexRun) r).getName());
                executor.execute(r);
            }
        });

        IST myTree = new IST();
        Random rand = new Random(1);
        List<Integer> keyList = new ArrayList<>();
        List<Integer> valueList = new ArrayList<>();
        int amountOfKeys = 1000;
        for (int i=0; i<amountOfKeys; i++) {
            keyList.add(rand.nextInt());
            valueList.add(rand.nextInt());
        }



        // Let start all core threads initially
        executor.prestartAllCoreThreads();
        int index = 0;
        while (true) {
            threadCounter++;

            // Adding threads one by one
            System.out.println("Adding ISTComplexRun : " + threadCounter);
            executor.execute(new ISTComplexRun("Task #" + threadCounter,myTree,keyList.subList(index,
                    index+10),valueList,"insert", true));
            index +=10;
            if (index == amountOfKeys) break;

            if (threadCounter == 1000)
                break;
        }
        index = 0;
        while (true) {
            threadCounter++;

            // Adding threads one by one
            System.out.println("Adding ISTComplexRun : " + threadCounter);
            executor.execute(new ISTComplexRun("Task #" + threadCounter,myTree,keyList.subList(index,
                    index+10),valueList,"remove", true));
            index +=10;
            if (index == amountOfKeys/2) break;

            if (threadCounter == 10000)
                break;
        }
        index = 0;
        while (true) {
            threadCounter++;

            // Adding threads one by one
            System.out.println("Adding ISTComplexRun : " + threadCounter);
            executor.execute(new ISTComplexRun("Task #" + threadCounter,myTree,keyList.subList(index,
                    index+10),valueList,"lookup", index<500));
            index +=10;
            if (index == amountOfKeys) break;

            if (threadCounter == 10000)
                break;
        }
    }



}






