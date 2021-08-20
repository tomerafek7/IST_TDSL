import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ISTTXTest {

    //@Test
    public void randomTest() {
        IST myTree = new IST();
        Random rand = new Random(1);
        List<Integer> keyList = new ArrayList<>();
        List<Integer> valueList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            keyList.add(i);
            valueList.add(i);
        }
        Collections.shuffle(keyList, rand);
        Collections.shuffle(valueList, rand);

        try {
            TX.TXbegin();
            //System.out.println("after begin");
            for (int i = 0; i < 1000; i++) {
                myTree.insert(keyList.get(i), valueList.get(i));
                //System.out.println(valueList.get(i));
                Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
                //System.out.println("insert " + i);
            }
            for (int i = 0; i < 1000; i++) {
                Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
            }
//            for (int i = 0; i < 1000; i++) {
//                myTree.remove(keyList.get(i));
//                Assert.assertNull(myTree.lookup(keyList.get(i)));
//            }
        } catch (TXLibExceptions.AbortException exp) {
            System.out.println("abort");
        } finally {
            TX.TXend();
        }

        try {
            TX.TXbegin();
//            for (int i = 0; i < 1000; i++) {
//                Assert.assertNull(myTree.lookup(keyList.get(i)));
//            }
        } catch (TXLibExceptions.AbortException exp) {
            System.out.println("abort");
        } finally {
            TX.TXend();
            myTree.rebuild(myTree.root.inner.children.get(0),myTree.root, 0 );
            myTree.checkLevels();
        }
    }

    //@Test
    public void randomTest3() throws FileNotFoundException {
        //System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output123.txt"))));
        IST myTree = new IST();
        Random rand = new Random(1);
        List<Integer> keyList = new ArrayList<>();
        List<Integer> valueList = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            keyList.add(i);
            valueList.add(i);
        }
        Collections.shuffle(keyList, rand);
        Collections.shuffle(valueList, rand);


        //System.out.println("after begin");
        for (int i = 0; i < 1000; i++) {
            try {
                TX.TXbegin();
                for (int j = 0; j < 100; j++) {
                    //System.out.println("i: " + i + " j: " + j);
                    myTree.insert(keyList.get(i * 100 + j), valueList.get(i * 100 + j));
                    //myTree.print();
                    //System.out.println(valueList.get(i));
                    Assert.assertEquals(valueList.get(i * 100 + j), myTree.lookup(keyList.get(i * 100 + j)));
                }
            } catch (TXLibExceptions.AbortException exp) {
                System.out.println("abort");
            } finally {
                TX.TXend();
//                myTree.checkLevels();
            }
        }
        myTree.debugCheckRebuild();
        for (int i = 0; i < 1000; i++) {
            try {
                TX.TXbegin();
                for (int j = 0; j < 100; j++) {
                    Assert.assertEquals(valueList.get(i * 100 + j), myTree.lookup(keyList.get(i * 100 + j)));
                }
            } catch (TXLibExceptions.AbortException exp) {
                System.out.println("abort");
            } finally {
                TX.TXend();
//                myTree.checkLevels();
            }
        }
        myTree.checkLevels();
        myTree.rebuild(myTree.root.inner.children.get(0),myTree.root, 0 );
        myTree.checkLevels();

        for (int i = 0; i < 1000; i++) {
            try {
                TX.TXbegin();
                for(int j=0; j<100; j++){
//                    System.out.println("i = " + i + " j = " + j);
                    myTree.remove(keyList.get(i*100+j));
                    if(i == 999 && j==99) System.out.println("last lookup!");
                    Assert.assertNull(myTree.lookup(keyList.get(i*100+j)));
                }
            } catch (TXLibExceptions.AbortException exp) {
                System.out.println("abort");
            } finally {
                TX.TXend();
//                myTree.checkLevels();
            }
            //System.out.println("insert " + i);
        }
        for (int i = 0; i < 1000; i++) {
            try {
                TX.TXbegin();
                for (int j = 0; j < 100; j++) {
                    Assert.assertNull(myTree.lookup(keyList.get(i * 100 + j)));
                }
            } catch (TXLibExceptions.AbortException exp) {
                System.out.println("abort");
            } finally {
                TX.TXend();
//                myTree.checkLevels();
            }
        }
        myTree.checkLevels();

//        myTree.rebuild(myTree.root.inner.children.get(0),myTree.root, 0 );
//        myTree.checkLevels();
//        for (int i = 0; i < 1000; i++) {
//            Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
//        }
//
//        for (int i = 0; i < 1000; i++) {
//            myTree.remove(keyList.get(i));
//            Assert.assertNull(myTree.lookup(keyList.get(i)));
//        }


//        try {
//            TX.TXbegin();
//            for (int i = 0; i < 1000; i++) {
//                Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
//            }
//        } catch (TXLibExceptions.AbortException exp) {
//            System.out.println("abort");
//        } finally {
//            TX.TXend();
//        }
    }

    //@Test
    public void randomTest2() {
        IST myTree = new IST();
        Random rand = new Random(1);
        List<Integer> keyList = new ArrayList<>();
        List<Integer> valueList = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            keyList.add(i);
            valueList.add(i);
        }
        Collections.shuffle(keyList, rand);
        Collections.shuffle(valueList, rand);

        for (int i = 0; i < 10000; i++) {
            myTree.insert(keyList.get(i), valueList.get(i));
            //System.out.println(valueList.get(i));
            Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
            if (i % 1000 == 0 && i != 0) {
                IST.debugPrintNumLeaves(myTree.root.inner.children.get(0));
            }
        }
        for (int i = 0; i < 10000; i++) {
            Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
        }

        for (int i = 0; i < 10000; i++) {
            //System.out.println(keyList.get(i));
            myTree.remove(keyList.get(i));
            Assert.assertNull(myTree.lookup(keyList.get(i)));
            if (i % 1000 == 0 && i != 0) {
                IST.debugPrintNumLeaves(myTree.root.inner.children.get(0));
            }
        }
    }

    //@Test
    public void simpleMultiThreadTest() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        IST myTree = new IST();
        int numThreads = 10;
        Random rn = new Random(0);
        ArrayList<Thread> threads = new ArrayList<>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            threads.add(new Thread(new ISTTXTest.ISTSimpleRun("T" + i, myTree, rn.nextInt(), latch)));
        }
        for (int i = 0; i < numThreads; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < numThreads; i++) {
            threads.get(i).join();
        }

    }

    void removeOutputFiles(){
        final File folder = new File("C:\\Users\\DELL\\IdeaProjects\\IST_TDSL");
        final File[] files = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept( final File dir,
                                   final String name ) {
                return name.matches( "output_T[0-9]*.txt" );
            }
        } );
        for ( final File file : files ) {
            if ( !file.delete() ) {
                System.err.println( "Can't remove " + file.getAbsolutePath() );
            }
        }
        final File folder2 = new File("C:\\Users\\DELL\\PycharmProjects\\IST_TDSL_Stats");
        final File[] files2 = folder2.listFiles(new FilenameFilter() {
            @Override
            public boolean accept( final File dir,
                                   final String name ) {
                return name.matches( "stats_T[0-9]*.csv" );
            }
        } );
        for ( final File file : files2 ) {
            if ( !file.delete() ) {
                System.err.println( "Can't remove " + file.getAbsolutePath() );
            }
        }
    }


    @Test
    public void complexMultiThreadTest() throws InterruptedException, FileNotFoundException {
//        System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
//        System.setErr(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
        // remove output_T* files before test
        removeOutputFiles();
        IST myTree = new IST();
        int numThreads = 10;
        Random rand = new Random(1);
        HashSet<Integer> keySet = new HashSet<>();
        List<Integer> valueList = new ArrayList<>();
        int amountOfKeys = 300000;
        while (keySet.size() != amountOfKeys) {
            keySet.add(rand.nextInt());
            valueList.add(rand.nextInt());
        }
        List<Integer> keyList =new ArrayList<>(keySet);

        // INSERTS
        System.out.println("Starting Inserts...\n");
        ArrayList<Thread> threads = new ArrayList<>(numThreads);
        int index = 0;
        for (int i = 0; i < numThreads; i++) {
            threads.add(new Thread(new ISTComplexRun("T" + i, myTree, keyList.subList(index,index+(amountOfKeys/numThreads)),
                    valueList.subList(index,index+(amountOfKeys/numThreads)),"insert",true)));
            index += amountOfKeys/numThreads;
        }
        for (int i = 0; i < numThreads; i++) {
            threads.get(i).start();
        }
        for (int i = 0; i < numThreads; i++) {
            threads.get(i).join();
        }
        try {
            myTree.checkLevels();
            assert myTree.checkRep();
        } catch (TXLibExceptions.AbortException e){
            System.out.println("Check Rep Failed. Exiting.");
            //System.exit(1);
        }
        myTree.debugCheckRebuild();
        System.out.println("Finished Inserts\n");

        // LOOKUPS
        System.out.println("Starting Lookups...\n");
        threads = new ArrayList<>(numThreads);
        index = 0;
        for (int i = 0; i < numThreads; i++) {
            threads.add(new Thread(new ISTComplexRun("T" + i, myTree, keyList.subList(index,index+(amountOfKeys/numThreads)),
                    valueList.subList(index,index+(amountOfKeys/numThreads)),"lookup",true)));
            index += amountOfKeys/numThreads;
        }
        for (int i = 0; i < numThreads; i++) {
            threads.get(i).start();
        }
        for (int i = 0; i < numThreads; i++) {
            threads.get(i).join();
        }
        myTree.debugCheckRebuild();
        myTree.checkLevels();
        System.out.println("Finished Lookups\n");

        // REMOVES
        System.out.println("Starting Removes...\n");
        threads = new ArrayList<>(numThreads);
        index = 0;
        for (int i = 0; i < numThreads; i++) {
            threads.add(new Thread(new ISTComplexRun("T" + i, myTree, keyList.subList(index,index+(amountOfKeys/numThreads)),
                    valueList.subList(index,index+(amountOfKeys/numThreads)),"remove",true)));
            index += amountOfKeys/numThreads;
        }
        for (int i = 0; i < numThreads; i++) {
            threads.get(i).start();
        }
        for (int i = 0; i < numThreads; i++) {
            threads.get(i).join();
        }
        myTree.debugCheckRebuild();
        myTree.checkLevels();
        System.out.println("Finished Removes\n");

        System.out.println("Test is done!\n");

        System.out.println("Num Of Aborts = " + TX.abortCount);

    }

    public class ISTSimpleRun implements Runnable {

        String name;
        IST tree;
        Integer key;
        CountDownLatch latch;

        public ISTSimpleRun(String name, IST tree, Integer key, CountDownLatch latch) {
            this.name = name;
            this.tree = tree;
            this.key = key;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException exp) {
                System.out.println(name + ": InterruptedException");
            }
            try {
                try {
                    TX.TXbegin();
                    tree.insert(key, "abc");
                    Assert.assertEquals("abc", tree.lookup(key));
                    tree.remove(key);
                    Assert.assertNull(tree.lookup(key));
                } finally {
                    TX.TXend();
               }
            } catch (TXLibExceptions.AbortException exp) {
                System.out.println("abort");
            }
        }
    }
}

