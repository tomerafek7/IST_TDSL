import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ISTTXTest {

    @Test
    public void randomTest(){
        IST myTree = new IST();
        Random rand = new Random(1);
        List<Integer> keyList = new ArrayList<>();
        List<Integer> valueList = new ArrayList<>();
        for (int i=0; i<1000; i++) {
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

            for (int i = 0; i < 1000; i++) {
                myTree.remove(keyList.get(i));
                Assert.assertNull(myTree.lookup(keyList.get(i)));
            }
        } catch (TXLibExceptions.AbortException exp) {
            System.out.println("abort");
        } finally {
            TX.TXend();
        }

        try {
            TX.TXbegin();
            for (int i = 0; i < 1000; i++) {
                Assert.assertNull(myTree.lookup(keyList.get(i)));
            }
        } catch (TXLibExceptions.AbortException exp) {
        System.out.println("abort");
        } finally {
        TX.TXend();
        }
    }

    @Test
    public void randomTest3(){
        IST myTree = new IST();
        Random rand = new Random(1);
        List<Integer> keyList = new ArrayList<>();
        List<Integer> valueList = new ArrayList<>();
        for (int i=0; i<1000; i++) {
            keyList.add(i);
            valueList.add(i);
        }
        Collections.shuffle(keyList, rand);
        Collections.shuffle(valueList, rand);


        //System.out.println("after begin");
        for (int i = 0; i < 1000; i++) {
            try {
                TX.TXbegin();
                myTree.insert(keyList.get(i), valueList.get(i));
                //myTree.print();
                //System.out.println(valueList.get(i));
                Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
            } catch (TXLibExceptions.AbortException exp) {
                System.out.println("abort");
            } finally {
                TX.TXend();
            }
            //System.out.println("insert " + i);
        }
        for (int i = 0; i < 1000; i++) {
            Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
        }

        for (int i = 0; i < 1000; i++) {
            myTree.remove(keyList.get(i));
            Assert.assertNull(myTree.lookup(keyList.get(i)));
        }


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
    public void randomTest2(){
        IST myTree = new IST();
        Random rand = new Random(1);
        List<Integer> keyList = new ArrayList<>();
        List<Integer> valueList = new ArrayList<>();
        for (int i=0; i<10000; i++) {
            keyList.add(i);
            valueList.add(i);
        }
        Collections.shuffle(keyList, rand);
        Collections.shuffle(valueList, rand);

        for (int i=0; i<10000; i++) {
            myTree.insert(keyList.get(i),valueList.get(i));
            //System.out.println(valueList.get(i));
            Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
            if (i%1000 == 0 && i!=0) {
                IST.debugPrintNumLeaves(myTree.root.inner.children.get(0));
            }
        }
        for (int i=0; i<10000; i++) {
            Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
        }

        for (int i=0; i<10000; i++) {
            //System.out.println(keyList.get(i));
            myTree.remove(keyList.get(i));
            Assert.assertNull(myTree.lookup(keyList.get(i)));
            if (i%1000 == 0 && i!=0) {
                IST.debugPrintNumLeaves(myTree.root.inner.children.get(0));
            }
        }
    }
}
