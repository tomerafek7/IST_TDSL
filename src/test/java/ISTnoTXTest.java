import org.junit.Assert;
import org.junit.Test;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ISTnoTXTest {


    @Test
    public void testCreation(){
        IST myTree = new IST();
        Assert.assertNotNull(myTree.root);
        Assert.assertTrue(((ISTSingleNode) myTree.root.children.get(0)).isEmpty);
    }

    @Test
    public void testInsert(){
        IST myTree = new IST();
        myTree.insert(3,"hi");
        Assert.assertEquals(myTree.lookup(3), "hi");
        myTree.insert(3,"bye");
        Assert.assertEquals(myTree.lookup(3), "bye");
        myTree.insert(100,"abc");
        Assert.assertEquals(myTree.lookup(100), "abc");
        myTree.insert(-100,"abcd");
        Assert.assertEquals(myTree.lookup(-100), "abcd");
        Assert.assertEquals("bye", myTree.lookup(3));
        Assert.assertEquals("abc", myTree.lookup(100));
        myTree.insert(131,"___");
        Assert.assertEquals("___", myTree.lookup(131));
        Assert.assertEquals("bye", myTree.lookup(3));
        Assert.assertEquals("abc", myTree.lookup(100));
        myTree.print();
        myTree.checkRep();
    }

    @Test
    public void testRemove() {
        IST myTree = new IST();
        myTree.insert(3,"hi");
        Assert.assertEquals(myTree.lookup(3), "hi");
        myTree.insert(3,"bye");
        Assert.assertEquals(myTree.lookup(3), "bye");
        myTree.insert(100,"abc");
        Assert.assertEquals(myTree.lookup(100), "abc");
        myTree.insert(-100,"abcd");
        Assert.assertEquals(myTree.lookup(-100), "abcd");
        Assert.assertEquals("bye", myTree.lookup(3));
        Assert.assertEquals("abc", myTree.lookup(100));
        myTree.insert(131,"___");
        Assert.assertEquals("___", myTree.lookup(131));
        Assert.assertEquals("bye", myTree.lookup(3));
        Assert.assertEquals("abc", myTree.lookup(100));
        Assert.assertThrows(AssertionError.class,() -> myTree.remove(7));
        myTree.remove(131);
        Assert.assertNull(myTree.lookup(131));
        Assert.assertThrows(AssertionError.class,() -> myTree.remove(131));
        myTree.remove(3);
        Assert.assertNull(myTree.lookup(3));
        Assert.assertThrows(AssertionError.class,() -> myTree.remove(-65));
        Assert.assertEquals("abc", myTree.lookup(100));
        Assert.assertEquals("abcd", myTree.lookup(-100));
    }

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

        for (int i=0; i<1000; i++) {
            if (valueList.get(i) == 56){
                i = i + 0;
            }
            myTree.insert(keyList.get(i),valueList.get(i));
            //System.out.println(valueList.get(i));
            Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
        }
        for (int i=0; i<1000; i++) {
            if (valueList.get(i) == 878){
                i = i + 0;
            }
            Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
        }

        for (int i=0; i<1000; i++) {
            //System.out.println(keyList.get(i));
            if (keyList.get(i) == 869){
                i = i + 0;
            }
            myTree.remove(keyList.get(i));
            Assert.assertNull(myTree.lookup(keyList.get(i)));
        }
    }

    @Test
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
                IST.debugPrintNumLeaves(myTree.root.children.get(0));
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
                IST.debugPrintNumLeaves(myTree.root.children.get(0));
            }
        }
    }

    @Test
    public void testRebuild(){
        IST myTree = new IST();
        myTree.insert(3,"hi");
        Assert.assertEquals(myTree.lookup(3), "hi");
        myTree.insert(3,"bye");
        Assert.assertEquals(myTree.lookup(3), "bye");
        myTree.insert(100,"abc");
        Assert.assertEquals(myTree.lookup(100), "abc");
        myTree.insert(-100,"abcd");
        Assert.assertEquals(myTree.lookup(-100), "abcd");
        Assert.assertEquals("bye", myTree.lookup(3));
        Assert.assertEquals("abc", myTree.lookup(100));
        myTree.insert(131,"___");
        Assert.assertEquals("___", myTree.lookup(131));
        Assert.assertEquals("bye", myTree.lookup(3));
        Assert.assertEquals("abc", myTree.lookup(100));
        //myTree.print();
        myTree.insert(1,"___");
        myTree.insert(53,"___");
        myTree.insert(200,"___");
        myTree.insert(32,"___");
        //myTree.print();
        myTree.insert(2,"___");
        myTree.insert(37,"___");
        myTree.insert(166,"___");
        myTree.insert(15,"___");
        myTree.insert(3,"___");
        //myTree.print();
        myTree.insert(38,"___");
        myTree.insert(167,"___");
        myTree.insert(13,"___");
        //myTree.print();
    }
}
