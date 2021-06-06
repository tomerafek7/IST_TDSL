import org.junit.Assert;
import org.junit.Test;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ISTnoTXTest {


    @Test
    public void testCreation(){
        IST<String> myTree = new IST<>();
        Assert.assertNotNull(myTree.root);
        Assert.assertTrue(((ISTSingleNode<String>) myTree.root.children.get(0)).isEmpty);
    }

    @Test
    public void testInsert(){
        IST<String> myTree = new IST<>();
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
    }

    @Test
    public void testRemove() {
        IST<String> myTree = new IST<>();
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
        IST<Integer> myTree = new IST<>();
        Random rand = new Random();
        List<Integer> keyList = new ArrayList<>();
        List<Integer> valueList = new ArrayList<>();
        for (int i=0; i<1000; i++) {
            keyList.add(i);
            valueList.add(i);
        }
        Collections.shuffle(keyList);
        Collections.shuffle(valueList);

        for (int i=0; i<1000; i++) {
            myTree.insert(keyList.get(i),valueList.get(i));
            Assert.assertEquals(valueList.get(i), myTree.lookup(keyList.get(i)));
        }
        for (int i=0; i<1000; i++) {
            myTree.remove(keyList.get(i));
            Assert.assertNull(myTree.lookup(keyList.get(i)));
        }


    }
}
