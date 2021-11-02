import org.junit.Assert;

import java.util.List;

public class ISTTask implements  Runnable{
    List <ISTOperation> operations;
    IST tree;
    ISTTask (List<ISTOperation> operations,IST tree){
        this.tree = tree;
        this.operations = operations;

    }
    @Override
    public void run() {
        while (true){
            try {
                try {
                    TX.TXbegin();
                    for (ISTOperation operation: operations){
                        switch (operation.type){
                            case "insert":
                                TX.print("insert  , TXnum: " + TX.lStorage.get().TxNum + ", key: " +operation.key);
                                tree.insert(operation.key, operation.value);
//                                Assert.assertEquals(operation.value, tree.lookup(operation.key);
                            case "remove":
                                TX.print("remove  , TXnum: " + TX.lStorage.get().TxNum + ", key: " +operation.key);
                                tree.remove(operation.key);
                            case "lookup":
                                TX.print("lookup  , TXnum: " + TX.lStorage.get().TxNum + ", key: " +operation.key);
                                tree.lookup(operation.key);
                        }

                    }

                } finally {
                    TX.TXend();
                    if(TX.DEBUG_MODE_IST) {
                        tree.debugCheckRebuild();
                        tree.checkLevels();
                    }
//                        TXLibExceptions excep = new TXLibExceptions();
//                        throw excep.new AbortException();
                }
            }catch (TXLibExceptions.AbortException exp) {
                TX.print("abort");
                continue;
            }
            break;
        }


    }
}
