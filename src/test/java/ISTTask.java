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
                    System.out.println("Starting task #" + TX.lStorage.get().TxNum + ", TID = " + Thread.currentThread().getId());
                    for (ISTOperation operation: operations){
                        switch (operation.type){
                            case "insert":
                                System.out.println("insert  , TXnum: " + TX.lStorage.get().TxNum + ", key: " +operation.key);
                                tree.insert(operation.key, operation.value);
                                break;
                            case "remove":
                                System.out.println("remove  , TXnum: " + TX.lStorage.get().TxNum + ", key: " +operation.key);
                                tree.remove(operation.key);
                                break;
                            case "lookup":
                                System.out.println("lookup  , TXnum: " + TX.lStorage.get().TxNum + ", key: " +operation.key);
                                tree.lookup(operation.key);
                                break;
                        }
                    }

                } finally {
                    TX.TXend();
                    tree.checkLevels();
//                        TXLibExceptions excep = new TXLibExceptions();
//                        throw excep.new AbortException();
                }
            } catch (TXLibExceptions.AbortException exp) {
                TX.print("abort");
                continue;
            }
            break;
        }
    }
}
