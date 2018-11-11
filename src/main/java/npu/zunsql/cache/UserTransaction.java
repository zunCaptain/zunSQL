package npu.zunsql.cache;

//import javax.print.DocFlavor;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

public class UserTransaction {
	protected static int transCount = 0;
	protected int transID;
//    protected static final String SUFFIX_JOURNAL = "-user-journal";

	protected ReadWriteLock lock;

	public UserTransaction(ReadWriteLock lock) {
		this.lock = lock;
		this.transID = transCount++;
	}

	public void begin() {
//            File journal = new File(Integer.toString(this.transID)+SUFFIX_JOURNAL);
//            try
//            {
//                if(!journal.exists())
//                {
//                    Boolean bool = journal.createNewFile();
//                    System.out.println("File created: "+bool);
//                }
//            }
//            catch (IOException e)
//            {
//                e.printStackTrace();
//            }

//            this.lock.writeLock().lock();

	}

	public void commit() {
//    	this.lock.writeLock().unlock();
	}

	public void rollback() {
//    	this.lock.writeLock().unlock();
	}
}
