package pt.uminho.di.raft.service.log;

import java.io.Serializable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;

/**
 *
 * @author fcampos
 */
public class LogEntry implements Serializable {

    static Logger logger = Logger.getLogger(LogEntry.class);

    private String uid;
    private Integer index;
    private Integer term;
    private String command;
    private Object parameters;
    private String response;
    private Boolean success;

    // mutex
    final Lock lock = new ReentrantLock();
    // condition
    boolean majorityReceived = false;
    final Condition majority = lock.newCondition();

    final Semaphore propagated = new Semaphore(1);

    private AtomicInteger responses;

    public LogEntry(String uid, Integer term, String command, Object parameters) {
        this.uid = uid;
        this.term = term;
        this.command = command;
        this.parameters = parameters;
    }

    public LogEntry(String uid, Integer index, Integer term, String command, Object parameters) {
        this(uid, term, command, parameters);
        this.index = index;
    }

    public void setMajorityResponses(int numReplicas)
    {
        int num = (numReplicas / 2);
        responses = new AtomicInteger(num);
        logger.debug("LogEntry " + uid + " waiting for " + num + " responses from " + numReplicas + " replicas...");
    }

    public void setUid(String uid)
    {
        this.uid = uid;
    }

    public String getUid() {
        return uid;
    }

    public Integer getIndex()
    {
        return index;
    }

    public void setIndex(Integer index)
    {
        this.index = index;
    }

    public String getCommand() {
        return this.command;
    }

    public void setCommand(String command)
    {
        this.command = command;
    }

    public Integer getTerm() {
        return this.term;
    }

    public void setTerm(Integer term)
    {
        this.term = term;
    }

    public Object getParameters()
    {
        return parameters;
    }

    public void setParameters(Object parameters)
    {
        this.parameters = parameters;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }
    
    @Override
    public String toString() {
        return "entry - uid: " + uid + "; index: " + index + "; term: " + term + "; command: " + command + "; parameters: " + parameters;
    }

    public String toShortString() {
        return "entry - uid: " + uid + "; command: " + command + "; parameters: " + parameters;
    }

    public void addLeaderResponse()
    {
        logger.debug(System.currentTimeMillis() + "Adding Leader Response for LogEntry with index " + getIndex());
        addReplicaResponse();
    }

    public synchronized void addReplicaResponse()
    {
        logger.debug(System.currentTimeMillis() + "Adding Replica Response for LogEntry with index " + getIndex());
        if(!majorityReceived)
        {
            int missingResponses = responses.decrementAndGet();

            
            if(0 == missingResponses)
            {
                logger.debug("Majority has been reached for LogEntry with index " + getIndex());
                unlock();
            }
        }
    }

    public void unlock() {
        logger.debug("LogEntry with index " + getIndex() + " unlocking at " + System.currentTimeMillis());
        lock.lock();
        try {
            majorityReceived = true;
            majority.signal();
        } finally {
            lock.unlock();
        }

        logger.debug("LogEntry with index " + getIndex() + " unlocked!");
    }

    public Object getResult()
    {
        logger.debug("LogEntry getResult started.");
        
        if(!majorityReceived)
        {
            // block on mutex while condition not satisfied
            lock.lock();

            long init = 0;
            if(logger.isDebugEnabled())
            {
                logger.debug("LogEntry with index " + getIndex() + " blocked waiting for majority of responses...");
                init = System.currentTimeMillis();
            }
            
            try {
                while (!majorityReceived) {
                    majority.await();
                }

            } catch (InterruptedException ex) {
                logger.error(ex.getMessage(), ex);
            } finally {
                lock.unlock();
            }

            if(logger.isDebugEnabled())
            {
                long end = System.currentTimeMillis();
                logger.debug("LogEntry with index " + getIndex() + " waited for " + (end - init) + " ms");
            }

            try {
                // acquire semaphore
                propagated.acquire();
                logger.debug("Acquired semaphore.");
            } catch (InterruptedException ex) {
                logger.error(ex.getMessage(), ex);
            }

            propagated.release();
            logger.debug("Released semaphore.");
        }

        logger.debug("LogEntry getResult ended.");
        
        return "OK";
    }
}
