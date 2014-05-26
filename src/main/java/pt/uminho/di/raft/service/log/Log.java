/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package pt.uminho.di.raft.service.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.ws4d.java.CoreFramework;
import pt.uminho.di.raft.entities.workers.CommittingTask;

/**
 *
 * @author fcampos
 */
public class Log {

    static Logger logger = Logger.getLogger(Log.class);

    // first index is 1
    private ArrayList<LogEntry> logEntries;

    // index of last entry committed to StateMachine
    private Integer commitIndex;
    // index of last entry applied to StateMachine
    private Integer lastApplied;

    private StateMachine sm;
    private HashMap<String, LogEntry> entriesMap;
    private String idString;

    public Log(String id)
    {
        logEntries = new ArrayList<LogEntry>(10000);
        entriesMap = new HashMap<String, LogEntry>(10000);
        sm = new BDBStateMachine();
//        sm = new HSQLStateMachine();
        // TODO: uncomment
        sm.initialize(id);

        idString = "[Server " + id + "] - ";

        commitIndex = 0;
        lastApplied = 0;
    }

    public Integer getEntryTerm(int index)
    {
        Integer reply = -1;

        logger.debug(idString + "index=" + index + "; entries size=" + logEntries.size());
        if(logEntries.size() > index)
            reply = logEntries.get(index).getTerm();
    
        return reply;
    }

    public Integer getLastLogIndex()
    {
        return logEntries.size();
    }

    public Integer getLastLogTerm()
    {
        Integer reply = -1;
        int size = logEntries.size();
        if(size > 0)
        {
            LogEntry last = logEntries.get(size-1);

            if(last != null)
            {
                logger.debug(idString + "Got last entry! Index:" + last.getIndex() + "; Term:" + last.getTerm()
                        + "; Command:" + last.getCommand() + "; Parameters:" + last.getParameters());
                reply = last.getTerm();
            }
                
        }
        
        return reply;
    }

    public LogEntry getEntry(int nextIndex)
    {
        LogEntry entry = null;
        try
        {
            entry = logEntries.get(nextIndex-1);
        }
        catch(ArrayIndexOutOfBoundsException ex)
        {
            logger.error(idString + ex.getMessage(), ex);
        }
        return entry;
    }

    public Integer getCommitIndex()
    {
        return commitIndex;
    }

    public Integer getLastApplied()
    {
        return lastApplied;
    }
    
    public void append(List<LogEntry> entries)
    {
        // 3.	If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it (§5.3)
        // 4.	Append any new entries not already in the log
        Iterator<LogEntry> iter = entries.iterator();

        while(iter.hasNext())
        {
            LogEntry newEntry = iter.next();

            Integer index = newEntry.getIndex();

            synchronized(this)
            {
                int entriesSize = logEntries.size();
                if(index > entriesSize)
                {
                    // 4.	Append any new entries not already in the log
                    insertLogEntry(newEntry);
                }
                else
                {
                    // 3.	If an existing entry conflicts with a new one (same index but different terms),
                    // delete the existing entry and all that follow it (§5.3)
                    LogEntry existingEntry = logEntries.get(index-1);
                    // same index
                    Integer existingTerm = existingEntry.getTerm();
                    Integer newTerm = newEntry.getTerm();
                    if(existingTerm != newTerm)
                    {
                        // delete the existing entry and all that follow it
                        for(int lastIndex = entriesSize; lastIndex >= index; lastIndex--)
                        {
                            logEntries.remove(lastIndex-1);
                        }
                        // add the new entry
                        insertLogEntry(newEntry);

                        // the others will be inserted in the following cycles
                    }
                }
            }
            
        }
        
    }

    public void updateCommitIndex(Integer leaderCommit)
    {
        // * 5.	If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, last log index)
        if(leaderCommit > commitIndex)
        {
            commitIndex = leaderCommit;

            int lastLogIndex = getLastLogIndex();
            if(commitIndex > lastLogIndex)
                commitIndex = lastLogIndex;
        }

        // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
        if (commitIndex > lastApplied) {
            CommittingTask task = new CommittingTask(this, idString);
            CoreFramework.getThreadPool().execute(task);
        }
    }

    public void checkCommittedEntries()
    {
        logger.debug(idString + "Committing entries to synch with leader... commitIndex=" + commitIndex + "; lastApplied=" + lastApplied);
        // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
        List<LogEntry> entriesToCommit = getEntries(lastApplied + 1, commitIndex + 1);
        if(entriesToCommit != null)
        {
            for (LogEntry uncommitedEntry : entriesToCommit) {
                if(commitLogEntry(uncommitedEntry))
                    lastApplied++;
            }
        }
    }

    public LogEntry getEntry(String uid)
    {
        logger.debug(idString + "Getting entry with UID: " + uid);
        return entriesMap.get(uid);
    }

    public synchronized LogEntry insertNewCommand(String uid, Integer term, String command, String parameters)
    {
        LogEntry newEntry = new LogEntry(uid, getLastLogIndex()+1, term, command, parameters);
        boolean success = insertLogEntry(newEntry);

        if(!success)
        {
            logger.debug(idString + "New entry " + uid + " not created.");
            newEntry = null;
        }
        else
            logger.debug(idString + "New entry " + uid + " created correctly.");
            

        return newEntry;
    }

    private boolean insertLogEntry(LogEntry entry)
    {
        boolean success = true;

        try
        {
            logger.debug(idString + "Inserting " + entry + " in Log at position " + entry.getIndex());
            logEntries.add(entry.getIndex()-1, entry);
            entriesMap.put(entry.getUid(), entry);
            logger.debug(idString + "Correctly inserted Log entry " + entry);
        }
        catch(Exception ex)
        {
            logger.error(idString + ex.getMessage(), ex);
            success = false;
        }

        return success;
    }
    
    public boolean commitLogEntry(LogEntry entry)
    {
        boolean success = true;

            try
            {
                // committing new entry
                logger.debug(idString + "Commiting " + entry + " in StateMachine...");
                success = sm.insertIntoDB(entry);

                if(success)
                {
                    logger.debug(idString + "Correctly commited Log entry " + entry);
                    entriesMap.put(entry.getUid(), entry);
                }
            }
            catch(Exception ex)
            {
                logger.error(idString + ex.getMessage(), ex);
                success = false;
            }
        
        return success;
    }

    public List<LogEntry> getNewEntries(Integer start)
    {
        return getEntries(start, commitIndex);
    }

    public synchronized List<LogEntry> getEntries(Integer start, Integer finish)
    {
        List<LogEntry> retList = null;

        if(finish > start)
            finish--;
        
        start--;


        if((start > -1) && (finish > -1)  && (start < logEntries.size()) && (start < finish))
        {
            logger.debug(idString + "Getting LogEntries from " + start + " to " + finish);
            retList = logEntries.subList(start, finish);
        }

        if((retList != null) && (!retList.isEmpty()))
            retList = new ArrayList<LogEntry>(retList);
            
        
        return retList;
    }

    public void incrementCommitIndex() {
        commitIndex++;
    }
}
