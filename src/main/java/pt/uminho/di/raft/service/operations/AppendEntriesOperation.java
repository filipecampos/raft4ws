/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pt.uminho.di.raft.service.operations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.ws4d.java.communication.CommunicationException;
import org.ws4d.java.schema.ComplexType;
import org.ws4d.java.schema.Element;
import org.ws4d.java.schema.SchemaUtil;
import org.ws4d.java.security.CredentialInfo;
import org.ws4d.java.service.InvocationException;
import org.ws4d.java.service.parameter.ParameterValue;
import org.ws4d.java.service.parameter.ParameterValueManagement;
import org.ws4d.java.structures.ListIterator;
import org.ws4d.java.types.URI;
import pt.uminho.di.raft.Constants;
import pt.uminho.di.raft.entities.Server;
import pt.uminho.di.raft.service.log.LogEntry;

/**
 *
 * @author fcampos
 * Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
 *
 * 1.	Reply false if term < currentTerm (§5.1)
 * 2.	Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
 * 3.	If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
 * 4.	Append any new entries not already in the log
 * 5.	If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, last log index)
 *
 * Arguments
 * -term - leader's term
 * -leaderId - so follower can redirect clients
 * -prevLogIndex - index of log entry immediately preceding new ones
 * -prevLogTerm - term of prevLogIndex entry
 * -entries[] - log entries to store (empty for heartbeat; may send more than one for efficiency)
 * -leaderCommit - leader’s commitIndex
 *
 * Results:
 * -term - currentTerm, for leader to update itself
 * -success - true if follower contained entry matching prevLogIndex and prevLogTerm
 */
public class AppendEntriesOperation extends RaftOperation {

    static final Logger logger = Logger.getLogger(AppendEntriesOperation.class);

    public AppendEntriesOperation(Server s) {
        super(s, Constants.AppendEntriesOperationName, Constants.RaftServiceQName);
    }

    @Override
    public void initInput() {
        Element termElem = new Element(Constants.TermElementQName, SchemaUtil.TYPE_INTEGER);
        Element leaderIdElem = new Element(Constants.LeaderIdElementQName, SchemaUtil.TYPE_ANYURI);
        Element prevLogIndexElem = new Element(Constants.PrevLogIndexElementQName, SchemaUtil.TYPE_INTEGER);
        Element prevLogTermElem = new Element(Constants.PrevLogTermElementQName, SchemaUtil.TYPE_INTEGER);

        ComplexType entryType = new ComplexType(Constants.EntryTypeQName, ComplexType.CONTAINER_SEQUENCE);
        Element entryUidElem = new Element(Constants.UidElementQName, SchemaUtil.TYPE_ANYURI);
        entryType.addElement(entryUidElem);
        Element entryIndexElem = new Element(Constants.IndexElementQName, SchemaUtil.TYPE_INTEGER);
        entryType.addElement(entryIndexElem);
        Element entryTermElem = new Element(Constants.TermElementQName, SchemaUtil.TYPE_INTEGER);
        entryType.addElement(entryTermElem);
        Element entryCommandElem = new Element(Constants.CommandElementQName, SchemaUtil.TYPE_STRING);
        entryType.addElement(entryCommandElem);
        Element entryParametersElem = new Element(Constants.ParametersElementQName, SchemaUtil.TYPE_STRING);
        entryType.addElement(entryParametersElem);
        Element entryElem = new Element(Constants.EntryElementQName, entryType);

        ComplexType entriesType = new ComplexType(Constants.EntriesTypeQName, ComplexType.CONTAINER_SEQUENCE);
        entryElem.setMinOccurs(0);
        entryElem.setMaxOccurs(-1);
        entriesType.addElement(entryElem);
        Element entriesElem = new Element(Constants.EntriesElementQName, entriesType);

        Element leaderCommitElem = new Element(Constants.LeaderCommitElementQName, SchemaUtil.TYPE_INTEGER);

        ComplexType requestType = new ComplexType(Constants.AppendEntriesRequestTypeQName, ComplexType.CONTAINER_SEQUENCE);
        requestType.addElement(termElem);
        requestType.addElement(leaderIdElem);
        requestType.addElement(prevLogIndexElem);
        requestType.addElement(prevLogTermElem);
        requestType.addElement(entriesElem);
        requestType.addElement(leaderCommitElem);

        Element request = new Element(Constants.AppendEntriesRequestElementQName, requestType);
        setInput(request);
    }

    @Override
    public void initOutput() {
        Element termElem = new Element(Constants.TermElementQName, SchemaUtil.TYPE_INTEGER);
        Element successElem = new Element(Constants.SuccessElementQName, SchemaUtil.TYPE_BOOLEAN);

        ComplexType responseType = new ComplexType(Constants.AppendEntriesResponseTypeQName, ComplexType.CONTAINER_SEQUENCE);
        responseType.addElement(termElem);
        responseType.addElement(successElem);

        Element response = new Element(Constants.AppendEntriesResponseElementQName, responseType);
        setOutput(response);
    }

    @Override
    protected ParameterValue invokeImpl(ParameterValue pv, CredentialInfo ci) throws InvocationException, CommunicationException {
        logger.debug(server.getIdString() + "Received pv=" + pv + " at " + System.currentTimeMillis());
        
        boolean responseSuccess = true;

        String termStr = ParameterValueManagement.getString(pv, Constants.TermElementName);
        Integer term = Integer.parseInt(termStr);

        Integer currentTerm = server.getCurrentTerm();

        // 1. Reply false if term < currentTerm (§5.1)
        if (term < currentTerm) {
            responseSuccess = false;
        } else {
            // reset election timeout timer
            String leaderIdStr = ParameterValueManagement.getString(pv, Constants.LeaderIdElementName);
            URI leaderId = new URI(leaderIdStr);
            server.contactedBy(leaderId, true, false);
            
            if (server.updateTerm(term)) {
                // retrieve updated term
                currentTerm = server.getCurrentTerm();
            }

            // 2.	Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
            String prevLogIndexStr = ParameterValueManagement.getString(pv, Constants.PrevLogIndexElementName);
            Integer prevLogIndex = Integer.parseInt(prevLogIndexStr);
            String prevLogTermStr = ParameterValueManagement.getString(pv, Constants.PrevLogTermElementName);
            Integer prevLogTerm = Integer.parseInt(prevLogTermStr);
            LogEntry entry = null;
            if ((prevLogIndex > 0) && (prevLogTerm > 0)) {
                if (prevLogIndex <= server.getLastLogIndex()) {
                    entry = server.getLogEntry(prevLogIndex);
                } else {
                    logger.warn(server.getIdString() + "Log has " + server.getLastLogIndex() + " entries. prevLogIndex=" + prevLogIndexStr);
                }
            }

            logger.debug(server.getIdString() + "prevLogIndex=" + prevLogIndexStr + "; serverIndex=" + server.getLastLogIndex()
                    + "; prevLogTerm=" + prevLogTermStr + "; serverTerm=" + server.getLastLogTerm());

            if (((prevLogIndex > 0) && (entry == null)) || ((entry != null) && (entry.getTerm() != prevLogTerm))) {
                responseSuccess = false;
            } else {
                try
                {
                    List entries = AppendEntriesOperation.extractEntries(pv);

                    if ((entries != null) && (!entries.isEmpty())) {
                        // 3.	If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
                        // 4.	Append any new entries not already in the log
                        // invocation has entries
                        logger.debug(server.getIdString() + "Appending entries " + entries);
                        server.appendEntries(entries);
                    }
                    else {
                        logger.debug(server.getIdString() + "No entries to append! Processing heartbeat...");
                    }

                    // 5.	If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, last log index)
                    String leaderCommitStr = ParameterValueManagement.getString(pv, Constants.LeaderCommitElementName);
                    Integer leaderCommit = Integer.parseInt(leaderCommitStr);
                    server.updateCommitIndex(leaderCommit);
                }
                catch(Exception ex)
                {
                    logger.error(ex.getMessage(), ex);
                    responseSuccess = false;
                }
            }
        }

        Integer responseTerm = currentTerm;

        ParameterValue response = createOutputValue();
        ParameterValueManagement.setString(response, Constants.TermElementName, responseTerm.toString());
        ParameterValueManagement.setString(response, Constants.SuccessElementName, "" + responseSuccess);
        logger.debug(server.getIdString() + "Responding with term=" + responseTerm.toString() + "; success=" + responseSuccess);

        return response;
    }

    public static List<LogEntry> extractEntries(ParameterValue pv) {
        List<LogEntry> logEntriesList = null;
        org.ws4d.java.structures.List entries = pv.getChildren(Constants.EntriesElementName);
        int numEntriesElements = entries.size();
        if (numEntriesElements > 0) {
            ParameterValue entriesPV = (ParameterValue) entries.get(0);
            logger.debug("children [entries] = " + entriesPV + " numEntries=" + numEntriesElements);
            ListIterator entriesIterator = entriesPV.getChildrenList();
            int numEntries = entriesPV.getChildrenCount();
            logEntriesList = new ArrayList<LogEntry>(numEntries);

            while (entriesIterator.hasNext()) {
                ParameterValue entryPV = (ParameterValue) entriesIterator.next();
                String uidStr = ParameterValueManagement.getString(entryPV, Constants.UidElementName);
                if(uidStr == null)
                {
                    logger.warn("Got entry with null uid!");
                    uidStr = "null";
                }
                String indexStr = ParameterValueManagement.getString(entryPV, Constants.IndexElementName);
                Integer index = Integer.parseInt(indexStr);
                String termStr = ParameterValueManagement.getString(entryPV, Constants.TermElementName);
                Integer term = Integer.parseInt(termStr);
                String commandStr = ParameterValueManagement.getString(entryPV, Constants.CommandElementName);
                String parametersStr = ParameterValueManagement.getString(entryPV, Constants.ParametersElementName);

                LogEntry newEntry = new LogEntry(uidStr, index, term, commandStr, parametersStr);
                logger.debug("Created entry: " + newEntry);
                logEntriesList.add(newEntry);
            }
        }

        return logEntriesList;
    }

    /**
     * Synchronized method due to concurrent access that affects ParameterValue creation by duplicating elements.
     * @param term
     * @param leaderId
     * @param prevLogIndex
     * @param prevLogTerm
     * @param entries
     * @param leaderCommit
     * @return
     */
    public synchronized ParameterValue getRequestPV(Integer term, URI leaderId, Integer prevLogIndex, Integer prevLogTerm, List<LogEntry> entries, Integer leaderCommit) {
        ParameterValue reply = createInputValue();

        ParameterValueManagement.setString(reply, Constants.TermElementName, term.toString());
        ParameterValueManagement.setString(reply, Constants.LeaderIdElementName, leaderId.toString());
        ParameterValueManagement.setString(reply, Constants.PrevLogIndexElementName, prevLogIndex.toString());
        ParameterValueManagement.setString(reply, Constants.PrevLogTermElementName, prevLogTerm.toString());
        if ((entries != null) && (!entries.isEmpty())) {
            ParameterValue entriesElement = reply.createChild(Constants.EntriesElementName);
            // not heartbeat
            Iterator<LogEntry> iter = entries.iterator();
            while (iter.hasNext()) {
                LogEntry entry = iter.next();
                ParameterValue child = entriesElement.createChild(Constants.EntryElementName);
                ParameterValueManagement.setString(child, Constants.UidElementName, entry.getUid().toString());
                ParameterValueManagement.setString(child, Constants.IndexElementName, entry.getIndex().toString());
                ParameterValueManagement.setString(child, Constants.TermElementName, entry.getTerm().toString());
                ParameterValueManagement.setString(child, Constants.CommandElementName, entry.getCommand());
                ParameterValueManagement.setString(child, Constants.ParametersElementName, entry.getParameters().toString());
            }
        }
        ParameterValueManagement.setString(reply, Constants.LeaderCommitElementName, leaderCommit.toString());

        logger.debug(server.getIdString() + "Created Request PV: " + reply);

        return reply;
    }
}
