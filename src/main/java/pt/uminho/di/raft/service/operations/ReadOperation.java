/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package pt.uminho.di.raft.service.operations;

import org.apache.log4j.Logger;
import org.ws4d.java.communication.CommunicationException;
import org.ws4d.java.schema.ComplexType;
import org.ws4d.java.schema.Element;
import org.ws4d.java.schema.SchemaUtil;
import org.ws4d.java.security.CredentialInfo;
import org.ws4d.java.service.InvocationException;
import org.ws4d.java.service.parameter.ParameterValue;
import org.ws4d.java.service.parameter.ParameterValueManagement;
import org.ws4d.java.types.URI;
import pt.uminho.di.raft.Constants;
import pt.uminho.di.raft.entities.Server;
import pt.uminho.di.raft.service.log.LogEntry;

/**
 *
 * @author fcampos
 *
 * •	If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
 *
 * Arguments
 * -command - command to execute
 * -parameters - parameters for the command
 *
 * Results:
 * -success - true if command was applied correctly to log and state machine
 */
public class ReadOperation extends RaftOperation {

    static final Logger logger = Logger.getLogger(ReadOperation.class);

    public ReadOperation(Server s)
    {
        super(s, Constants.ReadOperationName, Constants.RaftServiceQName);
    }

    @Override
    public void initInput()
    {
        ComplexType commandType = new ComplexType(Constants.ReadRequestTypeQName, ComplexType.CONTAINER_SEQUENCE);
        Element uidElem = new Element(Constants.UidElementQName, SchemaUtil.TYPE_ANYURI);
        commandType.addElement(uidElem);
        uidElem.setMinOccurs(0);
        
        Element request = new Element(Constants.ReadRequestElementQName, commandType);
        setInput(request);
    }

    @Override
    public void initOutput()
    {
        Element entryUidElem = new Element(Constants.UidElementQName, SchemaUtil.TYPE_ANYURI);
        entryUidElem.setMinOccurs(0);
        Element entryIndexElem = new Element(Constants.IndexElementQName, SchemaUtil.TYPE_INTEGER);
        entryIndexElem.setMinOccurs(0);
        Element entryTermElem = new Element(Constants.TermElementQName, SchemaUtil.TYPE_INTEGER);
        entryTermElem.setMinOccurs(0);
        Element entryCommandElem = new Element(Constants.CommandElementQName, SchemaUtil.TYPE_STRING);
        entryCommandElem.setMinOccurs(0);
        Element entryParametersElem = new Element(Constants.ParametersElementQName, SchemaUtil.TYPE_STRING);
        entryParametersElem.setMinOccurs(0);
        Element successElem = new Element(Constants.SuccessElementQName, SchemaUtil.TYPE_BOOLEAN);
        Element resultElem = new Element(Constants.ResultElementQName, SchemaUtil.TYPE_STRING);
        resultElem.setMinOccurs(0);
        Element leaderElem = new Element(Constants.LeaderAddressElementQName, SchemaUtil.TYPE_ANYURI);
        leaderElem.setMinOccurs(0);

        ComplexType responseType = new ComplexType(Constants.ReadResponseTypeQName, ComplexType.CONTAINER_SEQUENCE);
        responseType.addElement(entryUidElem);
        responseType.addElement(entryIndexElem);
        responseType.addElement(entryTermElem);
        responseType.addElement(entryCommandElem);
        responseType.addElement(entryParametersElem);
        responseType.addElement(successElem);
        responseType.addElement(resultElem);
        responseType.addElement(leaderElem);

        Element response = new Element(Constants.ReadResponseElementQName, responseType);
        setOutput(response);
    }

    @Override
    protected ParameterValue invokeImpl(ParameterValue pv, CredentialInfo ci) throws InvocationException, CommunicationException
    {
        ParameterValue response = createOutputValue();
        boolean responseSuccess = false;

        if(server.isLeader())
        {
            Object responseResult = "";
            try
            {
                // server is the current leader. so process invocation and reply

                // check if there is a uid specified in the request
                String uid = ParameterValueManagement.getString(pv, Constants.UidElementName);
                
                logger.debug(server.getIdString() + "Going to retrieve entry with uid=" + uid);

                LogEntry entry = null;
                if((uid != null) && (!uid.isEmpty()))
                {
                    // Checks if entry exists and retrieve it
                    entry = server.getLogEntry(uid);
                }
                else
                {
                    // Get last entry
                    entry = server.getLogEntry(server.getLastLogIndex());
                }

                if(entry != null)
                {
                    responseResult = entry.getResult();
                    responseSuccess = true;
                    ParameterValueManagement.setString(response, Constants.UidElementName, entry.getUid());
                    ParameterValueManagement.setString(response, Constants.IndexElementName, entry.getIndex().toString());
                    ParameterValueManagement.setString(response, Constants.TermElementName, entry.getTerm().toString());
                    ParameterValueManagement.setString(response, Constants.CommandElementName, entry.getCommand());
                    ParameterValueManagement.setString(response, Constants.ParametersElementName, entry.getParameters().toString());
                }
                else
                {
                    // no entry
                    responseResult = "Didn't get any entry!";
                }
            }
            catch(Exception ex)
            {
                logger.error(ex.getMessage(), ex);
                responseSuccess = false;
                responseResult = ex.getMessage();
            }
            
            ParameterValueManagement.setString(response, Constants.ResultElementName, responseResult.toString());
        }
        else
        {
            // server is not the leader. so point client to current leader
            URI leaderAddress = server.getLeaderAddress();
            String address = "unknown";
            if(leaderAddress != null)
                address = leaderAddress.toString();
            ParameterValueManagement.setString(response, Constants.LeaderAddressElementName, address); // URI for Raft Service
        }
        ParameterValueManagement.setString(response, Constants.SuccessElementName, "" + responseSuccess);

        return response;
    }

    public ParameterValue getRequestPV(String uid)
    {
        ParameterValue pv = createInputValue();

        if((uid != null) && (!uid.isEmpty()))
            ParameterValueManagement.setString(pv, Constants.UidElementName, uid);

        return pv;
    }

    
}
