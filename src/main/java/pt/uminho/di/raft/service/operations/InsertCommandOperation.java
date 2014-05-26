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
public class InsertCommandOperation extends RaftOperation {

    static final Logger logger = Logger.getLogger(InsertCommandOperation.class);

    public InsertCommandOperation(Server s)
    {
        super(s, Constants.InsertCommandOperationName, Constants.RaftServiceQName);
    }

    @Override
    public void initInput()
    {
        ComplexType commandType = new ComplexType(Constants.CommandTypeQName, ComplexType.CONTAINER_SEQUENCE);
        Element uidElem = new Element(Constants.UidElementQName, SchemaUtil.TYPE_ANYURI);
        commandType.addElement(uidElem);
        Element commandElem = new Element(Constants.CommandElementQName, SchemaUtil.TYPE_STRING);
        commandType.addElement(commandElem);
        Element parametersElem = new Element(Constants.ParametersElementQName, SchemaUtil.TYPE_STRING);
        commandType.addElement(parametersElem);

        Element request = new Element(Constants.InsertCommandRequestElementQName, commandType);
        setInput(request);
    }

    @Override
    public void initOutput()
    {
        Element successElem = new Element(Constants.SuccessElementQName, SchemaUtil.TYPE_BOOLEAN);
        Element resultElem = new Element(Constants.ResultElementQName, SchemaUtil.TYPE_STRING);
        resultElem.setMinOccurs(0);
        Element leaderElem = new Element(Constants.LeaderAddressElementQName, SchemaUtil.TYPE_ANYURI);
        leaderElem.setMinOccurs(0);

        ComplexType responseType = new ComplexType(Constants.InsertCommandResponseTypeQName, ComplexType.CONTAINER_SEQUENCE);
        responseType.addElement(successElem);
        responseType.addElement(resultElem);
        responseType.addElement(leaderElem);

        Element response = new Element(Constants.InsertCommandResponseElementQName, responseType);
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
                String uid = ParameterValueManagement.getString(pv, Constants.UidElementName);
                String command = ParameterValueManagement.getString(pv, Constants.CommandElementName);
                String parameters = ParameterValueManagement.getString(pv, Constants.ParametersElementName);

                logger.debug(server.getIdString() + "Going to invoke insertCommand: uid=" + uid + "; command=" + command + "; parameters=" + parameters);


                // Checks if entry already exists and if new, inserts entry in leader's log
                LogEntry entry = server.insertCommand(uid, command, parameters);

                // must wait until receiving a majority of success answers from replicas to reply back to the client.
                // The resulting LogEntry will then be committed and the commitIndex incremented.
                // In the event, that the client retries to insert the same command, the result will be returned immediately,
                //  as the entry has been already sent and executed by the replicas.
                responseResult = entry.getResult();
                if(responseResult != null)
                {
                    responseSuccess = true;

                    // leader commits command in StateMachine and increments commitIndex after receiving majority of responses from replicas
                    server.commitLogEntry(entry);
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

    public ParameterValue getRequestPV(String uid, String command, String parameters)
    {
        ParameterValue pv = createInputValue();

        ParameterValueManagement.setString(pv, Constants.UidElementName, uid);
        ParameterValueManagement.setString(pv, Constants.CommandElementName, command);
        ParameterValueManagement.setString(pv, Constants.ParametersElementName, parameters);

        return pv;
    }

    
}
