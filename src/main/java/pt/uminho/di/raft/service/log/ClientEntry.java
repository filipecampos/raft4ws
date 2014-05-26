package pt.uminho.di.raft.service.log;

import org.apache.log4j.Logger;

/**
 *
 * @author fcampos
 */
public class ClientEntry {

    static Logger logger = Logger.getLogger(ClientEntry.class);

    private String uid;
    private String command;
    private Object parameters;
    private String response;
    private Boolean success;

    public ClientEntry(String uid, String command, Object parameters) {
        this.uid = uid;
        this.command = command;
        this.parameters = parameters;
        this.success = false;
    }

    public void setUid(String uid)
    {
        this.uid = uid;
    }

    public String getUid() {
        return uid;
    }

    public String getCommand() {
        return this.command;
    }

    public void setCommand(String command)
    {
        this.command = command;
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
        return "entry - UID: " + uid + "; command: " + command + "; parameters: " + parameters + "; success: " + success + "; response: " + response;
    }

}
