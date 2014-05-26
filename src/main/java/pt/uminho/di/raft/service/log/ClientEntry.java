/*******************************************************************************
 * Copyright (c) 2014 Filipe Campos.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package pt.uminho.di.raft.service.log;

import org.apache.log4j.Logger;

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
