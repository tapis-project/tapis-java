package edu.utexas.tacc.tapis.shared.exceptions;

public class TapisClientException 
 extends TapisException
{
    private static final long serialVersionUID = 2070468207317431854L;
    
    // Fields.
    private int    code;
    private String status;
    private String tapisMessage;
    private String version;
    private Object result;

    // Constructors.
    public TapisClientException(String message) {super(message);}
    public TapisClientException(String message, Throwable cause) {super(message, cause);}
    
    // Accessors.
    public int getCode() {
        return code;
    }
    public void setCode(int code) {
        this.code = code;
    }
    public String getStatus() {
        return status;
    }
    public void setStatus(String status) {
        this.status = status;
    }
    public String getTapisMessage() {
        return tapisMessage;
    }
    public void setTapisMessage(String tapisMessage) {
        this.tapisMessage = tapisMessage;
    }
    public String getVersion() {
        return version;
    }
    public void setVersion(String version) {
        this.version = version;
    }
    public Object getResult() {
        return result;
    }
    public void setResult(Object result) {
        this.result = result;
    }
}
