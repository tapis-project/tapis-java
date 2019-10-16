package edu.utexas.tacc.tapis.sharedapi.responses;

public final class RespBasic 
 extends RespAbstract
{
    public RespBasic() {}
    public RespBasic(Object result) {this.result = result;}
    
    public Object result;
}
