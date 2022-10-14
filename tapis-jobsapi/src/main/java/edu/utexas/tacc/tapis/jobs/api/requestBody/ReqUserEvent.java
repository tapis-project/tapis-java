package edu.utexas.tacc.tapis.jobs.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import io.swagger.v3.oas.annotations.media.Schema;

public class ReqUserEvent 
 implements IReqBody
{
    // Fields.
    private String eventDetail;

	@Override
	public String validate()
	{
	    // Make sure some event information is provided.
	    if (StringUtils.isBlank(eventDetail)) 
	        return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "eventDetail");
	    
		// Success.
		return null; 
	}
	
	@Schema(required = true)
    public String getEventDetail() {
        return eventDetail;
    }

    public void setEventDetail(String eventDetail) {
        this.eventDetail = eventDetail;
    }
}
