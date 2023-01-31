package edu.utexas.tacc.tapis.jobs.api.requestBody;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import io.swagger.v3.oas.annotations.media.Schema;

public class ReqUserEvent 
 implements IReqBody
{
    // Constants
    private static final String DEFAULT_EVENTDETAIL = "DEFAULT";
    private static final Pattern _validDetail = Pattern.compile("[a-zA-Z0-9_]+"); // alphanumerics + _
    
    // Fields.
    private String eventData;
    private String eventDetail = DEFAULT_EVENTDETAIL;

	@Override
	public String validate()
	{
	    // Make sure some event information is provided.
	    if (StringUtils.isBlank(eventData)) 
	        return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "eventData");
	    
	    // Assign detail if necessary.
	    if (StringUtils.isBlank(eventDetail)) eventDetail = DEFAULT_EVENTDETAIL;
	    else if (!_validDetail.matcher(eventDetail).matches()) {
	        return MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validate", "eventDetail", 
	                               eventDetail + " (only alphanumerics and underscores allowed)");
	    }
	    
		// Success.
		return null; 
	}

    @Schema(required = true)
    public String getEventData() {
        return eventData;
    }

    public void setEventData(String eventData) {
        this.eventData = eventData;
    }
	
	@Schema(required = false)
    public String getEventDetail() {
        return eventDetail;
    }

    public void setEventDetail(String eventDetail) {
        this.eventDetail = eventDetail;
    }
}
