package edu.utexas.tacc.tapis.shared.uuid;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.exceptions.TapisUUIDException;

public enum UUIDType {
	PROFILE("001"),
	FILE("002"),
	INTERNALUSER("003"),
	TOKEN("004"),
	APP("005"),
	SYSTEM("006"),
	JOB("007"),
	TRANSFORM("008"),
	TRANSFER("009"),
	POSTIT("010"),
	NOTIFICATION("011"),
	METADATA("012"),
	SCHEMA("013"),
	MONITOR("014"),
	MONITORCHECK("015"),
	TENANT("016"),
	ENCODING_TASK("017"),
	DECODING_TASK("018"),
	USAGE_TRIGGER("019"),
	FILE_STAGING_TASK("020"),
	JOB_STAGING_TASK("021"),
	PROFILE_EVENT("022"),
	FILE_EVENT("023"),
	INTERNALUSER_EVENT("024"),
	TOKEN_EVENT("025"),
	APP_EVENT("026"),
	SYSTEM_EVENT("027"),
	JOB_EVENT("028"),
	TRANSFORM_EVENT("029"),
	TRANSFER_EVENT("030"),
	POSTIT_EVENT("031"),
	NOTIFICATION_EVENT("032"),
	METADATA_EVENT("033"),
	SCHEMA_EVENT("034"),
	MONITOR_EVENT("035"),
	MONITORCHECK_EVENT("036"),
	TENANT_EVENT("037"),
	USAGETRIGGER("038"),
	USAGETRIGGER_EVENT("039"),
	WORKER("040"),
	BATCH_QUEUE("041"),
	NOTIFICATION_DELIVERY("042"),
    
  ROLE("043"),
  PERMISSION("044"),
  PERMISSION_EVENT("045"),
  USER_ROLE("046"),
  USER_PERMISSION("047"),
  TAG("048"),
  TAG_EVENT("049"),
  GROUP("050"),
  GROUP_EVENT("051"),
  BATCH_QUEUE_EVENT("052"),
  REACTOR("053"),
  REACTOR_EVENT("057"),
  REPOSITORY("058"),
  REPOSITORY_EVENT("059"),
  REALTIME_CHANNEL("054"),
	REALTIME_CHANNEL_EVENT("055"),
	CLIENTS("055"),
	CLIENTS_EVENT("056"),
	ABACO_AGENT("060"),
	ABACO_AGENT_EVENT("061"),
	SCHEDULED_TRANSFER("062"),
	SCHEDULED_TRANSFER_EVENT("063"),
	
	JOB_QUEUE("064"),
  JOB_WORKER("065");
	
	private String code;

	private UUIDType(String val) {
		this.code = val;
	}

	public String getCode() {
		return code;
	}

	public static UUIDType getInstance(String val) throws TapisUUIDException
	{
		// Fetch by enum pneumonic.
		try {
			UUIDType type = UUIDType.valueOf(val);
			return type;
		} catch (Exception e) {}

		// Fetch by numeric code.
		for (UUIDType type: UUIDType.values()) {
			if (StringUtils.equals(val, type.code)) {
				return type;
			}
		}

		throw new TapisUUIDException("Invalid resource type. Valid types are: " +
				StringUtils.join(UUIDType.values(), ", "));
	}
}
