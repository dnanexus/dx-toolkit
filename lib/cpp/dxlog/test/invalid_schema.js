{
  "api": {
    "facility": 8,
    "required": ["userId", "msg"],
    "text": {
      "tag": "DNAnexusAPI"
    },
    "mongodb": {
      "columns": {
	 "timestamp": "int64",
	 "hostname": "string",
	 "level": "int",
	 "userId": "string",
	 "msg": "string"
      },
      "indexes": [{"timestamp": 1}, {"hostname": 1}, {"level": 1}, {"userId": 1}]
    }
  },

  "app": {
    "facility": 8,
    "required": ["jobId", "msg"],
    "text": {
      "format": "{jobId} [msg] {msg}",
      "tag": "DNAnexusAPP",
      "maxMsgSize": 2000
    },
    "mongodb": {
      "maxMsgSize": 2000,
      "columns": {
	 "timestamp": "int64",
	 "hostname": "string",
	 "level": "int",
	 "userId": "string",
	 "jobId": "string",
	 "projectId": "string",
	 "programId": "string",
	 "msg": "string"},
      "indexes": [{"timestamp": 1}, {"hostname": 1}, {"level": 1}, {"userId": 1}, {"jobId": 1}, {"projectId": 1}, {"programId": 1}]
    }
  },

  "cloudManager": {
    "facility": 8,
    "required": ["msg"],
    "text":{
      "maxMsgSize": 50000,
      "format": "[msg] {msg}",
      "tag": "DNAnexusCM"},
    "mongodb": {
      "columns": {
	 "timestamp": "int64",
	 "hostname": "string",
	 "level": "int",
	 "msg": "string"},
      "indexes": [{"timestamp": 1}, {"hostname": 1}, {"level": 1}]
    }
  }
}
