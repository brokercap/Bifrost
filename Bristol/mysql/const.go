// Constants documentation:
// https://dev.mysql.com/doc/search/?q=binlog&d=&p=1
package mysql

const (
	MIN_PROTOCOL_VERSION = 10
	MAX_PACKET_SIZE      = 1<<24 - 1
	TIME_FORMAT          = "2006-01-02 15:04:05"
)

type ClientFlag uint32

const (
	CLIENT_LONG_PASSWORD ClientFlag = 1 << iota
	CLIENT_FOUND_ROWS
	CLIENT_LONG_FLAG
	CLIENT_CONNECT_WITH_DB
	CLIENT_NO_SCHEMA
	CLIENT_COMPRESS
	CLIENT_ODBC
	CLIENT_LOCAL_FILES
	CLIENT_IGNORE_SPACE
	CLIENT_PROTOCOL_41
	CLIENT_INTERACTIVE
	CLIENT_SSL
	CLIENT_IGNORE_SIGPIPE
	CLIENT_TRANSACTIONS
	CLIENT_RESERVED
	CLIENT_SECURE_CONN
	CLIENT_MULTI_STATEMENTS
	CLIENT_MULTI_RESULTS
	CLIENT_PS_MULTI_RESULTS
	CLIENT_PLUGIN_AUTH
	CLIENT_CONNECT_ATTRS
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
)

type commandType byte

const (
	COM_QUIT commandType = iota + 1
	COM_INIT_DB
	COM_QUERY
	COM_FIELD_LIST
	COM_CREATE_DB
	COM_DROP_DB
	COM_REFRESH
	COM_SHUTDOWN
	COM_STATISTICS
	COM_PROCESS_INFO
	COM_CONNECT
	COM_PROCESS_KILL
	COM_DEBUG
	COM_PING
	COM_TIME
	COM_DELAYED_INSERT
	COM_CHANGE_USER
	COM_BINLOG_DUMP
	COM_TABLE_DUMP
	COM_CONNECT_OUT
	COM_REGISTER_SLAVE
	COM_STMT_PREPARE
	COM_STMT_EXECUTE
	COM_STMT_SEND_LONG_DATA
	COM_STMT_CLOSE
	COM_STMT_RESET
	COM_SET_OPTION
	COM_STMT_FETCH
	COM_DAEMON
	COM_BINLOG_DUMP_GTID
	COM_END
)

type FieldType byte

const (
	FIELD_TYPE_DECIMAL    FieldType = iota //0
	FIELD_TYPE_TINY                        //1
	FIELD_TYPE_SHORT                       //2
	FIELD_TYPE_LONG                        //3
	FIELD_TYPE_FLOAT                       //4
	FIELD_TYPE_DOUBLE                      //5
	FIELD_TYPE_NULL                        //6
	FIELD_TYPE_TIMESTAMP                   //7
	FIELD_TYPE_LONGLONG                    //8
	FIELD_TYPE_INT24                       //9
	FIELD_TYPE_DATE                        //10
	FIELD_TYPE_TIME                        //11
	FIELD_TYPE_DATETIME                    //12
	FIELD_TYPE_YEAR                        //13
	FIELD_TYPE_NEWDATE                     //14
	FIELD_TYPE_VARCHAR                     //15
	FIELD_TYPE_BIT                         //16
	FIELD_TYPE_TIMESTAMP2                  //17
	FIELD_TYPE_DATETIME2                   //18
	FIELD_TYPE_TIME2                       //19
)

const (
	FIELD_TYPE_JSON        FieldType = iota + 0xf5 //245
	FIELD_TYPE_NEWDECIMAL                          //246
	FIELD_TYPE_ENUM                                //247
	FIELD_TYPE_SET                                 //248
	FIELD_TYPE_TINY_BLOB                           //249
	FIELD_TYPE_MEDIUM_BLOB                         //250
	FIELD_TYPE_LONG_BLOB                           //251
	FIELD_TYPE_BLOB                                //252
	FIELD_TYPE_VAR_STRING                          //253
	FIELD_TYPE_STRING                              //254
	FIELD_TYPE_GEOMETRY                            //255
)
const (
	FIELD_TYPE_CHAR     FieldType = 1
	FIELD_TYPE_INTERVAL FieldType = 247
)

type FieldFlag uint16

const (
	FLAG_NOT_NULL FieldFlag = 1 << iota
	FLAG_PRI_KEY
	FLAG_UNIQUE_KEY
	FLAG_MULTIPLE_KEY
	FLAG_BLOB
	FLAG_UNSIGNED
	FLAG_ZEROFILL
	FLAG_BINARY
	FLAG_ENUM
	FLAG_AUTO_INCREMENT
	FLAG_TIMESTAMP
	FLAG_SET
	FLAG_UNKNOWN_1
	FLAG_UNKNOWN_2
	FLAG_UNKNOWN_3
	FLAG_UNKNOWN_4
)

type EventType byte

const (
	UNKNOWN_EVENT             EventType = iota //0
	START_EVENT_V3                             //1
	QUERY_EVENT                                //2
	STOP_EVENT                                 //3
	ROTATE_EVENT                               //4
	INTVAR_EVENT                               //5
	LOAD_EVENT                                 //6
	SLAVE_EVENT                                //7
	CREATE_FILE_EVENT                          //8
	APPEND_BLOCK_EVENT                         //9
	EXEC_LOAD_EVENT                            //10
	DELETE_FILE_EVENT                          //11
	NEW_LOAD_EVENT                             //12
	RAND_EVENT                                 //13
	USER_VAR_EVENT                             //14
	FORMAT_DESCRIPTION_EVENT                   //15
	XID_EVENT                                  //16
	BEGIN_LOAD_QUERY_EVENT                     //17
	EXECUTE_LOAD_QUERY_EVENT                   //18
	TABLE_MAP_EVENT                            //19
	WRITE_ROWS_EVENTv0                         //20
	UPDATE_ROWS_EVENTv0                        //21
	DELETE_ROWS_EVENTv0                        //22
	WRITE_ROWS_EVENTv1                         //23
	UPDATE_ROWS_EVENTv1                        //24
	DELETE_ROWS_EVENTv1                        //25
	INCIDENT_EVENT                             //26
	HEARTBEAT_EVENT                            //27
	IGNORABLE_EVENT                            //28
	ROWS_QUERY_EVENT                           //29
	WRITE_ROWS_EVENTv2                         //30
	UPDATE_ROWS_EVENTv2                        //31
	DELETE_ROWS_EVENTv2                        //32
	GTID_EVENT                                 //33
	ANONYMOUS_GTID_EVENT                       //34
	PREVIOUS_GTIDS_EVENT                       //35
	TRANSACTION_CONTEXT_EVENT                  // 36
	VIEW_CHANGE_EVENT                          // 37
	XA_PREPARE_LOG_EVENT                       // 38
)

const (
	// MariaDB event starts from 160
	MARIADB_ANNOTATE_ROWS_EVENT EventType = 160 + iota
	MARIADB_BINLOG_CHECKPOINT_EVENT
	MARIADB_GTID_EVENT
	MARIADB_GTID_LIST_EVENT
)

const (
	BINLOG_MARIADB_FL_STANDALONE      = 1 << iota /*1  - FL_STANDALONE is set when there is no terminating COMMIT event*/
	BINLOG_MARIADB_FL_GROUP_COMMIT_ID             /*2  - FL_GROUP_COMMIT_ID is set when event group is part of a group commit on the master. Groups with same commit_id are part of the same group commit.*/
	BINLOG_MARIADB_FL_TRANSACTIONAL               /*4  - FL_TRANSACTIONAL is set for an event group that can be safely rolled back (no MyISAM, eg.).*/
	BINLOG_MARIADB_FL_ALLOW_PARALLEL              /*8  - FL_ALLOW_PARALLEL reflects the (negation of the) value of @@SESSION.skip_parallel_replication at the time of commit*/
	BINLOG_MARIADB_FL_WAITED                      /*16 = FL_WAITED is set if a row lock wait (or other wait) is detected during the execution of the transaction*/
	BINLOG_MARIADB_FL_DDL                         /*32 - FL_DDL is set for event group containing DDL*/
)

type eventFlag uint16

const (
	LOG_EVENT_BINLOG_IN_USE_F eventFlag = 1 << iota
	LOG_EVENT_FORCED_ROTATE_F
	LOG_EVENT_THREAD_SPECIFIC_F
	LOG_EVENT_SUPPRESS_USE_F
	LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F
	LOG_EVENT_ARTIFICIAL_F
	LOG_EVENT_RELAY_LOG_F
	LOG_EVENT_IGNORABLE_F
	LOG_EVENT_NO_FILTER_F
	LOG_EVENT_MTS_ISOLATE_F
)

type StatusFlag int8

const (
	STATUS_CLOSED   StatusFlag = 0
	STATUS_CLOSING  StatusFlag = 1
	STATUS_STARTING StatusFlag = 10
	STATUS_RUNNING  StatusFlag = 11
	STATUS_STOPING  StatusFlag = 12
	STATUS_STOPED   StatusFlag = 13
	STATUS_KILLED   StatusFlag = 3
)

const (
	AUTH_MYSQL_OLD_PASSWORD    = "mysql_old_password"
	AUTH_NATIVE_PASSWORD       = "mysql_native_password"
	AUTH_CACHING_SHA2_PASSWORD = "caching_sha2_password"
	AUTH_SHA256_PASSWORD       = "sha256_password"
)

const (
	STATUS_IN_TRANS uint16 = 1 << iota
	STATUS_IN_AUTO_COMMIT
	STATUS_RESERVED
	STATUS_MORE_RESULTS_EXISTS
	STATUS_NO_GOOD_INDEX_USED
	STATUS_NO_INDEX_USED
	STATUS_CURSOR_EXISTS
	STATUS_LAST_ROW_SENT
	STATUS_DB_DROPPED
	STATUS_NO_BACK_SLASH_ESCAPES
	STATUS_META_DATA_CHANGED
	STATUS_QUERY_WAS_SLOW
	STATUS_PS_OUT_PARAMS
	STATUS_IN_TRANS_READ_ONLY
	STATUS_SESSSION_STATE_CHANGED
)
