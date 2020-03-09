#include "server.h"
#include "sqlite3/sqlite3.h"

#define ENHANCED_DB_FILENAME	"enhanced.db"
#define ENTRANCED_DB_FILENAME	"entranced.db"

// global sqlite db handle
static sqlite3 *enhancedDbDisk = NULL, *enhancedDbMemory = NULL, *entrancedDbDisk = NULL, *entrancedDbMemory = NULL;

static void ErrorCallback(void *ctx, int code, const char *msg) {
	Com_Printf("Server: SQL error (code %d): %s\n", code, msg);
}

static int TraceCallback(unsigned int type, void *ctx, void *ptr, void *info) {
	if (!ptr || !info || !sv_traceSQL->integer) {
		return 0;
	}

	if (type == SQLITE_TRACE_STMT) {
		char *sql = (char *)info;

		Com_Printf("Server: executing SQL: \n");

		if (!Q_stricmpn(sql, "--", 2)) {
			// a comment, which means this is a trigger, log it directly
			Com_Printf("--------------------------------------------------------------------------------\n");
			Com_Printf("%s\n", sql);
			Com_Printf("--------------------------------------------------------------------------------\n");
		}
		else {
			// expand the sql before logging it so we can see parameters
			sqlite3_stmt *stmt = (sqlite3_stmt *)ptr;
			sql = sqlite3_expanded_sql(stmt);
			Com_Printf("--------------------------------------------------------------------------------\n");
			Com_Printf("%s\n", sql);
			Com_Printf("--------------------------------------------------------------------------------\n");
			sqlite3_free(sql);
		}
	}
	else if (type == SQLITE_TRACE_PROFILE) {
		unsigned long long nanoseconds = *((unsigned long long *)info);
		unsigned int ms = nanoseconds / 1000000;
		Com_Printf("Executed in %ums\n", ms);
	}

	return 0;
}

void SV_DB_Init(void) {
	sqlite3_config(SQLITE_CONFIG_SINGLETHREAD); // we don't need multi threading
	sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0); // we don't need allocation statistics
	sqlite3_config(SQLITE_CONFIG_LOG, ErrorCallback, NULL); // error logging

	int rc = sqlite3_initialize();

	if (rc != SQLITE_OK) {
		Com_Printf("SV_DB_Init: failed to initialize SQLite3 (code: %d)\n", rc);
		return;
	}

	qboolean gotOne = qfalse;

	// try to load enhanced
	rc = sqlite3_open_v2(ENHANCED_DB_FILENAME, &enhancedDbDisk, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
	if (rc == SQLITE_OK) {
		gotOne = qtrue;
		Com_Printf("SV_DB_Init: successfully loaded " ENHANCED_DB_FILENAME ".\n");
	}

	// try to load entranced
	rc = sqlite3_open_v2(ENTRANCED_DB_FILENAME, &entrancedDbDisk, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
	if (rc == SQLITE_OK) {
		gotOne = qtrue;
		Com_Printf("SV_DB_Init: successfully loaded " ENTRANCED_DB_FILENAME ".\n");
	}

	if (!gotOne) {
		Com_Printf("SV_DB_Init: failed to load either " ENHANCED_DB_FILENAME " or " ENTRANCED_DB_FILENAME "!\n");
		return;
	}

	// try to load enhanced into memory
	rc = sqlite3_open_v2(":memory:", &enhancedDbMemory, SQLITE_OPEN_READWRITE, NULL);
	if (enhancedDbDisk) {
		if (rc == SQLITE_OK) {
			sqlite3_backup *backup = sqlite3_backup_init(enhancedDbMemory, "main", enhancedDbDisk, "main");
			if (backup) {
				rc = sqlite3_backup_step(backup, -1);
				if (rc == SQLITE_DONE) {
					rc = sqlite3_backup_finish(backup);
					if (rc != SQLITE_OK) {
						Com_Printf("SV_DB_Init: unable to load " ENHANCED_DB_FILENAME " into memory!\n");
						enhancedDbDisk = NULL;
						enhancedDbMemory = NULL;
						return;
					}
				}
				else {
					Com_Printf("SV_DB_Init: unable to load " ENHANCED_DB_FILENAME " into memory!\n");
					enhancedDbDisk = NULL;
					enhancedDbMemory = NULL;
					return;
				}
			}
			else {
				Com_Printf("SV_DB_Init: unable to load " ENHANCED_DB_FILENAME " into memory!\n");
				enhancedDbDisk = NULL;
				enhancedDbMemory = NULL;
				return;
			}
		}
		else {
			Com_Printf("SV_DB_Init: unable to load " ENHANCED_DB_FILENAME " into memory!\n");
			enhancedDbDisk = NULL;
			enhancedDbMemory = NULL;
			return;
		}
	}

	// try to load entranced into memory
	rc = sqlite3_open_v2(":memory:", &entrancedDbMemory, SQLITE_OPEN_READWRITE, NULL);
	if (entrancedDbDisk) {
		if (rc == SQLITE_OK) {
			sqlite3_backup *backup = sqlite3_backup_init(entrancedDbMemory, "main", entrancedDbDisk, "main");
			if (backup) {
				rc = sqlite3_backup_step(backup, -1);
				if (rc == SQLITE_DONE) {
					rc = sqlite3_backup_finish(backup);
					if (rc != SQLITE_OK) {
						Com_Printf("SV_DB_Init: unable to load " ENTRANCED_DB_FILENAME " into memory!\n");
						entrancedDbDisk = NULL;
						entrancedDbMemory = NULL;
						return;
					}
				}
				else {
					Com_Printf("SV_DB_Init: unable to load " ENTRANCED_DB_FILENAME " into memory!\n");
					entrancedDbDisk = NULL;
					entrancedDbMemory = NULL;
					return;
				}
			}
			else {
				Com_Printf("SV_DB_Init: unable to load " ENTRANCED_DB_FILENAME " into memory!\n");
				entrancedDbDisk = NULL;
				entrancedDbMemory = NULL;
				return;
			}
		}
		else {
			Com_Printf("SV_DB_Init: unable to load " ENTRANCED_DB_FILENAME " into memory!\n");
			entrancedDbDisk = NULL;
			entrancedDbMemory = NULL;
			return;
		}
	}

	// register trace callback if needed
	if (sv_traceSQL->integer) {
		if (enhancedDbDisk)
			sqlite3_trace_v2(enhancedDbDisk, SQLITE_TRACE_STMT | SQLITE_TRACE_PROFILE, TraceCallback, NULL);
		if (enhancedDbMemory)
			sqlite3_trace_v2(enhancedDbMemory, SQLITE_TRACE_STMT | SQLITE_TRACE_PROFILE, TraceCallback, NULL);
		if (entrancedDbDisk)
			sqlite3_trace_v2(entrancedDbDisk, SQLITE_TRACE_STMT | SQLITE_TRACE_PROFILE, TraceCallback, NULL);
		if (entrancedDbMemory)
			sqlite3_trace_v2(entrancedDbMemory, SQLITE_TRACE_STMT | SQLITE_TRACE_PROFILE, TraceCallback, NULL);
	}
	else {
		if (enhancedDbDisk)
			sqlite3_trace_v2(enhancedDbDisk, 0, NULL, NULL);
		if (enhancedDbMemory)
			sqlite3_trace_v2(enhancedDbMemory, 0, NULL, NULL);
		if (entrancedDbDisk)
			sqlite3_trace_v2(entrancedDbDisk, 0, NULL, NULL);
		if (entrancedDbMemory)
			sqlite3_trace_v2(entrancedDbMemory, 0, NULL, NULL);
	}
}

void SV_DB_Save(void) {
	if (enhancedDbMemory && enhancedDbDisk) {
		int startTime = Sys_Milliseconds();
		Com_Printf("SV_DB_Save: saving " ENHANCED_DB_FILENAME " changes to disk...");

		// we are using in memory db, save changes to disk
		qboolean success = qfalse;
		sqlite3_backup *backup = sqlite3_backup_init(enhancedDbDisk, "main", enhancedDbMemory, "main");
		if (backup) {
			int rc = sqlite3_backup_step(backup, -1);
			if (rc == SQLITE_DONE) {
				rc = sqlite3_backup_finish(backup);
				if (rc == SQLITE_OK) {
					success = qtrue;
				}
			}
		}

		int finishTime = Sys_Milliseconds();
		if (success)
			Com_Printf("done (took %d milliseconds).\n", finishTime - startTime);
		else
			Com_Printf("WARNING: Failed to backup " ENHANCED_DB_FILENAME "! Changes from this session have NOT been saved!\n");
	}

	if (entrancedDbMemory && entrancedDbDisk) {
		int startTime = Sys_Milliseconds();
		Com_Printf("SV_DB_Save: saving " ENTRANCED_DB_FILENAME " changes to disk...");

		// we are using in memory db, save changes to disk
		qboolean success = qfalse;
		sqlite3_backup *backup = sqlite3_backup_init(entrancedDbDisk, "main", entrancedDbMemory, "main");
		if (backup) {
			int rc = sqlite3_backup_step(backup, -1);
			if (rc == SQLITE_DONE) {
				rc = sqlite3_backup_finish(backup);
				if (rc == SQLITE_OK) {
					success = qtrue;
				}
			}
		}

		int finishTime = Sys_Milliseconds();
		if (success)
			Com_Printf("done (took %d milliseconds).\n", finishTime - startTime);
		else
			Com_Printf("WARNING: Failed to backup " ENTRANCED_DB_FILENAME "! Changes from this session have NOT been saved!\n");
	}
}

// returns qtrue if this is the first time we are ever getting it this session,
// or if we changed between different db files (i.e., we switched mods/gametypes during a single session)
qboolean SV_DB_Get(void **ptr) {
	static void *lastLoaded = NULL;

	char *gameversion = Cvar_VariableString("gameversion");
	if (VALIDSTRING(gameversion) && Q_stristr(gameversion, "base_entranced"))
		*ptr = entrancedDbMemory;
	else
		*ptr = enhancedDbMemory;

	if (*ptr != lastLoaded) {
		if (lastLoaded)
			SV_DB_Save(); // we changed, so save it, too
		lastLoaded = *ptr;
		return qtrue;
	}

	lastLoaded = *ptr;
	return qfalse;
}