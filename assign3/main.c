
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "tables.h"
#include "record_mgr.h"
#include <sys/time.h>
#include <time.h>

#define DB_PROMPT "DataBase System=# "
#define DB_VERSION "v22.01.01"
#define DB_TABLE_MAX 100
#define DB_RECORD_MAX 100

extern int main1 (void);
extern int main2 (void);

typedef enum 
{ 
	DB_DATABASE_HELP,
	DB_DATABASE_QUIT, 
	DB_DATABASE_TEST, 
	DB_TABLE_CREATE,
	DB_TABLE_DELETE,
	DB_RECORD_INSERT, 
	DB_RECORD_DELETE,
	DB_RECORD_UPDATE,
	DB_RECORD_SELECT
} DBOpsType;

typedef enum 
{ 
	DB_ATTR_CHAR,
	DB_ATTR_INT,
	DB_ATTR_FLOAT, 
	DB_ATTR_STR
} AttrType;

typedef struct {
  char attr_name[3][20];
  char attr_type[3][20];
} Table;

typedef struct {
  int a;
  char b[20];
  int c;
} parseRecord;

typedef struct {
  int id;
  char attr[20];
  char value[20];
} parseUpdate;

typedef struct {
  DBOpsType type;
  char table_name[20];
  union
  {
	  Table table;
	  parseRecord record;
	  parseUpdate update;
  }data;
  
} CMD;

typedef struct {
  RM_TableData tb;
  Schema *schema;
  RID rid[DB_RECORD_MAX];
} tableInfo;

typedef struct {
  char tableName[DB_TABLE_MAX][20];
  tableInfo ti[DB_TABLE_MAX]; 
} TableList;

//tableInfo ti ={0};
//tableInfo *table = &ti;
TableList tableList = {0};

int get_table_info(char *tableName)
{
	int i = 0;
	for(i=0;i<DB_TABLE_MAX;i++)
	{
		if(strncmp(tableName,&tableList.tableName[i],strlen(tableName)) == 0)
		{
			return i;
		}
	}
	return -1;
}

int add_table_info(char *tableName)
{
	int i = 0;
	for(i=0;i<DB_TABLE_MAX;i++)
	{
		if(strlen(&tableList.tableName[i])== 0)
		{
			strncpy(&tableList.tableName[i],tableName,strlen(tableName));
			return i;
		}
	}
	return -1;
}

int del_table_info(char *tableName)
{
	int i = 0;
	for(i=0;i<DB_TABLE_MAX;i++)
	{
		if(strncmp(tableName,&tableList.tableName[i],strlen(tableName)) == 0)
		{
			memset(&tableList.tableName[i],0,20);
			return 0;
		}
	}
	return -1;
}

char *index_to_table_name(int idx)
{
	return tableList.tableName[idx];
}

tableInfo *index_to_table_info(int idx)
{
	return &tableList.ti[idx];
}

int prepare_cmd(char* line_buffer, CMD* cmd)
{
	char tmp[256] = {0};
	int args_assigned = 0;

	if (strncmp(line_buffer, "create", 6) == 0)
	{
		cmd->type = DB_TABLE_CREATE;
		
		int args_assigned = sscanf(line_buffer, "create table %s %s %s %s %s %s %s", 
			cmd->table_name, 
			cmd->data.table.attr_name[0],cmd->data.table.attr_type[0],
			cmd->data.table.attr_name[1],cmd->data.table.attr_type[1],
			cmd->data.table.attr_name[2],cmd->data.table.attr_type[2]);
		
		if (args_assigned < 7) {
			return -1;
		}

		printf("create table:%s\n",cmd->table_name);
		printf("attr_name1:%s\n",cmd->data.table.attr_name[0]);
		printf("attr_name2:%s\n",cmd->data.table.attr_name[1]);
		printf("attr_name3:%s\n",cmd->data.table.attr_name[2]);
		printf("attr_type1:%s\n",cmd->data.table.attr_type[0]);
		printf("attr_type2:%s\n",cmd->data.table.attr_type[1]);
		printf("attr_type3:%s\n",cmd->data.table.attr_type[2]);
	}

	else if (strncmp(line_buffer, "drop", 4) == 0)
	{
		cmd->type = DB_TABLE_DELETE;
		args_assigned = sscanf(line_buffer, "drop table %s", cmd->table_name);
		if (args_assigned < 1) {
			return -1;
		}
		
		printf("table_name:%s\n",cmd->table_name);
	}
	else if (strncmp(line_buffer, "insert", 6) == 0)
	{
		cmd->type = DB_RECORD_INSERT;
		args_assigned = sscanf(line_buffer, "insert into %s value %d %s %d", 
			cmd->table_name, 
			&cmd->data.record.a,
			cmd->data.record.b,
			&cmd->data.record.c);
		//printf("%d\n",args_assigned);
		if (args_assigned < 4) {
			return -1;
		}

		printf("insert table:%s\n",cmd->table_name);
		printf("insert record:(%d %s %d)\n",cmd->data.record.a,cmd->data.record.b,cmd->data.record.c);
	}
	else if (strncmp(line_buffer, "update", 6) == 0)
	{
		cmd->type = DB_RECORD_UPDATE;
		args_assigned = sscanf(line_buffer, "update %s set %s %s where %d", 
			cmd->table_name, 
			cmd->data.update.attr,cmd->data.update.value,&cmd->data.update.id);
		//printf("%d\n",args_assigned);
		if (args_assigned < 4) {
			return -1;
		}

		printf("update table:%s\n",cmd->table_name);
		printf("update record:(%d %s %s)\n",
			cmd->data.update.id,cmd->data.update.attr,cmd->data.update.value);		
	}
	else if (strncmp(line_buffer, "delete", 6) == 0)
	{
		cmd->type = DB_RECORD_DELETE;
		args_assigned = sscanf(line_buffer, "delete from %s where id %d", cmd->table_name, &(cmd->data.record.a));
		if (args_assigned < 2) {
			return -1;
		}

		printf("delete table:%s\n",cmd->table_name);
		printf("delete record:(%d)\n",cmd->data.record.a);
	}
	else if (strncmp(line_buffer, "select",6) == 0) 
	{
		cmd->type = DB_RECORD_SELECT;
		args_assigned = sscanf(line_buffer, "select * from %s", cmd->table_name);
		if (args_assigned < 1) {
			return -1;
		}
		printf("select table_name:%s\n",cmd->table_name);
	}
	else if (strncmp(line_buffer, "quit",4) == 0)
	{
		cmd->type = DB_DATABASE_QUIT;
	} 
	else if (strncmp(line_buffer, "help",4) == 0 || strncmp(line_buffer, "?",1) == 0)
	{
		cmd->type = DB_DATABASE_HELP;
	} 
	else if (strncmp(line_buffer, "test",4) == 0)
	{
		int test_no = 0;
		cmd->type = DB_DATABASE_TEST;
		args_assigned = sscanf(line_buffer, "test %d", &test_no);
		if (args_assigned < 1) {
			return -1;
		}
		printf("test case runnint...\n");
		if(test_no==1)
		{
			main1();
		}
		else if (test_no == 2)
		{
			main2();
		}
		else if(test_no == 3)
		{
			main1();
			main2();
		}
	}
	else
		return -1;

  return 0;
}

int table_create(CMD* cmd)
{
    Schema *schema;
    int i = 0;
	int tableId = 0;
	tableInfo *table = NULL;
	int sizes[] = { 0, 4, 0 };
    int keys[] = {0};
    char **cpNames = (char **) malloc(sizeof(char*) * 3);
	DataType *cpDt = (DataType *) malloc(sizeof(DataType) * 3);
	int *cpSizes = (int *) malloc(sizeof(int) * 3);
    int *cpKeys = (int *) malloc(sizeof(int));
	
	tableId = get_table_info(cmd->table_name);
	if(tableId < 0)
	{
		tableId = add_table_info(cmd->table_name);
		if(tableId < 0)
		{
			printf("create table failed\n");
			return -1;
		}
		table = index_to_table_info(tableId);
	}
	else
	{
		printf("table %s exist\n",cmd->table_name);
		return 0;
	}
	//printf("%s%d\n",__FUNCTION__,__LINE__);
	for(i = 0; i < 3; i++)
    {
        cpNames[i] = (char *) malloc(2);
        strcpy(cpNames[i], cmd->data.table.attr_name[i]);

		if (strncmp(cmd->data.table.attr_type[i], "int", 3) == 0)
		{
			cpDt[i] = DT_INT;
		}
		if (strncmp(cmd->data.table.attr_type[i], "string", 6) == 0)
		{
			cpDt[i] = DT_STRING;
		}
    }
	
    memcpy(cpSizes, sizes, sizeof(int) * 3);
    memcpy(cpKeys, keys, sizeof(int));
	schema = createSchema(3, cpNames, cpDt, cpSizes, 1, cpKeys);
	
    initRecordManager(NULL);
    createTable(cmd->table_name,schema);
    //printf("%s%d\n",__FUNCTION__,__LINE__);
	openTable(&table->tb, cmd->table_name);
	table->schema = schema;
	return 0;
}

int table_drop(CMD* cmd)
{
	tableInfo *table = NULL;
	int tableId = -1;
	
	tableId = get_table_info(cmd->table_name);
	if(tableId < 0)
	{
		printf("table %s not found\n",cmd->table_name);
		return 0;
	}
	
	table = index_to_table_info(tableId);
	if(table == NULL)
	{
		return 0;
	}
	if (strncmp(cmd->table_name,table->tb.name,strlen(table->tb.name)) != 0)
	{
		printf("table %s is not found!\n",cmd->table_name);
		return 0;
	}
    if(closeTable(&table->tb) != 0)
	{
		printf("delete table %s error!\n",cmd->table_name);
	}
	deleteTable(table->tb.name);
    shutdownRecordManager();
	freeSchema(table->schema);
	
	char *tableName = index_to_table_name(tableId); 
	memset(tableName,0,20);
	memset(table,0,sizeof(tableInfo));
    return 0;
}


Record *
testRecord1(Schema *schema, int a, char *b, int c)
{
    Record *result = NULL;
    Value *value = NULL;

    createRecord(&result, schema);

    MAKE_VALUE(value, DT_INT, a);
    setAttr(result, schema, 0, value);

    freeVal(value);
    value = NULL;

    MAKE_STRING_VALUE(value, b);
    setAttr(result, schema, 1, value);

    freeVal(value);
    value = NULL;

    MAKE_VALUE(value, DT_INT, c);
    setAttr(result, schema, 2, value);
    freeVal(value);
    value = NULL;

    return result;
}


int record_insert(CMD* cmd)
{
	Record *r;
	tableInfo *table = NULL;
	int tableId = -1;
	tableId = get_table_info(cmd->table_name);
	if(tableId < 0)
	{
		printf("table %s not found\n",cmd->table_name);
		return 0;
	}
	
	table = index_to_table_info(tableId);
	if(table == NULL)
	{
		return 0;
	}
	
	if (strncmp(cmd->table_name,table->tb.name,strlen(table->tb.name)) != 0)
	{
		printf("table %s is not found!\n",cmd->table_name);
		return 0;
	}
	
	r = testRecord1(table->schema, cmd->data.record.a, (char *)&cmd->data.record.b, cmd->data.record.c);
	if (r == NULL)
	{
		return -1;
	}
	insertRecord(&table->tb,r); 
	table->rid[cmd->data.record.a] = r->id;
	freeRecord(r); 
    return 0;
}

int record_update(CMD* cmd)
{
	Record *r;
    int i,j; 
	tableInfo *table = NULL;
	int tableId = -1;
	tableId = get_table_info(cmd->table_name);
	if(tableId < 0)
	{
		printf("table %s not found\n",cmd->table_name);
		return 0;
	}
	
	table = index_to_table_info(tableId);
	if(table == NULL)
	{
		return 0;
	}
	
	parseRecord update = {0};
	if (cmd->data.update.id > DB_RECORD_MAX)
	{
		return 0;
	}
	
	if(table->rid[cmd->data.update.id].page ==0 && table->rid[cmd->data.update.id].slot==0)
	{
		return 0;
	}
	
    createRecord(&r, table->schema);
	if(getRecord(table, table->rid[cmd->data.update.id], r) == RC_OK)
    {
		Value *lVal1 = NULL; 
		Value *lVal2 = NULL; 
		Value *lVal3 = NULL; 
		int res = 0;
		getAttr(r, table->schema, 0, &lVal1);
		getAttr(r, table->schema, 1, &lVal2);
		getAttr(r, table->schema, 2, &lVal3);
		if(lVal1->v.intV != 0 && lVal3->v.intV != 0)
		{
			update.a = lVal1->v.intV;
			strncpy(update.b,lVal2->v.stringV,strlen(lVal2->v.stringV));
			update.c = lVal3->v.intV;
		}
		free(lVal1);
		free(lVal2);
		free(lVal3);
	}
	freeRecord(r); 
	
	for (i=1;i<3;i++)
	{
		if(strncmp(cmd->data.update.attr,table->schema->attrNames[i],
			strlen(table->schema->attrNames[i])) == 0)
		{
			if(i==1)
			{
				memset(update.b,0,sizeof(update.b));
				strncpy(update.b,cmd->data.update.value,strlen(cmd->data.update.value));
			}
			if(i==2)
			{
				(void)sscanf(cmd->data.update.value, "%d", &update.c);
			}
		}
	}
	
	//r = fromTestRecord(schema, &update);
	r = testRecord1(table->schema, update.a, (char *)&update.b, update.c);
    r->id = table->rid[cmd->data.update.id];
    updateRecord(&table->tb,r);
    freeRecord(r);  
    return 0;
}

int record_delete(CMD* cmd)
{
	Record *r;
	int i,j; 
	tableInfo *table = NULL;
	int tableId = -1;
	tableId = get_table_info(cmd->table_name);
	if(tableId < 0)
	{
		printf("table %s not found\n",cmd->table_name);
		return 0;
	}
	
	table = index_to_table_info(tableId);
	if(table == NULL)
	{
		return 0;
	}
	
	if (strncmp(cmd->table_name,table->tb.name,strlen(table->tb.name)) != 0)
	{
		printf("table %s is not found!\n",cmd->table_name);
		return 0;
	}
	
	deleteRecord(&table->tb,table->rid[cmd->data.record.a]);

    return 0;
}

int record_select(CMD* cmd)
{
	Record *r;
    int i,j; 
	int row = 0;
	
	tableInfo *table = NULL;
	int tableId = -1;
	tableId = get_table_info(cmd->table_name);
	if(tableId < 0)
	{
		printf("table %s not found\n",cmd->table_name);
		return 0;
	}
	
	table = index_to_table_info(tableId);
	if(table == NULL)
	{
		return 0;
	}
	
	if(table->schema == NULL)
	{
		return 0;
	}
	
    createRecord(&r, table->schema);
    for(i = 0;i<DB_RECORD_MAX;i++)
    {
		if(table->rid[i].page ==0 && table->rid[i].slot==0)
		{
			continue;
		}
	    if(getRecord(table, table->rid[i], r) != RC_OK)
		{
            continue;
		}
		Value *lVal1 = NULL; 
		Value *lVal2 = NULL; 
		Value *lVal3 = NULL; 
		int res = 0;
		getAttr(r, table->schema, 0, &lVal1);
		getAttr(r, table->schema, 1, &lVal2);
		getAttr(r, table->schema, 2, &lVal3);
		if(lVal1->v.intV != 0 && lVal3->v.intV != 0)
		{
			printf("(%d %s %d)\n",lVal1->v.intV,lVal2->v.stringV,lVal3->v.intV);
			row++;
		}
		free(lVal1);
		free(lVal2);
		free(lVal3);
		
	}
	freeRecord(r);
	printf("Total rows:%d\n",row);
	return 0;
}

void database_quit(char* line_buffer,char *cmd)
{
	int i = -1;
	for (i=0;i<DB_TABLE_MAX;i++)
	{
		tableInfo *table = NULL;
		table = index_to_table_info(i);
		if(table == NULL)
		{
			continue;
		}
		
		if(table->schema == NULL)
		{
			continue;
		}
		
		closeTable(&table->tb);
		deleteTable(table->tb.name);
		shutdownRecordManager();
		freeSchema(table->schema);
	}
	
	free(line_buffer);
	line_buffer = NULL;
    exit(EXIT_SUCCESS);
	return;
}

void database_help(char* line_buffer)
{
    printf("The data base help information!\n");
	printf("	Create Table using: create table <tableName> <attrNames1> <attrVlue1> <attrNames2> <attrVlue2> <attrNames3> <attrVlue3>\n");
	printf("		eg. create table table0 a int b string c int\n");
	printf("	Drop Table using: drop table <tableName>\n");
	printf("		eg. drop table table0\n");
	printf("	Insert record to table using: insert into <tableName> value <attrVlue1> <attrVlue2> <attrVlue3>\n");
	printf("		eg. insert into table0 value 1 1111 1\n");
	printf("	Update record to table using: update <tableName> set <attrNamex> <attrVluex> where <attrVlue1>\n");
	printf("		eg. update table0 set b 1122 where 1\n");
	printf("	Delete record to table using: delete from <tableName> where id <attrVlue1>\n");
	printf("		eg. delete from table0 where id 1\n");
	printf("	Display record to table using: select * from <tableName>\n");
	printf("		eg. select * from table0\n");
	printf("	Test case running using: test [1 | 2 | 3]\n");
	printf("		eg. test 3\n");
	printf("	Exit dataBase using: quit\n");
	printf("		eg. quit\n");
	
	return;
}

int execute_cmd(char *line_buffer,CMD* cmd)
{
	int res = 0;
	switch (cmd->type)
	{
		case DB_DATABASE_QUIT:
			//printf("%s%d\n",__FUNCTION__,__LINE__);
			database_quit(line_buffer,cmd);
			break;
		case DB_TABLE_CREATE:
			//printf("%s%d\n",__FUNCTION__,__LINE__);
			res = table_create(cmd);
			break;
		case DB_TABLE_DELETE:
			//printf("%s%d\n",__FUNCTION__,__LINE__);
			res = table_drop(cmd);
			break;
		case DB_RECORD_INSERT:
			//printf("%s%d\n",__FUNCTION__,__LINE__);
			res = record_insert(cmd);
			break;
		case DB_RECORD_UPDATE:
			//printf("%s%d\n",__FUNCTION__,__LINE__);
			res = record_update(cmd);
			break;
		case DB_RECORD_DELETE:
			//printf("%s%d\n",__FUNCTION__,__LINE__);
			res = record_delete(cmd);
			break;
		case DB_RECORD_SELECT:
			res = record_select(cmd);
			break;
		case DB_DATABASE_HELP:
			database_help(cmd);
			break;
	}
	return res;
}



struct timezone_t 

{
      int tz_minuteswest; 
      int tz_dsttime;
};

void get_system_time(char *buff)
{
    struct timeval tv;
    struct timezone_t tzone;   
    struct tm *t;
     
    gettimeofday(&tv, &tzone);
    t = localtime(&tv.tv_sec);
    sprintf(buff,"%d-%d-%d %d:%d:%d\n", 1900+t->tm_year, 1+t->tm_mon, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec);
}
int main() 
{
	char time_buffer[100] = {0};
	char* line_buffer = NULL;
	size_t line_length;
	size_t input_length ;
	size_t read_length;
	
	get_system_time(time_buffer);
	printf("DataBase version %s %s",DB_VERSION, time_buffer);
	printf("Type 'help' for help.\n");
	printf("You are now connected to database.\n\n");
	printf(DB_PROMPT);
    while (read_length = getline(&line_buffer, &line_length, stdin) > 0)
    {
		input_length = strlen(line_buffer) - 1;
		line_buffer[input_length] = 0;
		CMD cmd = {0};
	
		if (strlen(line_buffer) == 0)
		{
			goto END;
		}
		
		if (prepare_cmd(line_buffer, &cmd) != 0)
		{
			printf("Unrecognized keyword at start of '%s'.\n",line_buffer);
			goto END;
		}
		//printf("%s%d\n",__FUNCTION__,__LINE__);
		if(execute_cmd(line_buffer,&cmd) != 0)
		{
			printf("command exec failed\n");
			goto END;
		}
		//printf("%s%d\n",__FUNCTION__,__LINE__);
END:
		line_length = read_length= 0;
		//printf("%s%d\n",__FUNCTION__,__LINE__);
		if (line_buffer)
		{
			free(line_buffer);
			line_buffer = NULL;
		}
		
		printf(DB_PROMPT);
  }
}
