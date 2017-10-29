#include "ocilib.h"
#include <time.h>
#include <stdio.h>
#include <stdlib.h>  
#include <string.h>

typedef struct st_Date {

    int year,month,day;

} TDate;

int isLeapyear (int y) {

    int r = (y%400==0) || ((y%4==0) && (y%100!=0)); 
    return r;
}

TDate getPreDate(TDate date) {

    TDate td=date;
    td.day--;
    if(td.day==0){

        td.month--;
        if(td.month==0){
            td.day=31;
            td.month=12;
            td.year--;
        }else{
            switch(td.month){

                case 1:
                case 3:
                case 5:
                case 7:
                case 8:
                case 10:
                    td.day=31;
                    break;
                case 4:
                case 6:
                case 9:
                case 11:
                    td.day=30; 
                    break;
                case 2:
                    td.day=( isLeapyear(td.year) ? 29 : 28 );
                    break;
            }
        }
    }   

    return td;
}

TDate getCurrentDate() {

    TDate td;
    time_t rawtime;
    struct tm * timeinfo;
    time ( &rawtime );
    timeinfo = localtime ( &rawtime );
    td.year = timeinfo->tm_year + 1900;
    td.month = timeinfo->tm_mon + 1;
    td.day = timeinfo->tm_mday;

    return td;

}

void err_handler(OCI_Error *err) {
	
	printf("code  : ORA-%05i\n" 
		   "msg   : %s\n"                 
		   "sql   : %s\n",                 
		   OCI_ErrorGetOCICode(err),                  
		   OCI_ErrorGetString(err),                 
		   OCI_GetSql(OCI_ErrorGetStatement(err))            
		   ); 
}

int main(int argc, char** argv) {

	OCI_Connection *cn_src;
    OCI_Connection *cn_des;
    OCI_Statement  *st;
    OCI_Resultset  *rs;

    char vStartDate[16] = {'\0'};

    TDate yesterday;
    yesterday = getPreDate(getCurrentDate());

    if (argc > 2) {

        printf("parameter number error!\n");
        return -1;
    }

    if (argc == 1) {

        sprintf(vStartDate, "%04d-%02d-%02d",yesterday.year,yesterday.month,yesterday.day);  

    }else{

        strcpy(vStartDate, argv[1]);

    }

	if (!OCI_Initialize(err_handler, NULL, OCI_ENV_DEFAULT)){

        return EXIT_FAILURE;

    }

    cn_src = OCI_ConnectionCreate("B2CTDB", "system", "oracle", OCI_SESSION_DEFAULT);
    cn_des = OCI_ConnectionCreate("GXYSDB", "mtrack", "mtrack", OCI_SESSION_DEFAULT);
    st = OCI_StatementCreate(cn_src);

    OCI_Prepare(st, OTEXT("select substr(t.last_outquay_time,1,10),a.company_namec,count(*) ")
    OTEXT("from hostdb.his_inventory t, hostdb.cod_company a, company b ")
    OTEXT("where t.forwarder_code = a.company_id and a.company_type = 'B' and a.company_namec = b.companyname ")
    OTEXT("and t.receive_delivery_flag = 'D' and t.last_outquay_time like :vStartDate ")
    OTEXT("and t.ctn_status <> 'E' group by a.company_namec,substr(t.last_outquay_time,1,10)"));

    OCI_BindString(st, ":vStartDate", strcat(vStartDate,"%%") , 16);
    
    OCI_Execute(st);

    rs = OCI_GetResultset(st);



    while (OCI_FetchNext(rs)){

    	//Res_Delivery_FACT fact;
        OCI_Date *date;
        OCI_Statement  *st;
        
        date = OCI_DateCreate(NULL);

        OCI_DateFromText(date, OCI_GetString(rs, 1), "YYYY-MM-DD");

        int measure = OCI_GetInt(rs, 3);
        char companyName[64] = {'\0'};
        strcpy(companyName, OCI_GetString(rs, 2));

        st = OCI_StatementCreate(cn_des);
    
        OCI_Prepare(st, OTEXT("insert into DW_DELIVERY_MEASURE ")
                    OTEXT("( ")
                    OTEXT("   ID,  COMPANY_NAME, DOCK, CTN_MEASURE, LAST_OUT_QUAY_TIME ")
                    OTEXT(") " )
                    OTEXT("values ")
                    OTEXT("( ")
                    OTEXT("   Seq_Dw_Delivery_Measure.Nextval, :vCompanyName, :vDock, :vMeasure, :vDate ")
                    OTEXT(") "));

        OCI_BindString(st, ":vCompanyName", companyName, 64);
        OCI_BindString(st, ":vDock", "BLCT2", 8);
        OCI_BindInt(st, ":vMeasure", &measure);
        OCI_BindDate(st, OTEXT(":vDate"), date);
        OCI_Execute(st);

        OCI_DateFree(date);

        //printf("time: %s, company_namec %s, count %i\n", OCI_GetString(rs, 1) ,OCI_GetString(rs, 2) , OCI_GetInt(rs, 3));

    }

    OCI_Commit(cn_des);

    OCI_ConnectionFree(cn_src);
    OCI_ConnectionFree(cn_des);
    OCI_Cleanup();

    return EXIT_SUCCESS;  

}
