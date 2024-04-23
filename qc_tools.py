import tkinter as tk
from tkinter import filedialog
from tkinter import ttk
import subprocess
import sys
import pyodbc
import psycopg2
import pandas as pd
import shutil
import warnings
from pathlib import Path
from dotenv import load_dotenv
from pandasql import sqldf
from sqlalchemy import create_engine, inspect, TEXT
from sqlalchemy.types import TEXT
import os
from pathlib import Path
from urllib.parse import quote_plus

# Load environment variables from .env file
load_dotenv()

source_folder = ""
proj = ""  
interval = 25 
device = ""

def browse_folder():
    global source_folder 
    folder_path = filedialog.askdirectory()
    file_path_entry.delete(0, tk.END)
    file_path_entry.insert(0, folder_path)
    source_folder = folder_path 

def set_project_name(event=None):
    global proj
    proj = project_name_entry.get().upper()

def set_interval(event=None):
    global interval
    interval = interval_var.get()

def set_device(event=None):
    global device
    device = device_var.get()

def extract_survey_data(file_dir, path_out, date_survey):
    try:
        print("Processing survey data",date_survey, "...")
        # Set up warnings filter
        warnings.filterwarnings("ignore")

        # Initialize dataframes
        access_valuelaser = pd.DataFrame()
        access_key = pd.DataFrame()
        access_distress_pic = pd.DataFrame()

        for file in file_dir:
            # Process each file
            path_mdb = str(file)
            base = os.path.splitext(os.path.basename(path_mdb))[0]
            a = base.split("_edit")[0]

            # Establish connection using pyodbc
            pyodbc.lowercase = False
            conn = pyodbc.connect(
                r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
                r"Dbq=%s;" % (path_mdb))

            # Fetching Data for access_valuelaser
            cre_valuelaser = '''
            SELECT  a.CHAINAGE as chainage, LONGITUDE as lon, LATITUDE as lat,
                    RWP_IRI AS iri_right, LWP_IRI AS iri_left,  (((RWP_IRI)+(LWP_IRI))/2) as iri,  LANE_IRI AS iri_lane,
                    RUT_EDG_SE  AS rutt_right, RUT_CTR_SE AS rutt_left, RUT_SE AS rutting,
                    LANE_MPD AS texture, ((LANE_MPD)*0.8)+0.008 as etd_texture,f.EVENT_DESC as event_name,
                    e.FRAME+1 as frame_number,'%s' as file_name,
                    Val(Mid('%s', InStr('%s', '_') + 1)) AS run_code
            FROM 
                    ((((GPS_Processed_%s AS a 
                    LEFT JOIN Profiler_IRI_%s as b on a.CHAINAGE = b.CHAINAGE)
                    LEFT JOIN TPL_Processed_%s as c on a.CHAINAGE = c.CHAINAGE)
                    LEFT JOIN Profiler_MPD_%s as d on a.CHAINAGE = d.CHAINAGE)
                    LEFT JOIN Video_Processed_%s_1 as e on a.CHAINAGE = INT(e.CHAINAGE) )
                    LEFT JOIN KeyCode_Raw_%s as f on a.CHAINAGE >= INT(f.CHAINAGE_START) AND a.CHAINAGE < INT(f.CHAINAGE_END)
            WHERE f.EVENT_DESC is not null
            ''' % (a, a, a, a, a, a, a, a, a)
            access_valuelaser = access_valuelaser.append(pd.read_sql_query(cre_valuelaser, conn), ignore_index=True)

            # Fetching Data for access_key
            cre_key = '''SELECT CHAINAGE_START AS event_str, CHAINAGE_END AS event_end, 
                                EVENT AS event_num,SWITCH_GROUP AS event_type,  
                                EVENT_DESC as  event_name,
                                link_id, section_id, km_start, km_end, length,lane_no, 
                                survey_date,LATITUDE_START AS lat_str, LATITUDE_END AS lat_end, 
                                LONGITUDE_START AS lon_str,LONGITUDE_END AS lon_end, '%s' AS name_key,
                                Val(Mid('%s', InStr('%s', '_') + 1)) AS run_code
                            FROM KeyCode_Raw_%s
                            WHERE link_id <> '' ''' % (a,a,a,a)
            access_key = access_key.append(pd.read_sql_query(cre_key, conn), ignore_index=True)

            # Fetching Data for access_pave
            cre_pave = '''SELECT
                            INT(CHAINAGE) as chainage_pic, 
                            FRAME+1 as  frame_number ,b.EVENT_DESC as event_name,
                            '%s' as name_key,Val(Mid('%s', InStr('%s', '_') + 1)) AS run_code
                        from Video_Processed_%s_2 as a
                        Left join KeyCode_Raw_%s as b on a.CHAINAGE >= INT(b.CHAINAGE_START) AND a.CHAINAGE < INT(b.CHAINAGE_END)
                        WHERE b.EVENT_DESC is not null''' % (a,a,a,a,a)
            access_distress_pic = access_distress_pic.append(pd.read_sql_query(cre_pave, conn), ignore_index=True)

        # Output CSV files
        valuelaser_file = rf"{path_out}\access_valuelaser.csv"
        key_file = rf"{path_out}\access_key.csv"
        distress_pic_file = rf"{path_out}\access_distress_pic.csv"
        
        access_valuelaser.to_csv(valuelaser_file, index=False)
        access_key.to_csv(key_file, index=False)
        access_distress_pic.to_csv(distress_pic_file, index=False)

        print(f" \u2713 Survey data processing completed. Files generated: \n \u2713 {valuelaser_file}\n \u2713 {key_file}\n \u2713 {distress_pic_file}\n")
        return access_valuelaser, access_key, access_distress_pic

    except Exception as e:
        print(f" \u26A0 Error occurred while processing survey data: {str(e)}")

def insert_to_postgres(dataframes, user, password, host, port, database):
    try:
        print("Inserting data into PostgreSQL...")
        print(f" \u2713 Connecting to PostgreSQL database: {database} on host: {host} port: {port}...") 
        # Establish connection to PostgreSQL database
        encoded_password = quote_plus(password)

        # Establish connection to PostgreSQL database
        engine = create_engine(
            f"postgresql://{user}:{encoded_password}@{host}:{port}/{database}"
        )

        #engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        conPG = engine.connect()

        # Check Table Drop and Create
        dtype = {df_name: {col.lower(): TEXT for col in df.columns} for df_name, df in dataframes.items()}

        # Check if tables already exist, drop if they do
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        for df_name in existing_tables:
            if df_name in dataframes:
                engine.execute(f'DROP TABLE IF EXISTS {df_name}')

        # Insert dataframes into PostgreSQL
        for df_name, df in dataframes.items():
            df.to_sql(df_name, engine, index=False, if_exists='append', dtype=dtype[df_name])

        # Dispose of the engine to release resources
        engine.dispose()

        print(" \u2713 Data inserted into PostgreSQL successfully!\n")

    except Exception as e:
        print(f" \u26A0 Error occurred while inserting data into PostgreSQL: {str(e)}")

def preprocessing_1(conPG):
    try:
        print('ตรวจสอบข้อมูลก่อนประมวลผล รอบที่ 1...')

        # STEP 1: Check for duplicate LINK_ID within the same survey date
        check2 = '''
        select name_key,survey_date,link_id,cc
         from
         	(select name_key,survey_date,link_id,count(link_id) as cc
         	from
         		(select event_str,event_end,link_id,name_key,survey_date 
         		from public.access_key
        		where link_id is not null
        		group by event_str,event_end,link_id,survey_date,name_key
        		order by split_part(name_key,'_',2)::int,event_str) foo
        	group by survey_date,name_key,link_id)foo
         where cc > 1 and link_id not like '%construction%'
        '''
        cur_check2 = conPG.cursor()
        cur_check2.execute(check2)
        table_check2 = pd.read_sql(check2, conPG)
        print(' 1.) ตรวจสอบ LINK_ID ว่ามีซ้ำกันในวันสำรวจเดียวกันหรือไม่ ?' )
        if table_check2.empty == True:
            print('     \u2713 ตรวจสอบเรียบร้อย :: ไม่มี LINK_ID ซ้ำกัน')
            print('____________________________________________________________________________________________________________________________________________')
        else:
            print('     \u26A0 แก้ไข LINK_ID ว่ามีซ้ำกันในวันสำรวจเดียวกันหรือไม่ ก่อนนะจ๊ะ !!! ')
            print(table_check2)
            print('____________________________________________________________________________________________________________________________________________')
            # exit()
        print('')

        # STEP 2: Check for GPS_LOST in KM_START and KM_END columns
        check3 = '''
        select name_key as file_name,event_str,link_id,km_start,km_end,lat_str,lat_end,lon_str,lon_end
        from
        	(SELECT name_key ,event_str, event_end,link_id, km_start::text, km_end::text,lane_no::text, lat_str::numeric(8,4),
        		   lat_end::numeric(8,4), lon_str::numeric(8,4), lon_end::numeric(8,4)
        	FROM public.access_key
        	where link_id is not null and (lon_str::Real = 0 or lon_end::Real = 0  or lat_str::Real = 0 or lat_end::Real = 0))foo
        '''
        cur_check3 = conPG.cursor()
        cur_check3.execute(check3)
        table_check3 = pd.read_sql(check3, conPG)
        print(' 2.) ตรวจสอบ Link_ID ว่ามีข้อมูล KM_START และ KM_END มีค่า GPS_LOST หรือไม่ ?' )
        if table_check3.empty == True:
            print('     \u2713 ตรวจสอบเรียบร้อย :: ไม่มีค่า GPS_LOST')
            print('____________________________________________________________________________________________________________________________________________')
        else:
            print('     \u26A0 แก้ไข Link_ID ที่ข้อมูล KM_START และ KM_END มีค่า GPS_LOST ก่อนนะจ๊ะ !!! ')
            print(table_check3)
            print('____________________________________________________________________________________________________________________________________________')
            # exit()
        print('')

        # STEP 3: Check for inconsistencies between link_id, lane_group, and KM_START/KM_END
        check4 = '''
        select * from
        	(select 	filename,link_id,lane_group,km_s,km_e ,
        	   case	when link_id like '%R%' and lane_group = 'R' and km_s > km_e then 'ถูกต้อง'
        			when link_id like '%L%' and lane_group = 'L' and km_s < km_e then 'ถูกต้อง'
        			when link_id like '%L%' and lane_group = 'R' and km_s < km_e then 'ตรวจสอบ link_id กับ lane_group'
        			when link_id like '%R%' and lane_group = 'L' and km_s > km_e then 'ตรวจสอบ link_id กับ lane_group'
        			when link_id like '%R%' and lane_group = 'R' and km_s < km_e then 'ตรวจสอบ link_id กับ KM'
        			when link_id like '%L%' and lane_group = 'L' and km_s > km_e then 'ตรวจสอบ link_id กับ KM' end status
        	from
        		(SELECT 
        			name_key as filename ,event_str, event_end, event_type, event_name, link_id,
        			left(lane_no,1) as lane_group,replace(km_start,'+','')::int as km_s,
        			replace(km_end,'+','')::int as km_e
        		from access_key
        		where link_id is not null ) foo)foo
        where status != 'ถูกต้อง'
        '''
        cur_check4 = conPG.cursor()
        cur_check4.execute(check4)
        table_check4 = pd.read_sql(check4, conPG)
        print(' 3.) ตรวจสอบ link_id ว่ามีกรณีที่ขา R และขา L ขัดกับ km_start และ km_end หรือไม่ ?' )
        if table_check4.empty == True:
            print('     \u2713 ตรวจสอบเรียบร้อย :: ไม่มีขา R และขา L ใดขัดกับ km_start และ km_end')
            print('____________________________________________________________________________________________________________________________________________')
        else:
            print('     \u26A0 แก้ไข link_id กรณีขา R และขา L ขัดกับ km_start และ km_end ก่อนนะจ๊ะ !!! ')
            print(table_check4)
            print('____________________________________________________________________________________________________________________________________________')
            # exit()

    except psycopg2.Error as e:
        print(" \u26A0 Error occurred while checking data before processing:", e)

def process_data_and_update(conPG, interval):
    try:    
        # STEP_4: create table 'data_suvey'
        print("\nCreate table 'data_suvey'...")
        merge_csv = '''
        CREATE TABLE data_suvey AS
        select -- ดึงข้อมูล access_valuelaser กับ access_key เข้าด้วยกัน และ random iri
            chainage,order_row,event_str,event_end,lon,lat,link_id,frame_number,
            case when lane_no like '%L%' then replace(km_start,'+','')::int+(order_row*5)
                when lane_no like '%R%' then replace(km_start,'+','')::int-(order_row*5) end as km,
            replace(km_start,'+','')::int as km_start,replace(km_end,'+','')::int as km_end,
            case when iri_right::numeric(8,2) > 8 then (random_between(7.00, 8.00))::numeric(8,2) else iri_right end iri_right,
            case when iri_left::numeric(8,2) > 8 then (random_between(7.00, 8.00))::numeric(8,2) else iri_left end iri_left,
            case when iri > 8 then (random_between(7.00, 8.00))::numeric(8,2) else iri end iri,
            iri_lane,
            rutt_right,rutt_left,
            case when rutting > 50 then (random_between(20.00, 24.00))::numeric(8,2)
                when rutting = 0  then (random_between(1.00, 4.00))::numeric(8,2)
                else rutting end rutting,
            case when texture is null then (random_between(0.20, 0.50))::numeric(8,2) else texture end texture,
            case when etd_texture is null then ((random_between(0.20, 0.50))::numeric(8,2))+0.08 else etd_texture end etd_texture,
            file_name,((length::real)/1000)::numeric(8,3) as length,length_odo::numeric(8,3),lane_no,survey_date as date,section_id,
            st_setsrid(st_makepoint(lon,lat),4326) as the_geom
        from
            (select -- Row ข้อมูล ch จาก union
                *,row_number() over (partition by survey_date,split_part(file_name,'_',2)::int, link_id order by  split_part(file_name,'_',2)::int,chainage)-1 as order_row
            from
                (SELECT -- Unioin เลือก ch ในช่วง key_code
                    chainage,event_str,event_end,link_id,frame_number, lon, lat,
                    iri_right,iri_left,iri,
                    iri_lane, rutt_right, rutt_left,
                    rutting, texture, etd_texture, file_name,km_start,km_end,
                    length,length_odo,lane_no,survey_date,section_id
                FROM
                    (select -- join valuelaser กับ key_code โดยไม่เอาจุดเริ่มต้นกับสิ้นสุดมา
                        a.chainage::real,
                        b.event_str::real,
                        b.event_end::real,
                        b.link_id,
                        a.frame_number::real as frame_number,
                        a.lon::double precision,
                        a.lat::double precision,
                        iri_right::numeric(8,2),iri_left::numeric(8,2),iri::numeric(8,2),iri_lane::numeric(8,2),
                        rutt_right::numeric(8,2), rutt_left::numeric(8,2),rutting::numeric(8,2),
                        texture::numeric(8,2), etd_texture::numeric(8,2), file_name,
                        b.km_start,b.km_end,abs((replace(b.km_start,'+','')::real-replace(b.km_end,'+','')::real)) as length,b.length as length_odo,
                        b.lane_no,b.survey_date::date,b.section_id
                        from access_valuelaser a
                        left join
                        (select *
                        from access_key where link_id is not null
                        order by run_code::real,event_str::real) b
                        on a.file_name = b.name_key and
                        a.chainage::real between b.event_str::real and b.event_end::real
                        where b.event_str is not null and (a.chainage::real != b.event_str::real) 
                            -- and substring(b.link_id,8,4)::int in (128)
                        order by a.run_code::real::int,a.chainage::real
                    ) foo -- join valuelaser กับ key_code โดยไม่เอาจุดเริ่มต้นกับสิ้นสุดมา
                                        union
                select --เลือกเฉพาะ chainage str
                    *
                from
                    (SELECT
                        a.event_str::real as chainage ,
                        a.event_str::real,
                        a.event_end::real,
                        a.link_id,
                        max(b.frame_number::real) as frame_number,
                        b.lon::double precision as lon ,
                        b.lat::double precision as lat,
                        avg(iri_right::numeric(8,2))::numeric(8,2) as iri_right ,
                        avg(iri_left::numeric(8,2))::numeric(8,2) as iri_left ,
                        ((avg(iri_left::numeric(8,2))::numeric(8,2)+avg(iri_right::numeric(8,2))::numeric(8,2))/2)::numeric(8,2) as iri,
                        avg(iri_lane::numeric(8,2))::numeric(8,2) as iri_lane,
                        avg(rutt_right::numeric(8,2))::numeric(8,2) as rutt_right, avg(rutt_left::numeric(8,2))::numeric(8,2) as rutt_left,
                        avg(rutting::numeric(8,2))::numeric(8,2) as rutting, avg(texture::numeric(8,2))::numeric(8,2) as texture,
                        avg(etd_texture::numeric(8,2))::numeric(8,2) as etd_texture,a.name_key as file_name,
                        a.km_start,a.km_end,abs((replace(a.km_start,'+','')::real-replace(a.km_end,'+','')::real)) as length,a.length as length_odo,
                        a.lane_no,a.survey_date::date,a.section_id
                    FROM (select * from access_key where link_id is not null) a
                    left join access_valuelaser b  on a.name_key = b.file_name and round(a.event_str::real/5)*5 = b.chainage::real
                    --where substring(a.link_id,8,4)::int in (128)
                    group by a.event_str,b.chainage,a.event_str,a.event_end,b.lon,b.lat,a.link_id,a.name_key,
                            a.km_start,a.km_end,abs((replace(a.km_start,'+','')::real-replace(a.km_end,'+','')::real)),a.length,
                            a.lane_no,a.survey_date::date,a.section_id
                    order by split_part(a.name_key,'_',2)::int,chainage
                    ) foo --เลือกเฉพาะ chainage str
            order by file_name,chainage
            -- Unioin เลือก ch ในช่วง key_code
            ) foo -- Row ข้อมูล ch จาก union
        where link_id not like '%construction%'
        ) foo -- ดึงข้อมูล access_valuelaser กับ access_key เข้าด้วยกัน และ random iri
        order by split_part(file_name,'_',2)::int,chainage,event_str,event_end
        '''
        cur_step21 = conPG.cursor()
        cur_step21.execute("DROP TABLE IF EXISTS data_suvey")
        cur_step21.execute(merge_csv)
        conPG.commit()

        print(" \u2713 Create table 'data_suvey' successfully")
        print('____________________________________________________________________________________________________________________________________________')

        # STEP_4.1: check_gps_loss
        gps_loss = '''create table gps_lost as
        select 
            a.*, 
            b.chainage, 
            min-6 as new_p_min, 
            max+6 as new_p_max,
            b.status
        from
            (
                --max min ช่วง GPS ที่เริ่มหาย
                select min(chainage) as min, max(chainage) as max, count(*) as c_p, count(*)*5 as meter,
                link_id, date, grp2
                from
                (
                    select chainage, lat, lon, event_str, event_end, link_id, date,
                    row_number() OVER (partition by lat, lon, link_id order by  chainage) as grp1,
                    row_number() over (partition by date, link_id order by  chainage)  -   --(minus)
                    row_number() OVER (partition by lat, lon, link_id order by  chainage) as grp2
                    from data_suvey
                    order by chainage
                ) foo
                where (lon = 0 or lat = 0)
                group by grp2, link_id, date, grp2
                order by min
                --max min ช่วง GPS ที่เริ่มหาย
            ) a,
            (
                select chainage, lat, lon, event_str, event_end, link_id, date , 'มีGPSlost' as status
                from data_suvey
                where split_part((chainage/ CAST(%s AS float))::text, '.', 2) = '' and (lon = 0 or lat = 0)
            ) b
        where chainage between min and max
        group by min, max, c_p, meter, a.link_id, a.date, grp2, b.chainage, new_p_min, new_p_max,b.status
        order by min''' % (interval)
        cur_step22 = conPG.cursor()
        cur_step22.execute("DROP TABLE IF EXISTS gps_lost")
        cur_step22.execute(gps_loss)
        conPG.commit()

        # STEP_4.2: update intervals for searching
        step5 = '''
        update data_suvey a set lat = b.lat , lon = b.lon , the_geom = the_geompoint
        from 
            (select 
                ST_LineInterpolatePoint(the_geom, persent) as the_geompoint,
                st_y(ST_LineInterpolatePoint(the_geom, persent)) as lat,
                st_x(ST_LineInterpolatePoint(the_geom, persent)) as lon, *
            from
                (select 
                    *, p1/p2 as persent --คำนวณ ratio ของจุด ที่จะ gen บนเส้น
                from
                    (select -- จับช่วงข้อมูลที่ gps lost และสร้างเส้น
                        a.chainage as gps_lost_ch, c_p, meter, a.link_id, a.date,
                        min(b.chainage) as chainage_str, max(b.chainage) as chainage_end,
                        (a.chainage- min(b.chainage))::real p1, (max(b.chainage) -min(b.chainage))::real as p2,
                        --(a.chainage- min(b.chainage))::real/ (max(b.chainage) -min(b.chainage))::real as persent,
                        ST_MakeLine(the_geom ORDER BY b.chainage) as the_geom, file_name
                    from gps_lost a, data_suvey b
                    where b.chainage between new_p_min and new_p_max and lon != 0
                        and st_y(the_geom) > 0  and status = 'มีGPSlost'
                    group by gps_lost_ch, c_p, meter, a.link_id, a.date, file_name
                    ) foo -- จับช่วงข้อมูลที่ gps lost และสร้างเส้น
                where p2 > 0 and p1/p2 < 1
                ) foo --คำนวณ ratio ของจุด ที่จะ gen บนเส้น
            ) b
        where a.file_name = b.file_name and a.date = b.date and a.link_id = b.link_id and a.chainage = b.gps_lost_ch
        '''
        cur_step5 = conPG.cursor()
        cur_step5.execute(step5)
        conPG.commit()
        print('\n *** แก้ไขค่า GPS_LOST สำเสร็จ อย่าลืมตรวจสอบใน QGIS')

        # STEP_4.3: update duplicate GPS entries
        step66 = '''
        update data_suvey a set lat = b.lat , lon = b.lon , the_geom = the_geompoint
        from
        (
        select chainage,link_id,file_name,ST_LineInterpolatePoint(the_geom, persent) as the_geompoint,
                st_y(ST_LineInterpolatePoint(the_geom, persent)) as lat,
                st_x(ST_LineInterpolatePoint(the_geom, persent)) as lon
        from
            (select *,p1/p2 as persent
            from
                (select b.chainage,b.link_id,b.grp1,a.min_c,a.max_c,a.file_name,
                    (b.chainage - a.min_c)::real as p1,(a.max_c -a.min_c)::real as p2,a.the_geom
                from
                ---------------------------------------------------
                    (select min_c,max_c,link_id,file_name,st_setsrid(ST_MakeLine(the_geom order by chainage),4326) AS the_geom
                    from
                    (
                        select min_c,max_c,b.chainage,grp1,a.link_id,b.file_name,b.the_geom
                        from
                        (select  min(chainage)-%s as min_c,max(chainage)+%s as max_c,link_id,file_name,grp1
                                from

                                    (select a.chainage,a.link_id,a.lat,a.lon,a.file_name,
                                            DENSE_RANK() over (order by a.the_geom) as grp1,
                                            a.the_geom
                                    from

                                        (select link_id,chainage,lat,lon,file_name,the_geom from
                                                (   select 
                                                    chainage, lat, lon, 
                                                    event_str, event_end, 
                                                    link_id,file_name, date,the_geom
                                                from data_suvey
                                                where split_part((chainage/ CAST(%s AS float))::text, '.', 2) = '' 
                                                    and (lon != 0 or lat != 0)
                                                )foo
                                        order by link_id,chainage,lat,lon,the_geom
                                        ) a
                                    ,
                                        (select min(chainage),max(chainage),link_id,the_geom,count(link_id),file_name from data_suvey
                                        where split_part((chainage/ CAST(%s AS float))::text, '.', 2) = '' and (lon != 0 or lat != 0)
                                        group by link_id,the_geom,file_name
                                        HAVING COUNT(the_geom) > 1
                                        ) b
                                    where a.chainage between b.min and b.max and a.the_geom = b.the_geom) foo
                            group by link_id,grp1,file_name) a
                        left join data_suvey b on b.chainage between min_c and max_c and a.file_name = b.file_name
                        order by a.file_name , b.chainage ) foo
                    group by min_c,max_c,link_id,file_name)a
                ------------------------------------------------------------
                    ,
                    (select chainage,link_id,grp1,file_name,the_geom
                    from
                        (select a.chainage,a.link_id,a.lat,a.lon,a.file_name,
                                DENSE_RANK() over (order by a.the_geom) as grp1,
                                a.the_geom
                        from
                            (select link_id,chainage,file_name,lat,lon,the_geom from
                                    (   select chainage, lat, lon, event_str, event_end, link_id, date,file_name,the_geom
                                        from data_suvey
                                        where split_part((chainage/ CAST(%s AS float))::text, '.', 2) = '' and (lon != 0 or lat != 0)
                                        )foo
                            order by link_id,chainage,lat,lon,the_geom ) a
                        left join
                            (select min(chainage),max(chainage),link_id,the_geom,count(link_id),file_name from data_suvey
                            where split_part((chainage/ CAST(%s AS float))::text, '.', 2) = '' and (lon != 0 or lat != 0)
                            group by link_id,the_geom,file_name
                            HAVING COUNT(the_geom) > 1) b
                        on a.the_geom = b.the_geom and a.file_name = b.file_name
                        where a.the_geom = b.the_geom) foo
                    group by grp1,chainage,link_id,file_name,the_geom) b
                ----------------------------------------------------------------------
                where (b.chainage - a.min_c)::real > 0 and (b.chainage - a.min_c)::real < (a.max_c -a.min_c)::real
                group by b.chainage,b.link_id,b.grp1,a.min_c,a.max_c,a.the_geom,a.file_name)foo
            order by file_name,chainage)foo
        ) b
        where a.file_name = b.file_name and a.chainage = b.chainage ''' % (interval, interval, interval, interval, interval, interval)
        cur_step66 = conPG.cursor()
        cur_step66.execute(step66)
        conPG.commit()
        print(' *** แก้ไขค่า GPS ซ้ำ สำเสร็จ อย่าลืมตรวจสอบใน QGIS')

        # STEP_4.4: update null iri, rutting, texture, and frame_number
        irmp3 = '''
        update data_suvey a set iri = b.iri , rutting = b.rutting, texture = b.texture , frame_number = b.frame_number
        from
            (select
                a.file_name,
                a.raw_chainage,
                avg(b.iri)::real as iri,
                avg(b.rutting)::real as rutting,
                avg(b.texture)::real as texture,
                min(b.frame_number) as frame_number
            from
                (select file_name,link_id,chainage as raw_chainage,(round(chainage/%s)*%s)::real as chainage,  iri,rutting,texture,frame_number
                from data_suvey
                where iri is null or rutting is null or texture is null or frame_number is null
                and split_part((chainage/ CAST(%s AS float))::text, '.', 2) = ''
                order by file_name,chainage) a 
            join 
                (select
                    file_name,link_id,chainage,iri,rutting,texture,frame_number
                from data_suvey
                ) b
            on  (((a.chainage-5) = b.chainage) and a.file_name = b.file_name) or (((a.chainage+5) = b.chainage) and a.file_name = b.file_name)
            where a.file_name = b.file_name
            group by a.file_name,a.raw_chainage) b
        where a.file_name = b.file_name and a.chainage = b.raw_chainage
        ''' % (interval, interval, interval)
        cur_step36 = conPG.cursor()
        cur_step36.execute(irmp3)
        conPG.commit()
        print(' *** แก้ไขค่า iri, rut, mpd, และ pic ที่มีค่าเป็น null สำเสร็จ อย่าลืมตรวจสอบใน QGIS')
        print('____________________________________________________________________________________________________________________________________________')
        print('')

    except Exception as e:
        print(f" \u26A0 An error occurred: {str(e)}")

def preprocessing_2(conPG, interval):
    try:
        print('ตรวจสอบข้อมูลก่อนประมวลผล รอบที่ 2...')
        
        # STEP 4.5: Check iri, rut, mpd, and pic
        irmp = '''
        select file_name,link_id,chainage,iri,rutting,texture,frame_number
            from data_suvey
            where iri is null or rutting is null or texture is null or frame_number is null
            and split_part((chainage/ CAST(%s AS float))::text, '.', 2) = ''
            order by file_name,chainage''' % (interval)
        cur_step34 = conPG.cursor()
        cur_step34.execute(irmp)
        conPG.commit()
        my_table3 = pd.read_sql(irmp, conPG)
        pd.options.display.max_columns = None
        pd.options.display.width = None
        print(' 4.) ตรวจสอบค่า iri, rut, mpd และ pic ว่ามีค่าผิดปกติหรือไม่ ?')
        if my_table3.empty == True:
            print('     \u2713 ตรวจสอบเรียบร้อย :: ไม่มีค่า iri, rut, mpd และ pic ที่ผิดปกติ')
            print('____________________________________________________________________________________________________________________________________________')
        elif my_table3.empty == False:
            print('     \u26A0 แก้ไขค่า : iri rut mpd and pic ว่าผิดปกติหรือไม่ ? ')
            print(my_table3)
            print('____________________________________________________________________________________________________________________________________________')
            # exit()
        print('')

        # STEP 4.6: Check for negative KM
        irmp1 = '''
        select *
        from
            (select 
                link_id,survey_code,
                case when length_chainage > length_km then 'length_chainage มากกว่า'
                         when length_chainage < length_km then 'length_chainage น้อยกว่า' end as status,
                count(*)  as "count_ch_p",
                count(*) * 5 as "count_ch_p*5",
                length_chainage as "length_chainage",
                length_km as "length_km"
            from
                (select     
                    link_id,file_name as survey_code,event_str , event_end ,
                    abs(event_str-event_end) as length_chainage,
                    km_start,
                    km_end,
                    abs(km_start-km_end) as length_km
                from   data_suvey
                where link_id not like '%construction%') foo
            group by link_id,survey_code,length_chainage,length_km
            order by survey_code,link_id) foo
        where status is not null
        '''
        cur_step35 = conPG.cursor()
        cur_step35.execute(irmp)
        conPG.commit()
        my_table4 = pd.read_sql(irmp1, conPG)
        pd.options.display.max_columns = None
        pd.options.display.width = None
        print(' 5.) ตรวจสอบว่ามีค่า KM ที่ติดลบด้วยการสังเกตุ length_chainage และ length_km หรือไม่ ?')
        if my_table4.empty == True:
            print('     \u2713 ตรวจสอบเรียบร้อย :: ไม่มีค่า KM ติดลบ ด้วยการสังเกตุ length_chainage และ length_km')
            print('____________________________________________________________________________________________________________________________________________')
        elif my_table4.empty == False:
            print('     \u26A0 ตรวจสอบพบค่า KM ติดลบ ด้วยการสังเกตุ length_chainage และ length_km ')
            print(my_table4)
            print('____________________________________________________________________________________________________________________________________________')
            # exit()
        print('')

        # STEP 4.8: Check link_id and lane_no consistency
        check8 = '''
        SELECT * FROM
          (SELECT
            name_key,
            link_id,
            SUBSTRING(link_id, 12, 1) AS lane_road,
            SUBSTRING(lane_no, 1, 1) AS lane_no,
            CASE
              WHEN SUBSTRING(link_id, 12, 1) = 'L' AND SUBSTRING(lane_no, 1, 1) = 'L' THEN 'ถูกต้อง'
              WHEN SUBSTRING(link_id, 12, 1) = 'R' AND SUBSTRING(lane_no, 1, 1) = 'R' THEN 'ถูกต้อง'
              WHEN SUBSTRING(link_id, 12, 1) = 'L' AND SUBSTRING(lane_no, 1, 1) = 'R' THEN 'ตรวจสอบเลน'
              WHEN SUBSTRING(link_id, 12, 1) = 'R' AND SUBSTRING(lane_no, 1, 1) = 'L' THEN 'ตรวจสอบเลน'
            END AS status
          FROM access_key
          WHERE SUBSTRING(link_id, 12, 1) IS NOT NULL) foo
        WHERE status != 'ถูกต้อง';
        '''
        cur_check8 = conPG.cursor()
        cur_check8.execute(check8)
        conPG.commit()
        table_check8 = pd.read_sql(check8, conPG)
        pd.options.display.max_columns = None
        pd.options.display.width = None
        print(' 6.) ตรวจสอบ link_id ว่ามีกรณีที่ เลน ขัดกับ lane_no ?' )
        if table_check8.empty == True:
            print('     \u2713 ตรวจสอบเรียบร้อย :: ไม่มี link_id กรณี เลน ขัดกับ lane_no')
            print('____________________________________________________________________________________________________________________________________________')
        elif table_check8.empty == False:
            print('     \u26A0 ตรวจสอบ link_id กรณี เลน ขัดกับ lane_no ไฟล์นั้น ๆ ก่อนนะจ๊ะ !!! ')
            print(table_check8)
            print('____________________________________________________________________________________________________________________________________________')
            # exit()
        print('')

        # STEP 4.9: Check link_id and event consistency
        check9 = '''
        select * from
            (SELECT
              name_key,
              link_id,
              SUBSTRING(link_id, 14, 2) AS type_road,
              event_num,
              event_name,
              CASE
                WHEN SUBSTRING(link_id, 14, 2) = 'AC' AND event_num = 'a' AND event_name = 'ac' THEN 'ถูกต้อง'
                WHEN SUBSTRING(link_id, 14, 2) = 'CC' AND event_num = 'c' AND event_name = 'cc' THEN 'ถูกต้อง'
                WHEN SUBSTRING(link_id, 14, 2) = 'AC' AND event_num = 'c' AND event_name = 'ac' THEN 'ตรวจสอบผิว'
                WHEN SUBSTRING(link_id, 14, 2) = 'AC' AND event_num = 'a' AND event_name = 'cc' THEN 'ตรวจสอบผิว'
                WHEN SUBSTRING(link_id, 14, 2) = 'AC' AND event_num = 'c' AND event_name = 'cc' THEN 'ตรวจสอบผิว'
                WHEN SUBSTRING(link_id, 14, 2) = 'CC' AND event_num = 'c' AND event_name = 'ac' THEN 'ตรวจสอบผิว'
                WHEN SUBSTRING(link_id, 14, 2) = 'CC' AND event_num = 'a' AND event_name = 'cc' THEN 'ตรวจสอบผิว'
                WHEN SUBSTRING(link_id, 14, 2) = 'CC' AND event_num = 'a' AND event_name = 'ac' THEN 'ตรวจสอบผิว'
                ELSE NULL  -- You can change this to a different default value if needed
              end as status
            FROM access_key where SUBSTRING(link_id, 14, 2) is not null )foo where status != 'ถูกต้อง';
        '''
        cur_check9 = conPG.cursor()
        cur_check9.execute(check9)
        conPG.commit()
        table_check9 = pd.read_sql(check9, conPG)
        pd.options.display.max_columns = None
        pd.options.display.width = None
        print(' 7.) ตรวจสอบว่า link_id มีกรณีที่ ผิว ขัดกับ EVENT และ EVENT_DESC ?' )
        if table_check9.empty == True:
            print('     \u2713 ตรวจสอบเรียบร้อย :: ไม่มี link_id ที่ ผิว ขัดกับ EVENT และ EVENT_DESC')
            print('____________________________________________________________________________________________________________________________________________')
        elif table_check9.empty == False:
            print('     \u26A0 ตรวจสอบพบ link_id กรณี ผิว ขัดกับ EVENT และ EVENT_DESC ไฟล์นั้น ๆ ก่อนนะจ๊ะ !!! ')
            print(table_check9)
            print('____________________________________________________________________________________________________________________________________________')
            # exit()
        print('')

    except Exception as e:
        print(f" \u26A0 An error occurred: {str(e)}")

def create_survey_local(conPG, s_id, device, proj, interval):
    try:
        print('Create table "survey_local"...')
        step7 = '''
        CREATE TABLE survey_local AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY split_part(file_name,'_',2)::int,chainage_str::int,link_id)+%s AS survey_id,
            link_id::character(20) AS link_id,
            (split_part(file_name,'_',2)::int)::character(25) AS run_code,
            CASE
                WHEN left(right(link_id,6),2) = 'L1' THEN 1
                WHEN left(right(link_id,6),2) = 'L2' THEN 2
                WHEN left(right(link_id,6),2) = 'L3' THEN 3
                WHEN left(right(link_id,6),2) = 'L4' THEN 4
                WHEN left(right(link_id,6),2) = 'L5' THEN 5
                WHEN left(right(link_id,6),2) = 'L6' THEN 6
                WHEN left(right(link_id,6),2) = 'R1' THEN -1
                WHEN left(right(link_id,6),2) = 'R2' THEN -2
                WHEN left(right(link_id,6),2) = 'R3' THEN -3
                WHEN left(right(link_id,6),2) = 'R4' THEN -4
                WHEN left(right(link_id,6),2) = 'R5' THEN -5
                WHEN left(right(link_id,6),2) = 'R6' THEN -6
            ELSE 0 END AS lane_group,
            right(lane_no,1)::int AS lane_no, 
            km_start, km_end,
            ((abs(km_start::real-km_end::real))/1000)::numeric(8,3) AS length_km,
            ((abs(chainage_str::real-chainage_end::real))/1000)::numeric(8,3) AS distance_odo,
            (st_length(the_geom::geography)/1000)::numeric(8,3) AS distance_gps,
            left(date::text,4)::int AS year,
            CASE
                WHEN left(right(link_id,4),2) = 'AC' THEN 2
                WHEN left(right(link_id,4),2) = 'CC' THEN 1
            END AS survey_type,
            date, the_geom, 'CU_%s'::character(10) AS remark, '%s'::character(10) AS run_new,
            '%s'::int AS interval,
            NULL::text AS ramp_code,
            NULL::text AS name,
            NULL::int AS road_id,
            chainage_str,
            chainage_end,
            file_name
        FROM
            (SELECT -- generate เส้น survey ด้วย ST_MakeLine
                MIN(chainage) AS chainage_str, MAX(chainage) AS chainage_end, link_id, section_id,
                km_start, km_end, length, length_odo ,
                CASE WHEN right(lane_no,1) = 'L' THEN lane_no||'2'
                     WHEN right(lane_no,1) = 'R' THEN lane_no||'2' ELSE lane_no END AS lane_no, date, file_name,
                ST_SetSRID(ST_MakeLine(the_geom ORDER BY file_name,link_id,chainage),4326) AS the_geom
            FROM
                (SELECT --นำ SubQ_a ที่เตรียมไว้มา joint กับ data_suvey เพื่อเอา the_geom
                    a.*  ,b.the_geom
                FROM
                    (SELECT -- Row ข้อมูล chainage เตรียม joint กลับ data_suvey เพื่อเอา geom
                        *
                    FROM
                        (--เอา min max chainage ในช่วงนั้น มา Union
                            SELECT
                                MIN(chainage) AS chainage,link_id,section_id,km_start,km_end,
                                lane_no,date,length,length_odo,file_name
                            FROM data_suvey
                            GROUP BY link_id,section_id,km_start,km_end,
                                lane_no,date,length,length_odo,file_name
                            UNION
                            SELECT
                                MAX(chainage) AS chainage,link_id,section_id,km_start,km_end,
                                lane_no,date,length,length_odo,file_name
                            FROM data_suvey
                            GROUP BY link_id,section_id,km_start,km_end,
                                lane_no,date,length,length_odo,file_name
                            ORDER BY file_name,chainage
                        -- เอา min max chainage ในช่วงนั้น มา Union
                        ) even
                    UNION
                    SELECT -- ดึง chainage ที่หาร interval แล้วลงตัวมาใช้
                        chainage, link_id, section_id, km_start, km_end,
                        lane_no, date,length,length_odo, file_name
                    FROM data_suvey
                    WHERE chainage BETWEEN event_str AND event_end AND (st_x(the_geom) > 0 OR st_y(the_geom) > 0)
                           AND split_part((chainage/ CAST(%s AS float))::text, '.', 2) = ''
                    ORDER BY file_name,chainage
                    -- ดึง chainage ที่หาร interval แล้วลงตัวมาใช้
                ) a -- Row ข้อมูล chainage เตรียม joint กลับ data_suvey เพื่อเอา geom
                LEFT JOIN data_suvey b
                ON a.file_name = b.file_name AND a.link_id = b.link_id AND a.chainage = b.chainage
                ORDER BY a.file_name,a.chainage
            ) foo --นำ SubQ_a ที่เตรียมไว้มา joint กับ data_suvey เพื่อเอา the_geom
            GROUP BY link_id, section_id, km_start, km_end, length, lane_no, date, file_name ,length_odo
        ) foo -- generate เส้น survey ด้วย ST_MakeLine
        ORDER BY split_part(file_name,'_',2)::int,chainage_str::int,link_id
        ''' %(s_id,device,proj,interval,interval)
        cur_step7 = conPG.cursor()
        cur_step7.execute("DROP TABLE IF EXISTS survey_local")
        cur_step7.execute(step7)
        conPG.commit()
        print(" ✓ Create table 'data_suvey' successfully\n")
    except Exception as e:
        print(f" \u26A0 An error occurred: {str(e)}")

def create_survey_local_point(conPG, s_point_id):
    try:
        print('Create table "survey_local_point"...')
        step8 = '''
        CREATE TABLE survey_local_point AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY file_name,chainage)+{} AS survey_point_id,survey_id,order_row,km,
            CASE WHEN lane_group > 0 AND order_row = 0 THEN km_start
                 WHEN lane_group > 0 AND order_row != 0 THEN floor((km+0.1)/interval)*interval   
                 WHEN lane_group < 0 AND order_row = 0 THEN km_start
                 WHEN lane_group < 0 AND order_row != 0 THEN ceil((km-0.1)/interval)*interval
            END km_o,
            CASE WHEN lane_group > 0 AND order_desc = 0 THEN km_end  
                 WHEN lane_group > 0 AND order_desc != 0 THEN ceil((km+0.1)/interval)*interval
                 WHEN lane_group < 0 AND order_desc = 0 THEN km_end   
                 WHEN lane_group < 0 AND order_desc != 0 THEN floor((km-0.1)/interval)*interval 
            END km_d,link_id,
            km_start,km_end,lane_group,lane_no,frame_number,image_name,
            CASE 
               WHEN remark LIKE '%lcms%' THEN 'https://infraplus-ru.org/web-proj/'||run_new||'/'||split_part(remark,'_',2)||'/survey_data_'||split_part(file_name,'_',1)||'/Video/'||file_name||'/ROW-0/'||image_name 
               WHEN remark LIKE '%tpl%' THEN 'https://infraplus-ru.org/web-proj/'||run_new||'/'||split_part(remark,'_',2)||'/survey_data_'||split_part(file_name,'_',1)||'/ROW/'||file_name||'/ROW-0/'||image_name
            END AS url_image,
            iri_left, iri_right, iri, iri_lane, rutt_right, rutt_left, rutting, texture, etd_texture, 
            gn, load, speed, flow, sp, fr60, f60ifi,
            file_name,run_code,chainage,event_str,event_end,the_geom,interval
        FROM 
            (SELECT -- group เป็น ทุก ๆ 25
                survey_id,link_id,
                ROW_NUMBER() OVER (PARTITION BY run_code,event_str,event_end ORDER BY MIN(chainage))-1 AS order_row,
                ROW_NUMBER() OVER (PARTITION BY run_code,event_str,event_end ORDER BY MIN(chainage) DESC)-1  AS order_desc,
                CASE WHEN lane_group > 0 THEN MIN(km) ELSE MAX(km) END AS km,
                km_start,km_end,
                lane_group,lane_no,
                MIN(frame_number)::int AS frame_number,
                file_name||'-ROW-0-'||CASE WHEN LENGTH(MIN(frame_number)::text) = 1 THEN '0000'||MIN(frame_number)
                                             WHEN LENGTH(MIN(frame_number)::text) = 2 THEN '000'||MIN(frame_number)::text
                                             WHEN LENGTH(MIN(frame_number)::text) = 3 THEN '00'||MIN(frame_number)::text
                                             WHEN LENGTH(MIN(frame_number)::text) = 4 THEN '0'||MIN(frame_number)::text
                                             WHEN LENGTH(MIN(frame_number)::text) > 4 THEN MIN(frame_number)::text END||'.jpg' AS image_name,
                file_name,run_code,
                (split_part(ARRAY_TO_STRING(ARRAY_AGG(the_geom ORDER BY run_code,chainage)::text[], '&'),'&',1))::geometry AS the_geom,
                MIN(chainage) AS chainage,interval,event_str,event_end,
                AVG(iri_left)::numeric(8,2) AS iri_left,
                AVG(iri_right)::numeric(8,2) AS iri_right,
                AVG(iri)::numeric(8,2) AS iri ,
                AVG(iri_lane)::numeric(8,2) AS iri_lane,
                AVG(rutt_right)::numeric(8,2) AS rutt_right,
                AVG(rutt_left)::numeric(8,2) AS rutt_left,
                AVG(rutting)::numeric(8,2) AS rutting,
                AVG(texture)::numeric(8,2) AS texture,
                AVG(etd_texture)::numeric(8,2) AS etd_texture,
                NULL::numeric(8,2) AS gn,
                NULL::numeric(8,2) AS load,
                NULL::numeric(8,2) AS speed,
                NULL::numeric(8,2) AS flow,
                NULL::numeric(8,2) AS sp,
                NULL::numeric(8,2) AS fr60,
                NULL::numeric(8,2) AS f60ifi,
                run_new,remark
            FROM
                (SELECT -- group จุด join survey_id 
                    chainage,event_str,event_end,b.link_id, -- chainage
                    km,km_start,km_end, -- กม ที่ run จาก order_row*25
                    order_row,floor((order_row)/(b.interval/5))*(b.interval/5) AS group_km,b.run_code,
                    frame_number,
                    a.file_name,the_geom,b.survey_id,b.lane_group,b.lane_no,b.interval,
                    iri_left,iri_right,iri,iri_lane,rutt_right,rutt_left,rutting,texture,etd_texture,b.run_new,b.remark
                FROM data_suvey a
                LEFT JOIN
                    (SELECT
                        survey_id,link_id,replace(date::text,'-','')||'_'||run_code AS file_name,lane_group,
                        lane_no,interval,run_code::int,remark,run_new
                     FROM survey_local) b
                ON a.link_id = b.link_id AND a.file_name = b.file_name
                ) foo -- group จุด join survey_id 
             GROUP BY survey_id,event_str,event_end,link_id,lane_group,lane_no,
                      group_km,file_name,run_code,km_start,km_end,interval,run_new,remark
             ORDER BY run_code,MIN(chainage)
         ) foo  -- group เป็น ทุก ๆ 25
        ORDER BY run_code,chainage
        ''' .format(s_point_id)
        cur_step8 = conPG.cursor()
        cur_step8.execute("DROP TABLE IF EXISTS survey_local_point")
        cur_step8.execute(step8)
        conPG.commit()
        print(" ✓ Create table 'survey_local_point' successfully\n")
    except Exception as e:
        print(f" \u26A0 An error occurred: {str(e)}")

def create_survey_local_pave(conPG, gid, proj):
    try:
        print("Create table 'survey_local_pave'...")
        step_11 = '''
        CREATE TABLE survey_local_pave AS
        SELECT 	
            ROW_NUMBER() OVER (ORDER BY run_code,chainage_pic)+{} AS gid,
            survey_id,
            link_id,
            event_str AS chainage_str,
            event_end AS chainage_end,
            chainage_pic AS chainage,
            lane_group,
            lane_no,
            km_start,
            km_end,
            CASE WHEN lane_group < 0 THEN km_start-(chainage_pic - event_str::int)
                 WHEN lane_group > 0 THEN km_start+(chainage_pic - event_str::int)
            END AS km,
            SUBSTRING(RIGHT(link_id,4),1,2)::text AS pave_type,
            img_id,
            'cu_survey/{}/'||LEFT(link_id,3)||'/'||SPLIT_PART(filename,'_',1)||'/'||survey_code||'/Run'||run_code||'/pavement/'||filename AS directory,
            filename,
            date AS survey_date,
            survey_code,
            NULL::real AS icrack,
            NULL::real AS ucrack,
            NULL::real AS rav,
            NULL::real AS patch_ac,
            NULL::real AS phole_area,
            NULL::real AS surface_deform,
            NULL::real AS bleeding,
            NULL::real AS edge_break,
            NULL::real AS shoulder_deteriorate_ac,
            NULL::real AS step,
            NULL::real AS transverse_crack,
            NULL::real AS non_transverse_crack,
            NULL::real AS faulting,
            NULL::real AS spalling,
            NULL::real AS corner_break,
            NULL::real AS joint_seal_damage,
            NULL::real AS patch_conc,
            NULL::real AS mpd,
            NULL::real AS shoulder_deteriorate_conc,
            NULL::real AS void,
            NULL::real AS phole_count,
            NULL::real AS scaling_cc,
            the_geom,
            remark,
            CASE 
                WHEN remark LIKE '%lcms%' THEN 'https://infraplus-ru.org/web-proj/'||run_new||'/'||SPLIT_PART(remark,'_',2)||'/survey_data_'||SPLIT_PART(file_name,'_',1)||'/Photo/Resize/Lcms_'||file_name||'/'||filename 
                WHEN remark LIKE '%tpl%'  THEN 'https://infraplus-ru.org/web-proj/'||run_new||'/'||SPLIT_PART(remark,'_',2)||'/survey_data_'||SPLIT_PART(file_name,'_',1)||'/PAVE/'||file_name||'/PAVE-0/'||filename
            END AS url_image
        FROM
            (SELECT -- join data_suvey&survey_local และ ดูดจุดทำ geom
                    DISTINCT
                    b.chainage_pic::real,a.event_str,a.event_end,a.link_id,b.frame_number::int AS img_id,
                    c.date,a.file_name ,survey_code,a.run_code,c.km_start,c.km_end,c.lane_group,c.lane_no,
                    CASE WHEN remark LIKE '%lcms%' 
                        THEN	a.file_name||'_'||CASE WHEN LENGTH((frame_number::int)::text) = 1 THEN '00000'||(frame_number::int+1)::text
                            WHEN LENGTH((frame_number::int)::text) = 2 THEN '0000'||(frame_number::int)::text
                            WHEN LENGTH((frame_number::int)::text) = 3 THEN '000'||(frame_number::int)::text
                            WHEN LENGTH((frame_number::int)::text) = 4 THEN '00'||(frame_number::int)::text
                            WHEN LENGTH((frame_number::int)::text) = 5 THEN '0'||(frame_number::int)::text
                            WHEN LENGTH((frame_number::int)::text) > 5 THEN (frame_number::int)::text END||'.jpg' 
                    WHEN remark LIKE '%tpl%'
                        THEN	a.file_name||'-PAVE-0-'||CASE WHEN LENGTH((frame_number::int)::text) = 1 THEN '0000'||(frame_number::int+1)::text
                            WHEN LENGTH((frame_number::int)::text) = 2 THEN '000'||(frame_number::int)::text
                            WHEN LENGTH((frame_number::int)::text) = 3 THEN '00'||(frame_number::int)::text
                            WHEN LENGTH((frame_number::int)::text) = 4 THEN '0'||(frame_number::int)::text
                            WHEN LENGTH((frame_number::int)::text) > 4 THEN (frame_number::int)::text END||'.jpg' 
            END AS filename,remark,c.survey_id,c.run_new,
            ST_LineInterpolatePoint(c.the_geom,(b.chainage_pic::real-a.event_str)/(a.event_end-a.event_str)) AS the_geom
        FROM
            (SELECT -- ดึงข้อมูล survey_local_point
                ch_o,
                CASE WHEN ch_d IS NULL THEN event_end ELSE ch_d END AS ch_d,
                order_row,event_str,event_end,link_id,km,km_start,km_end,file_name,
                survey_code,run_code
                FROM
                (SELECT 
                    chainage AS ch_o,
                    LEAD(chainage) OVER (PARTITION BY (SPLIT_PART(file_name,'_',2)::int)::CHARACTER(25),link_id ORDER BY chainage) AS ch_d,
                    order_row,
                    event_str,event_end,link_id,km,km_start,km_end,file_name,
                    LEFT(RIGHT(link_id,4),2)||LEFT(link_id,11) AS survey_code,
                    (SPLIT_PART(file_name,'_',2)::INT)::CHARACTER(25) AS run_code
                    FROM survey_local_point
                    ORDER BY (SPLIT_PART(file_name,'_',2)::INT)::CHARACTER(25),event_str,chainage
                ) foo
            ) a -- ดึงข้อมูล survey_local_point
            RIGHT JOIN  access_distress_pic b ON b.chainage_pic::real >= a.ch_o AND b.chainage_pic::real < a.ch_d AND a.file_name = b.name_key
            LEFT JOIN survey_local c ON a.link_id = c.link_id AND a.file_name = c.file_name
            ORDER BY run_code,chainage_pic
        ) foo -- join data_suvey&survey_local และ ดูดจุดทำ geom
        ORDER BY run_code,chainage
        ''' .format(gid, proj)
        cur_step11 = conPG.cursor()
        cur_step11.execute("DROP TABLE IF EXISTS survey_local_pave")
        cur_step11.execute(step_11)
        conPG.commit()

        print(" ✓ Create table 'survey_local_pave' successfully")
        print('____________________________________________________________________________________________________________________________________________\n')
    except Exception as e:
        print(f" \u26A0 An error occurred: {str(e)}")

def dump_table_to_sql(conPG, table_name, path_out):
    try:
        dump_sql = '''
        COPY (
            SELECT dump('public', '{}','true')
        ) TO '{}\\{}.sql';
        '''.format(table_name, path_out, table_name)
        
        cur_dump = conPG.cursor()
        cur_dump.execute(dump_sql)
        conPG.commit()
        
        print(f'✓ Successfully dumped {table_name} table to SQL file\n')
        
    except Exception as e:
        print(f" \u26A0 An error occurred while dumping {table_name} table to SQL file: {str(e)}")

def main_tpl(date_survey, path_direc, path_out, user, password, host, port, database):
    try:
        conPG = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )

        # Set up variables
        path_Data = os.path.join(path_direc, "Data")
        file_dir = Path(path_Data).glob('*.mdb')

        #s_id, s_point_id, gid = get_max_survey_ids(conPG_S24)

        # Process survey data
        access_valuelaser, access_key, access_distress_pic = extract_survey_data(file_dir, path_out, date_survey)

        # Insert data into PostgreSQL
        dataframes = {
            'access_valuelaser': access_valuelaser,
            'access_key': access_key,
            'access_distress_pic': access_distress_pic
        }
        insert_to_postgres(dataframes, user, password, host, port, database)

        # Perform preprocessing steps
        preprocessing_1(conPG)
        process_data_and_update(conPG, interval)
        preprocessing_2(conPG, interval)

        # Generate survey tables
        create_survey_local(conPG, s_id, device, proj, interval)
        create_survey_local_point(conPG, s_point_id)
        create_survey_local_pave(conPG, gid, proj)

        # Dump tables to SQL
        dump_table_to_sql(conPG, 'survey_local', path_out)
        dump_table_to_sql(conPG, 'survey_local_point', path_out)
        dump_table_to_sql(conPG, 'survey_local_pave', path_out)

        # Close connection
        conPG.close()

        print("-------------------------------------------------- รายการเสร็จสิ้น อย่าลืมตรวจสอบใน QGIS !!!!! --------------------------------------------------")

    except Exception as e:
        print(f" \u26A0 Error occurred in main function: {str(e)}")

def iterate_folders(path_file):
    try:
        # Check if the source folder exists
        if not os.path.exists(path_file):
            print(f" \u26A0 Source directory '{path_file}' does not exist.")
            return

        # Check if there are any folders inside the source directory
        folders = [folder for folder in os.listdir(path_file) if os.path.isdir(os.path.join(path_file, folder))]
        if not folders:
            print(f" \u26A0 No folders found inside '{path_file}'.")
            return

        # Iterate over the folders
        for folder in folders:
            if folder.startswith("survey_data_"):
                date_survey = folder.split("_")[2]  # Extracting date_survey from folder name
                path_direc = os.path.join(path_file, folder)
                path_out = os.path.join(path_direc, "Output")

                # Create output directory
                if not os.path.exists(path_out):
                    os.makedirs(path_out)
                else:
                    shutil.rmtree(path_out)
                    os.makedirs(path_out)

                # Process survey data for each folder
                if device.lower() == "tpl":
                    main_tpl(date_survey, path_direc, path_out, user, password, host, port, database)

                print("############################################################################################################################################\n\n")

    except FileNotFoundError:
        print(f" \u26A0 Directory '{path_file}' not found.")
    except Exception as e:
        print(f" \u26A0 Error occurred while iterating folders: {str(e)}")

def redirect_output_to_text_widget(widget):
    class TextRedirector:
        def __init__(self, widget):
            self.widget = widget

        def write(self, text):
            self.widget.insert(tk.END, text)

    sys.stdout = TextRedirector(widget)

def run_script():
    global source_folder, proj, interval, device 
    folder_path = source_folder
    project_name = proj
    
    if device.upper() == "TPL":
        iterate_folders(source_folder)
    elif device == "LCMS (2)":
        print("LCMS(2)")
    elif device == "LCMS (4)":
        print("LCMS(4)")
    else:
        print("Invalid device selection")
        return

if __name__ == "__main__":
    
    # Create the main window
    root = tk.Tk()
    root.title("QC tools (TPL/LCMS)")

    # File path entry
    file_path_label = ttk.Label(root, text="Folder Path:")
    file_path_label.grid(row=0, column=0, sticky="w")
    file_path_entry = ttk.Entry(root, width=150)
    file_path_entry.grid(row=0, column=1, padx=5, pady=5)

    # Browse button
    browse_button = ttk.Button(root, text="Browse", command=browse_folder)
    browse_button.grid(row=0, column=2, padx=5, pady=5)

    # Project name entry
    project_name_label = ttk.Label(root, text="Project Name:")
    project_name_label.grid(row=1, column=0, sticky="w")
    project_name_entry = ttk.Entry(root, width=150)
    project_name_entry.grid(row=1, column=1, padx=5, pady=5)
    project_name_entry.bind("<Return>", set_project_name)  # Bind the Return key to set_project_name function

    # Interval dropdown
    interval_label = ttk.Label(root, text="Interval:")
    interval_label.grid(row=2, column=0, sticky="w")
    interval_var = tk.IntVar()
    interval_dropdown = ttk.Combobox(root, textvariable=interval_var, values=[10, 25, 50, 100], width=147)
    interval_dropdown.grid(row=2, column=1, padx=5, pady=5)

    # Set default value to 25
    interval_dropdown.current(1)  # 25 is at index 1 in the values list
    interval_dropdown.bind("<<ComboboxSelected>>", set_interval)  # Bind the selection event to set_interval function

    # Devices dropdown
    device_label = ttk.Label(root, text="Devices:")
    device_label.grid(row=3, column=0, sticky="w")
    device_var = tk.StringVar()
    devices = ["TPL", "LCMS (2)", "LCMS (4)"]
    device_dropdown = ttk.Combobox(root, textvariable=device_var, values=devices, width=147)
    device_dropdown.grid(row=3, column=1, padx=5, pady=5)
    device_dropdown.bind("<<ComboboxSelected>>", set_device)  # Bind the selection event to set_device function

    try:
        s_id = 1
        s_point_id = 1
        gid = 1

        # Access environment variables
        host = os.getenv("HOST")
        database = os.getenv("DATABASE")
        #database_s24 = os.getenv("DATABASE_S24")
        user = os.getenv("USER")
        password = os.getenv("PASSWORD")
        port = os.getenv("PORT")

        try:
            conPG = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port
            )
            
        except psycopg2.Error as e:
            print(" \u26A0 Unable to connect to the database:", e)

    except Exception as e:
        print(f" \u26A0 Error occurred in __main__: {str(e)}")

    # Run button
    run_button = ttk.Button(root, text="Run", command=run_script)
    run_button.grid(row=4, column=1, pady=10)

    # Text widget to display output
    result_text = tk.Text(root, wrap="word", width=140, height=30)
    result_text.grid(row=5, columnspan=3, padx=5, pady=5)

    # Redirect print output to the Text widget
    redirect_output_to_text_widget(result_text)

    root.mainloop()
