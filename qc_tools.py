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
import matplotlib.pyplot as plt
import mplcursors
from matplotlib.widgets import CheckButtons

# Load environment variables from .env file
load_dotenv()

source_folder = ""
proj = ""  
interval = 25 
device = ""
database = proj.lower()

def query_table(sql_query, conPG):
    cursor = conPG.cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    cursor.close()
    #conPG.close()
    return rows

def browse_folder():
    global source_folder 
    folder_path = filedialog.askdirectory()
    file_path_entry.delete(0, tk.END)
    file_path_entry.insert(0, folder_path)
    source_folder = folder_path 
    print("source_folder:", source_folder) 

def set_project_name(event=None):
    global proj
    proj = project_name_var.get().upper()
    print("Project Name:", proj) 

def set_interval(event=None):
    global interval
    interval = interval_var.get()
    print("Interval:", interval) 

def set_device(event=None):
    global device
    device = device_var.get().lower()
    print("Device Name:", device.upper()) 

def extract_survey_data_tpl(file_dir, path_out, date_survey):
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
            cre_key = '''
                SELECT 
                    CHAINAGE_START AS event_str, 
                    CHAINAGE_END AS event_end, 
                    EVENT AS event_num,
                    SWITCH_GROUP AS event_type,  
                    EVENT_DESC AS event_name,
                    link_id, 
                    section_id, 
                    km_start, 
                    km_end, 
                    length,
                    lane_no, 
                    survey_date,
                    LATITUDE_START AS lat_str, 
                    LATITUDE_END AS lat_end, 
                    LONGITUDE_START AS lon_str,
                    LONGITUDE_END AS lon_end, 
                    '%s' AS name_key,
                    Val(Mid('%s', InStr('%s', '_') + 1)) AS run_code
                FROM 
                    KeyCode_Raw_%s
                WHERE 
                    link_id <> ''
                ''' % (a,a,a,a)
            access_key = access_key.append(pd.read_sql_query(cre_key, conn), ignore_index=True)

            # Fetching Data for access_pave
            cre_pave = '''
                SELECT 
                    INT(CHAINAGE) AS chainage_pic, 
                    FRAME + 1 AS frame_number, 
                    b.EVENT_DESC AS event_name,
                    '%s' AS name_key,
                    Val(Mid('%s', InStr('%s', '_') + 1)) AS run_code
                FROM 
                    Video_Processed_%s_2 AS a
                LEFT JOIN 
                    KeyCode_Raw_%s AS b ON a.CHAINAGE >= INT(b.CHAINAGE_START) AND a.CHAINAGE < INT(b.CHAINAGE_END)
                WHERE 
                    b.EVENT_DESC IS NOT NULL
                ''' % (a,a,a,a,a)
            access_distress_pic = access_distress_pic.append(pd.read_sql_query(cre_pave, conn), ignore_index=True)

            if graph_var.get():
                print(f'Processing on: {base}\n \u2713 ตรวจสอบค่า MAX, MIN, MEAN ของ IRI, RUT, TEXTURE จากกราฟ ว่าผิดปกติหรือไม่ ?' )
                plot_and_show_statistics(access_valuelaser, base, proj, device)
                print('____________________________________________________________________________________________________________________________________________')

        valuelaser_file = rf"{path_out}\access_valuelaser.csv"
        key_file = rf"{path_out}\access_key.csv"
        distress_pic_file = rf"{path_out}\access_distress_pic.csv"

        access_valuelaser.to_csv(valuelaser_file, index=False)
        access_key.to_csv(key_file, index=False)
        access_distress_pic.to_csv(distress_pic_file, index=False)

        print(f" \u2713 Survey data processing completed. Files generated: \n \u2713 {valuelaser_file}\n \u2713 {key_file}\n \u2713 {distress_pic_file}\n")

        return access_valuelaser, access_key, access_distress_pic

    except Exception as e:
        print(f" \U0001F6AB Error occurred while processing survey data: {str(e)}")

def extract_survey_data_lcms(file_dir, path_Data, path_out, date_survey):
    try:
        print("Processing survey data",date_survey, "...")
        # Set up warnings filter
        warnings.filterwarnings("ignore")

        # Initialize dataframes
        df_keycode = pd.DataFrame()
        df_valua_ac = pd.DataFrame()
        df_crack_ac = pd.DataFrame()
        df_valua_cc = pd.DataFrame()
        df_crack_cc = pd.DataFrame()

        for file in file_dir:
            # Process each file
            path_mdb = str(file)
            base = os.path.splitext(os.path.basename(path_mdb))[0]
            b = base.split("_edit")[0]

            # Establish connection using pyodbc
            pyodbc.lowercase = False
            conn = pyodbc.connect(
                r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
                r"Dbq=%s;" % (path_mdb))

            # Fetching Data for access_key
            cre_key = '''
            SELECT CHAINAGE_START AS event_str, CHAINAGE_END AS event_end, 
            EVENT AS event_num,SWITCH_GROUP AS event_type,  
            EVENT_DESC as  event_name,
            link_id, section_id, km_start, km_end, length,lane_no, 
            survey_date,LATITUDE_START AS lat_str, LATITUDE_END AS lat_end, 
            LONGITUDE_START AS lon_str,LONGITUDE_END AS lon_end, '%s' AS name_key
            FROM KeyCode_Raw_%s
            WHERE link_id <> '';
                    ''' % (b, b)
            df_keycode = df_keycode.append(pd.read_sql_query(cre_key, conn), ignore_index=True)

            # Fetching Data access_valuelaser_ac (IRI RUT MPD)
            cre_valuelaser_ac = '''
                    SELECT chainage,LONGITUDE as lon,LATITUDE as lat,iri_right, iri_left,iri, iri_lane,rutt_right,rutt_left,rutting,
                        texture , etd_texture,'%s' as name_key, 'ac' as event_name
                    FROM
                        (SELECT
                        CHAINAGE ,LONGITUDE,LATITUDE,
                        iri_right, iri_left,iri, iri_lane,
                        rut2 as rutt_right,rut3 as rutt_left,rut4 as rutting
                        FROM
                            (SELECT a.CHAINAGE,a.LONGITUDE,a.LATITUDE,
                                        iri2 as iri_right, iri3 as iri_left, iri4 as iri,iri5 as iri_lane
                            FROM GPS_Processed_%s AS a
                                LEFT JOIN
                                (SELECT (ROUND(CHAINAGE/5))*5 as iri1,AVG(RWP_IRI) as iri2 ,AVG(LWP_IRI) as iri3 ,(((AVG(RWP_IRI))+(AVG(LWP_IRI)))/2) as iri4 ,AVG(LANE_IRI) as iri5
                                FROM LCMS_Rough_Processed_%s
                                GROUP BY (ROUND(CHAINAGE/5))*5) AS b ON a.CHAINAGE = b.iri1) a
                                LEFT JOIN
                                (SELECT (ROUND(CHAINAGE/5))*5 as rut1,MAX(RIGHT_DEPTH) as rut2 ,MAX(LEFT_DEPTH) as rut3 ,
                                    IIf(Max(RIGHT_DEPTH) = Max(LEFT_DEPTH), Max(RIGHT_DEPTH),
                                    IIf(Max(RIGHT_DEPTH) > Max(LEFT_DEPTH), Max(RIGHT_DEPTH), Max(LEFT_DEPTH))) AS rut4
                                FROM LCMS_Rut_Processed_%s
                                GROUP BY (ROUND(CHAINAGE/5))*5) AS b ON a.CHAINAGE = b.rut1) a
                                LEFT JOIN
                                (
                                SELECT mpd1,(((AVG(mpd2))+(AVG(mpd3)))/2) AS texture,
                                            ((((AVG(mpd2))+(AVG(mpd3)))/2)*0.8)+0.008 as etd_texture
                                FROM
                                (SELECT
                                    CHAINAGE,
                                    (ROUND(CHAINAGE/5))*5 as mpd1,
                                    IIf(MPD_BAND_2 < 0 AND MPD_BAND_2  <> -1, ABS(MPD_BAND_2),
                                        IIf(MPD_BAND_2 = -1, 0,
                                            IIf(MPD_BAND_2 > 0, MPD_BAND_2, NULL))) AS mpd2,
                                    IIf(MPD_BAND_4 < 0 AND MPD_BAND_4  <> -1, ABS(MPD_BAND_4),
                                        IIf(MPD_BAND_4 = -1, 0,
                                            IIf(MPD_BAND_4 > 0, MPD_BAND_4, NULL))) AS mpd3
                                FROM LCMS_Texture_Processed_%s)
                                GROUP BY mpd1) AS b on a.CHAINAGE = b.mpd1
                    ''' % (b, b, b, b, b)
            df_valua_ac = df_valua_ac.append(pd.read_sql_query(cre_valuelaser_ac, conn), ignore_index=True)

            # Fetching Data access_valuelaser_ac (IRI RUT MPD)
            cre_valuelaser_cc = '''
                    SELECT chainage,LONGITUDE as lon,LATITUDE as lat,iri_right, iri_left,iri, iri_lane,rutt_right,rutt_left,rutting,
                        texture , etd_texture,'%s' as name_key, 'ac' as event_name
                    FROM
                        (SELECT
                        CHAINAGE ,LONGITUDE,LATITUDE,
                        iri_right, iri_left,iri, iri_lane,
                        rut2 as rutt_right,rut3 as rutt_left,rut4 as rutting
                        FROM
                            (SELECT a.CHAINAGE,a.LONGITUDE,a.LATITUDE,
                                        iri2 as iri_right, iri3 as iri_left, iri4 as iri,iri5 as iri_lane
                            FROM GPS_Processed_%s AS a
                                LEFT JOIN
                                (SELECT (ROUND(CHAINAGE/5))*5 as iri1,AVG(RWP_IRI) as iri2 ,AVG(LWP_IRI) as iri3 ,(((AVG(RWP_IRI))+(AVG(LWP_IRI)))/2) as iri4 ,AVG(LANE_IRI) as iri5
                                FROM LCMS_Rough_Processed_%s
                                GROUP BY (ROUND(CHAINAGE/5))*5) AS b ON a.CHAINAGE = b.iri1) a
                                LEFT JOIN
                                (SELECT (ROUND(CHAINAGE/5))*5 as rut1,MAX(RIGHT_DEPTH) as rut2 ,MAX(LEFT_DEPTH) as rut3 ,
                                    IIf(Max(RIGHT_DEPTH) = Max(LEFT_DEPTH), Max(RIGHT_DEPTH),
                                    IIf(Max(RIGHT_DEPTH) > Max(LEFT_DEPTH), Max(RIGHT_DEPTH), Max(LEFT_DEPTH))) AS rut4
                                FROM LCMS_Rut_Processed_%s
                                GROUP BY (ROUND(CHAINAGE/5))*5) AS b ON a.CHAINAGE = b.rut1) a
                                LEFT JOIN
                                (
                                SELECT mpd1,(((AVG(mpd2))+(AVG(mpd3)))/2) AS texture,
                                            ((((AVG(mpd2))+(AVG(mpd3)))/2)*0.8)+0.008 as etd_texture
                                FROM
                                (SELECT
                                    CHAINAGE,
                                    (ROUND(CHAINAGE/5))*5 as mpd1,
                                    IIf(MPD_BAND_2 < 0 AND MPD_BAND_2  <> -1, ABS(MPD_BAND_2),
                                        IIf(MPD_BAND_2 = -1, 0,
                                            IIf(MPD_BAND_2 > 0, MPD_BAND_2, NULL))) AS mpd2,
                                    IIf(MPD_BAND_4 < 0 AND MPD_BAND_4  <> -1, ABS(MPD_BAND_4),
                                        IIf(MPD_BAND_4 = -1, 0,
                                            IIf(MPD_BAND_4 > 0, MPD_BAND_4, NULL))) AS mpd3
                                FROM LCMS_Texture_Processed_%s)
                                GROUP BY mpd1) AS b on a.CHAINAGE = b.mpd1
                    ''' % (b, b, b, b, b)
            df_valua_cc = df_valua_cc.append(pd.read_sql_query(cre_valuelaser_cc, conn), ignore_index=True)

            # Fetching Data access_valuelaser_ac (CRACK)
            cre_crack_ac = '''
                        SELECT
                        CHAINAGE,LENGTH as length_crack, AREA as area_crack , CLASSIFICATION as class,
                        SEVERITY as sev,imgae_file , '%s' as name_key, 'ac' as event_name
                        FROM

                        (SELECT CHAINAGE , (ROUND(CHAINAGE/5))*5 ,LENGTH, AREA ,CLASSIFICATION,SEVERITY,
                        IIf(InStr(1, IMAGE_FILE_INDEX, '+') > 0,
                                Left(IMAGE_FILE_INDEX, InStr(1, IMAGE_FILE_INDEX, '+') - 1),
                                IMAGE_FILE_INDEX) as imgae_file
                        FROM LCMS_Crack_Processed_%s
                        ORDER BY CHAINAGE) AS a
                        ''' % (b, b)
            df_crack_ac = df_crack_ac.append(pd.read_sql_query(cre_crack_ac, conn), ignore_index=True)

            # Fetching Data access_valuelaser_ac (CRACK)
            cre_crack_cc = '''
                    SELECT
                        CHAINAGE,LENGTH as length_crack, AREA as area_crack , CLASSIFICATION as class,
                        SEVERITY as sev,imgae_file , '%s' as name_key, 'cc' as event_name
                    FROM

                    (SELECT CHAINAGE , (ROUND(CHAINAGE/5))*5 ,LENGTH, AREA ,CLASSIFICATION,SEVERITY,
                    IIf(InStr(1, IMAGE_FILE_INDEX, '+') > 0,
                            Left(IMAGE_FILE_INDEX, InStr(1, IMAGE_FILE_INDEX, '+') - 1),
                            IMAGE_FILE_INDEX) as imgae_file
                    FROM LCMS_Crack_Processed_%s
                    ORDER BY CHAINAGE) AS a
                    ''' % (b, b)
            df_crack_cc = df_crack_cc.append(pd.read_sql_query(cre_crack_cc, conn), ignore_index=True)
            conn.close()
        else:
            data = {
                'chainage': [],
                'lon': [],
                'lat': [],
                'iri_right': [],
                'iri_left': [],
                'iri': [],
                'iri_lane': [],
                'rutt_right': [],
                'rutt_left': [],
                'rutting': [],
                'texture': [],
                'etd_texture': [],
                'name_key': [],
                'event_name': []
            }
            df_valua_ac = df_valua_ac.append(pd.DataFrame(data), ignore_index=True)
            df_valua_cc = df_valua_cc.append(pd.DataFrame(data), ignore_index=True)

            data = {
                'CHAINAGE': [],
                'length_crack': [],
                'area_crack': [],
                'class': [],
                'sev': [],
                'imgae_file': [],
                'name_key': [],
                'event_name': []
            }
            df_crack_ac = df_crack_ac.append(pd.DataFrame(data), ignore_index=True)
            df_crack_cc = df_crack_cc.append(pd.DataFrame(data), ignore_index=True)

        print(f" \u2713 Get value IRI and CRACK of 'AC' and 'CC' completed.")

        # Get select value Distress_AC 
        path_data_dis_ac = r'%s\distress' % (path_Data)
        file_dir_dis_ac = Path(path_data_dis_ac).glob('*.mdb')

        df_ble_ac = pd.DataFrame()
        df_pot_ac = pd.DataFrame()
        df_rav_ac = pd.DataFrame()
        df_pic_ac = pd.DataFrame()
        df_rav_cc = pd.DataFrame()
        df_pic_cc = pd.DataFrame()

        for file in file_dir_dis_ac:
            path_mdb = str(file)
            base = os.path.splitext(os.path.basename(path_mdb))[0]
            b = base.split("_edit")[0]

            # Establish connection using pyodbc
            pyodbc.lowercase = False
            conn = pyodbc.connect(
                r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
                r"Dbq=%s;" % (path_mdb))
            
            # Fetching bleeding_AC
            cre_bleeding = '''
                SELECT Chainage,BI_LEFT,BI_RIGHT,SEVERITY_LEFT,SEVERITY_RIGHT, '%s' as name_key , 'ac' as event_name,IIf(InStr(1, IMAGE_FILE_INDEX, '+') > 0,
                            Left(IMAGE_FILE_INDEX, InStr(1, IMAGE_FILE_INDEX, '+') - 1),
                            IMAGE_FILE_INDEX) as imgae_file
                FROM LCMS_Bleeding_Processed_%s
                ''' % (b, b)
            df_ble_ac = df_ble_ac.append(pd.read_sql_query(cre_bleeding, conn), ignore_index=True)

            # Fetching Pothole_AC 
            cre_pothole = '''
                    SELECT
                        *
                    FROM
                    (SELECT  CHAINAGE,AREA,(AREA/AREA) as count,SEVERITY,IIf(InStr(1, IMAGE_FILE_INDEX, '+') > 0,
                                        Left(IMAGE_FILE_INDEX, InStr(1, IMAGE_FILE_INDEX, '+') - 1),
                                        IMAGE_FILE_INDEX) as imgae_file, '%s' as name_key , 'ac' as event_name
                    FROM LCMS_Potholes_Processed_%s
                    ) as A
                    ''' % (b, b)
            df_pot_ac = df_pot_ac.append(pd.read_sql_query(cre_pothole, conn), ignore_index=True)

            # Fetching RAV_AC
            cre_rav_ac = '''
            SELECT CHAINAGE,RI ,HIGH_RI_AREA_M2,MEDIUM_RI_AREA_M2,LOW_RI_AREA_M2,IIf(InStr(1, IMAGE_FILE_INDEX, '+') > 0,
                                Left(IMAGE_FILE_INDEX, InStr(1, IMAGE_FILE_INDEX, '+') - 1),
                                IMAGE_FILE_INDEX) as imgae_file, '%s' as name_key , 'ac' as event_name
            FROM LCMS_Raveling_Processed_%s
                    ''' % (b, b)
            df_rav_ac = df_rav_ac.append(pd.read_sql_query(cre_rav_ac, conn), ignore_index=True)

            # Fetching RAV_CC
            cre_rav_cc = '''
            SELECT CHAINAGE,RI ,HIGH_RI_AREA_M2,MEDIUM_RI_AREA_M2,LOW_RI_AREA_M2,IIf(InStr(1, IMAGE_FILE_INDEX, '+') > 0,
                                Left(IMAGE_FILE_INDEX, InStr(1, IMAGE_FILE_INDEX, '+') - 1),
                                IMAGE_FILE_INDEX) as imgae_file, '%s' as name_key , 'cc' as event_name
            FROM LCMS_Raveling_Processed_%s
                    ''' % (b, b)
            df_rav_cc = df_rav_cc.append(pd.read_sql_query(cre_rav_cc, conn), ignore_index=True)

            # Fetching PICTURE_AC
            cre_pic_ac = '''
            SELECT (ROUND(CHAINAGE/5))*5 as ch_pic,FRAME,'%s' as name_key, 'ac' as event_name
            FROM Video_Processed_%s_1
                    ''' % (b, b)
            df_pic_ac = df_pic_ac.append(pd.read_sql_query(cre_pic_ac, conn), ignore_index=True)

            # Fetching PICTURE_CC
            cre_pic_cc = '''
            SELECT (ROUND(CHAINAGE/5))*5 as ch_pic,FRAME,'%s' as name_key, 'cc' as event_name
            FROM Video_Processed_%s_1
                    ''' % (b, b)
            df_pic_cc = df_pic_cc.append(pd.read_sql_query(cre_pic_cc, conn), ignore_index=True)
            conn.close()
        else:
            data = {
                'Chainage': [],
                'BI_LEFT': [],
                'BI_RIGHT': [],
                'SEVERITY_LEFT': [],
                'SEVERITY_RIGHT': [],
                'name_key': [],
                'event_name': [],
                'imgae_file': []
            }
            df_ble_ac = df_ble_ac.append(pd.DataFrame(data), ignore_index=True)

            data = {
                'CHAINAGE': [],
                'AREA': [],
                'count': [],
                'SEVERITY': [],
                'imgae_file': [],
                'name_key': [],
                'event_name': []
            }
            df_pot_ac = df_pot_ac.append(pd.DataFrame(data), ignore_index=True)

            data = {
                'CHAINAGE': [],
                'RI': [],
                'HIGH_RI_AREA_M2': [],
                'MEDIUM_RI_AREA_M2': [],
                'LOW_RI_AREA_M2': [],
                'imgae_file': [],
                'name_key': [],
                'event_name': []
            }
            df_rav_ac = df_rav_ac.append(pd.DataFrame(data), ignore_index=True)
            df_rav_cc = df_rav_cc.append(pd.DataFrame(data), ignore_index=True)

            data = {
                'ch_pic': [],
                'FRAME': [],
                'name_key': [],
                'event_name': []
            }
            df_pic_ac = df_pic_ac.append(pd.DataFrame(data), ignore_index=True)
            df_pic_cc = df_pic_cc.append(pd.DataFrame(data), ignore_index=True)

        print(f" \u2713 Get value Distress of 'AC' and 'CC' completed.")

        # AC from mdb
        df_keycode_ac = df_keycode[(df_keycode['event_name'] == 'ac') | (df_keycode['event_name'].str[0] == 'a')]
        df_valua_ac = pd.DataFrame(df_valua_ac)
        df_crack_ac = pd.DataFrame(df_crack_ac)
        df_ble_ac = pd.DataFrame(df_ble_ac)
        df_pot_ac = pd.DataFrame(df_pot_ac)
        df_rav_ac = pd.DataFrame(df_rav_ac)
        df_pic_ac = pd.DataFrame(df_pic_ac)

        # CC from mdb
        df_keycode_cc = df_keycode[(df_keycode['event_name'] == 'cc') | (df_keycode['event_name'].str[0] == 'c')]
        df_valua_cc = pd.DataFrame(df_valua_cc)
        df_crack_cc = pd.DataFrame(df_crack_cc)
        df_rav_cc  = pd.DataFrame(df_rav_cc)
        df_pic_cc  = pd.DataFrame(df_pic_cc)

        env = {'df_keycode_ac': df_keycode_ac,
            'df_valua_ac': df_valua_ac,
            'df_crack_ac': df_crack_ac,
            'df_ble_ac': df_ble_ac,
            'df_pot_ac': df_pot_ac,
            'df_rav_ac': df_rav_ac,
            'df_pic_ac': df_pic_ac,
            'df_keycode_cc': df_keycode_cc,
            'df_valua_cc': df_valua_cc,
            'df_crack_cc': df_crack_cc,
            'df_rav_cc': df_rav_cc,
            'df_pic_cc': df_pic_cc
            }

        # Querry df_access_valuelaser
        access_valuelaser = pd.DataFrame()
        df_access_valuelaser = """
                SELECT
                    a.chainage, a.lon, a.lat,
                    iri_right, iri_left, iri, iri_lane,
                    rutt_right, rutt_left, rutting,
                    texture, etd_texture, a.event_name,
                    c.FRAME as frame_number,
                    a.name_key as file_name,
                    CAST(substr(a.name_key, instr(a.name_key, '_') + 1) AS INTEGER) as run_code
                FROM df_valua_ac a
                JOIN df_keycode_ac b ON (a.chainage BETWEEN b.event_str AND b.event_end) AND (a.name_key = b.name_key)
                LEFT JOIN df_pic_ac c ON (c.ch_pic = a.chainage) AND (c.name_key = b.name_key)

                UNION

                SELECT
                    a.chainage, a.lon, a.lat,
                    iri_right, iri_left, iri, iri_lane,
                    rutt_right, rutt_left, rutting,
                    texture, etd_texture, a.event_name,
                    c.FRAME as frame_number,
                    a.name_key as file_name,
                    CAST(substr(a.name_key, instr(a.name_key, '_') + 1) AS INTEGER) as run_code
                FROM df_valua_cc a
                JOIN df_keycode_cc b ON (a.chainage BETWEEN b.event_str AND b.event_end) AND (a.name_key = b.name_key)
                LEFT JOIN df_pic_cc c ON (c.ch_pic = a.chainage) AND (c.name_key = b.name_key)
                ORDER BY run_code,chainage
                """
        df = sqldf(df_access_valuelaser, env)
        access_valuelaser = df.append(access_valuelaser, ignore_index=True)
        access_valuelaser.to_csv("%s\\access_valuelaser.csv" % (path_out), index=False )
        print(f" \u2713 *** Querry df_access_valuelaser Successfully")

        # Querry access_key
        access_key = pd.DataFrame()
        df_access_key = """
            SELECT
                event_str,event_end,event_num,event_type,
                event_name,link_id, section_id, km_start, km_end, length,
                lane_no,survey_date,lat_str,lat_end,lon_str,lon_end,name_key,
                CAST(substr(name_key, instr(name_key, '_') + 1) AS INTEGER) as run_code
            FROM df_keycode_ac
            Union
            SELECT
                event_str,event_end,event_num,event_type,
                event_name,link_id, section_id, km_start, km_end, length,
                lane_no,survey_date,lat_str,lat_end,lon_str,lon_end,name_key,
                CAST(substr(name_key, instr(name_key, '_') + 1) AS INTEGER) as run_code
            FROM df_keycode_cc
            order by run_code
        """
        df = sqldf(df_access_key, env)
        access_key = df.append(access_key, ignore_index=True)
        access_key = access_key.rename(columns=lambda x: x.lower())
        access_key.to_csv("%s\\access_key.csv" % (path_out), index=False )
        print(f" \u2713 *** Querry df_access_key Successfully")

        # Querry access_distress_pic
        access_distress_pic = pd.DataFrame()
        df_access_pave = """
            SELECT
                chainage_pic,imgae_file as frame_number,event_name,name_key,run_code
            FROM
                    (SELECT
                        a.CHAINAGE,
                        ROUND(a.CHAINAGE/5.0 - 0.5)*5 as chainage_pic,
                        CAST(SUBSTR(a.imgae_file, INSTR(a.imgae_file, '_') + 1) AS INTEGER) AS imgae_file,
                        a.event_name,
                        a.name_key,
                        CAST(substr(a.name_key, instr(a.name_key, '_') + 1) AS INTEGER) as run_code
                    FROM df_rav_ac a
                JOIN df_keycode_ac b ON (a.CHAINAGE BETWEEN b.event_str AND b.event_end) AND (a.name_key = b.name_key) )
            GROUP BY chainage_pic,frame_number,event_name,name_key,run_code

            UNION

            SELECT
                chainage_pic,imgae_file as frame_number,event_name,name_key,run_code
            FROM
                    (SELECT
                        a.CHAINAGE,
                        ROUND(a.CHAINAGE/5.0 - 0.5)*5 as chainage_pic,
                        CAST(SUBSTR(a.imgae_file, INSTR(a.imgae_file, '_') + 1) AS INTEGER) AS imgae_file,
                        a.event_name,
                        a.name_key,
                        CAST(substr(a.name_key, instr(a.name_key, '_') + 1) AS INTEGER) as run_code
                    FROM df_rav_cc a
                    JOIN df_keycode_cc b ON (a.CHAINAGE BETWEEN b.event_str AND b.event_end) AND (a.name_key = b.name_key) )
            GROUP BY chainage_pic,frame_number,event_name,name_key,run_code
            ORDER BY run_code,chainage_pic
        """
        df = sqldf(df_access_pave, env)
        access_distress_pic = df.append(access_distress_pic, ignore_index=True)
        access_distress_pic = access_distress_pic.rename(columns=lambda x: x.lower())
        access_distress_pic.to_csv("%s\\access_distress_pic.csv" % (path_out), index=False )
        print(f" \u2713 *** Querry df_access_distress_pic Successfully")

        # Querry access_crack
        access_crack = pd.DataFrame()
        df_access_crack = """
                SELECT
                    a.chainage,
                    a.length_crack,
                    a.area_crack,
                    a.class,
                    a.sev,
                    a.imgae_file,
                    a.name_key,
                    a.event_name,
                    CAST(substr(a.name_key, instr(a.name_key, '_') + 1) AS INTEGER) as run_code
                FROM df_crack_ac a
                JOIN df_keycode_ac b ON (a.chainage BETWEEN b.event_str AND b.event_end) AND (a.name_key = b.name_key)

                Union

                SELECT
                    a.chainage,
                    a.length_crack,
                    a.area_crack,
                    a.class,
                    a.sev,
                    a.imgae_file,
                    a.name_key,
                    a.event_name,
                    CAST(substr(a.name_key, instr(a.name_key, '_') + 1) AS INTEGER) as run_code
                FROM df_crack_cc a
                JOIN df_keycode_cc b ON (a.chainage BETWEEN b.event_str AND b.event_end) AND (a.name_key = b.name_key)
                ORDER BY run_code
        """
        df = sqldf(df_access_crack, env)
        access_crack = df.append(access_crack, ignore_index=True)
        access_crack = access_crack.rename(columns=lambda x: x.lower())
        access_crack.to_csv("%s\\access_crack.csv" % (path_out), index=False )
        print(f" \u2713 *** Querry df_access_crack Successfully")

        # Querry access_pothole
        access_pothole = pd.DataFrame()
        df_access_pothole = """
                SELECT
                    a.chainage,
                    a.area,
                    a.count,
                    a.SEVERITY as sev,
                    a.imgae_file ,
                    a.name_key,
                    a.event_name,
                    CAST(substr(a.name_key, instr(a.name_key, '_') + 1) AS INTEGER) as run_code
                FROM df_pot_ac a
                JOIN df_keycode_ac b ON (a.chainage BETWEEN b.event_str AND b.event_end) AND (a.name_key = b.name_key)
        """
        df = sqldf(df_access_pothole, env)
        access_pothole = df.append(access_pothole, ignore_index=True)
        access_pothole = access_pothole.rename(columns=lambda x: x.lower())
        access_pothole.to_csv("%s\\access_pothole.csv" % (path_out), index=False )
        print(f" \u2713 *** Querry df_access_pothole Successfully")

        # Querry access_bleeding
        access_bleeding = pd.DataFrame()
        df_access_bleeding = """
                SELECT
                    a.chainage,
                    a.BI_LEFT as ble_left,
                    a.BI_RIGHT as ble_right,
                    a.SEVERITY_LEFT as sev_left,
                    a.SEVERITY_RIGHT as sev_right,
                    a.imgae_file ,
                    a.name_key,
                    a.event_name,
                    CAST(substr(a.name_key, instr(a.name_key, '_') + 1) AS INTEGER) as run_code
                FROM df_ble_ac a
                JOIN df_keycode_ac b ON (a.chainage BETWEEN b.event_str AND b.event_end) AND (a.name_key = b.name_key)
                ORDER BY run_code
        """
        df = sqldf(df_access_bleeding, env)
        access_bleeding = df.append(access_bleeding, ignore_index=True)
        access_bleeding = access_bleeding.rename(columns=lambda x: x.lower())
        access_bleeding.to_csv("%s\\access_bleeding.csv" % (path_out), index=False )
        print(f" \u2713 *** Querry df_access_bleeding Successfully")

        # Querry access_rav
        access_rav = pd.DataFrame()
        df_access_rav = """
                SELECT
                    a.chainage,
                    a.RI as ri,
                    a.HIGH_RI_AREA_M2 as high_area,
                    a.MEDIUM_RI_AREA_M2 as medium_area,
                    a.LOW_RI_AREA_M2 as low_area,
                    a.imgae_file ,
                    a.name_key,
                    a.event_name,
                    CAST(substr(a.name_key, instr(a.name_key, '_') + 1) AS INTEGER) as run_code
                FROM df_rav_ac a
                JOIN df_keycode_ac b ON (a.chainage BETWEEN b.event_str AND b.event_end) AND (a.name_key = b.name_key)

                Union

                SELECT
                    a.chainage,
                    a.RI as ri,
                    a.HIGH_RI_AREA_M2 as high_area,
                    a.MEDIUM_RI_AREA_M2 as medium_area,
                    a.LOW_RI_AREA_M2 as low_area,
                    a.imgae_file ,
                    a.name_key,
                    a.event_name,
                    CAST(substr(a.name_key, instr(a.name_key, '_') + 1) AS INTEGER) as run_code
                FROM df_rav_cc a
                JOIN df_keycode_cc b ON (a.chainage BETWEEN b.event_str AND b.event_end) AND (a.name_key = b.name_key)
                ORDER BY run_code
        """
        df = sqldf(df_access_rav, env)
        access_rav = df.append(access_rav, ignore_index=True)
        access_rav = access_rav.rename(columns=lambda x: x.lower())
        access_rav.to_csv("%s\\access_rav.csv" % (path_out), index=False )
        print(f" \u2713 *** Querry df_access_rav Successfully")
        
        print(f" \u2713 Survey data processing completed.\n   Files generated: \n   \u2713 {path_out}\\access_valuelaser.csv\n   \u2713 {path_out}\\access_key.csv\n   \u2713 {path_out}\\access_distress_pic.csv\n   \u2713 {path_out}\\access_crack.csv\n   \u2713 {path_out}\\access_pothole.csv\n   \u2713 {path_out}\\access_bleeding.csv\n   \u2713 {path_out}\\access_rav.csv\n")

        if graph_var.get():
            print(f'Processing on: {base}\n \u2713 ตรวจสอบค่า MAX, MIN, MEAN ของ IRI, RUT, TEXTURE จากกราฟ ว่าผิดปกติหรือไม่ ?' )
            plot_and_show_statistics(access_valuelaser, base, proj, device)
            print('____________________________________________________________________________________________________________________________________________')

        return access_valuelaser, access_key, access_distress_pic, access_crack, \
            access_pothole, access_bleeding, access_rav
        
    except Exception as e:
        print(f" \U0001F6AB Error occurred while processing survey data: {str(e)}")

def insert_to_postgres(dataframes_tpl, dataframes_lcms, user, password, host, port, database):
    try:
        print("Inserting data into PostgreSQL...")
        print(f" \u2713 Connecting to PostgreSQL database: {database} on host: {host} port: {port}...") 
        # Establish connection to PostgreSQL database
        encoded_password = quote_plus(password)

        # Establish connection to PostgreSQL database
        engine = create_engine(
            f"postgresql://{user}:{encoded_password}@{host}:{port}/{database}"
        )

        conPG = engine.connect()

        if device.upper() == "TPL":
            dataframes = dataframes_tpl
            
        if device.upper() == "LCMS":
            dataframes = dataframes_lcms
            
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
        print(f" \U0001F6AB Error occurred while inserting data into PostgreSQL: {str(e)}")

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
            print(f'     \U0001F6AB แก้ไข LINK_ID ว่ามีซ้ำกันในวันสำรวจเดียวกันหรือไม่ ก่อนนะจ๊ะ !!! \n{table_check2}')
            print('____________________________________________________________________________________________________________________________________________')
            return
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
            print(f'     \U0001F6AB แก้ไข Link_ID ที่ข้อมูล KM_START และ KM_END มีค่า GPS_LOST ก่อนนะจ๊ะ !!! \n{table_check3}')
            print('____________________________________________________________________________________________________________________________________________')
            #return
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
            print(f'     \U0001F6AB แก้ไข link_id กรณีขา R และขา L ขัดกับ km_start และ km_end ก่อนนะจ๊ะ !!! \n{table_check4}')
            print('____________________________________________________________________________________________________________________________________________')
            #return

    except psycopg2.Error as e:
        print(" \U0001F6AB Error occurred while checking data before processing:", e)

def process_data_and_update(conPG, interval):
    try:    
        # STEP_4: create table 'data_suvey'
        print("\nCreate table 'data_suvey'...")
        merge_csv = '''
        CREATE TABLE  data_suvey as
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
            order by min''' %(interval)
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
                                                    (select 
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
                                        (	select chainage, lat, lon, event_str, event_end, link_id, date,file_name,the_geom
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
            where a.file_name = b.file_name and a.chainage = b.chainage 
            ''' %(interval,interval,interval,interval,interval,interval)
        cur_step66 = conPG.cursor()
        cur_step66.execute(step66)
        conPG.commit()
        print(' *** แก้ไขค่า GPS ซ้ำ สำเสร็จ อย่าลืมตรวจสอบใน QGIS')

        # STEP_4.4: update null iri, rutting, texture, and frame_number
        irmp3 = '''
            UPDATE data_suvey a 
            SET 
                iri = b.iri,
                rutting = b.rutting,
                texture = b.texture,
                frame_number = b.frame_number
            FROM (
                SELECT 
                    a.file_name,
                    a.raw_chainage,
                    avg(b.iri)::REAL AS iri,
                    avg(b.rutting)::REAL AS rutting,
                    avg(b.texture)::REAL AS texture,
                    min(b.frame_number) AS frame_number
                FROM (
                    SELECT 
                        file_name,
                        link_id,
                        chainage AS raw_chainage,
                        (ROUND(chainage / %s) * %s)::REAL AS chainage,
                        iri,
                        rutting,
                        texture,
                        frame_number
                    FROM 
                        data_suvey
                    WHERE 
                        (iri IS NULL OR rutting IS NULL OR texture IS NULL OR frame_number IS NULL)
                        AND split_part((chainage / CAST(%s AS FLOAT))::TEXT, '.', 2) = ''
                    ORDER BY 
                        file_name,
                        chainage
                ) a
                JOIN (
                    SELECT 
                        file_name,
                        link_id,
                        chainage,
                        iri,
                        rutting,
                        texture,
                        frame_number
                    FROM 
                        data_suvey
                ) b ON (((a.chainage - 5) = b.chainage AND a.file_name = b.file_name) OR ((a.chainage + 5) = b.chainage AND a.file_name = b.file_name))
                WHERE 
                    a.file_name = b.file_name
                GROUP BY 
                    a.file_name,
                    a.raw_chainage
            ) b
            WHERE 
                a.file_name = b.file_name AND a.chainage = b.raw_chainage;
            ''' %(interval,interval,interval)
        cur_step36 = conPG.cursor()
        cur_step36.execute(irmp3)
        conPG.commit()
        print(' *** แก้ไขค่า iri, rut, mpd, และ pic ที่มีค่าเป็น null สำเสร็จ อย่าลืมตรวจสอบใน QGIS')
        print('____________________________________________________________________________________________________________________________________________')
        print('')

    except Exception as e:
        print(f" \U0001F6AB An error occurred: {str(e)}")

def preprocessing_2(conPG, interval):
    try:
        print('ตรวจสอบข้อมูลก่อนประมวลผล รอบที่ 2...')
        
        # STEP 4.5: Check iri, rut, mpd, and pic
        irmp = '''
            SELECT 
                file_name,
                link_id,
                chainage,
                iri,
                rutting,
                texture,
                frame_number
            FROM 
                data_suvey
            WHERE 
                iri IS NULL OR rutting IS NULL OR texture IS NULL OR frame_number IS NULL
                AND split_part((chainage / CAST(%s AS float))::TEXT, '.', 2) = ''
            ORDER BY 
                file_name,
                chainage
            ''' % (interval)
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
            print(f'     \U0001F6AB แก้ไขค่า : iri rut mpd and pic ว่าผิดปกติหรือไม่ ? \n{my_table3}')
            print('____________________________________________________________________________________________________________________________________________')
            #return
        print('')

        # STEP 4.6: Check for negative KM
        irmp1 = '''
            SELECT *
            FROM (
                SELECT 
                    link_id,
                    survey_code,
                    CASE 
                        WHEN length_chainage > length_km THEN 'length_chainage มากกว่า'
                        WHEN length_chainage < length_km THEN 'length_chainage น้อยกว่า'
                    END AS status,
                    COUNT(*) AS count_ch_p,
                    COUNT(*) * 5 AS count_ch_p_times_5,
                    length_chainage AS length_chainage,
                    length_km AS length_km
                FROM (
                    SELECT 	
                        link_id,
                        file_name AS survey_code,
                        event_str,
                        event_end,
                        ABS(event_str - event_end) AS length_chainage,
                        km_start,
                        km_end,
                        ABS(km_start - km_end) AS length_km
                    FROM data_suvey
                    WHERE link_id NOT LIKE '%construction%'
                ) AS foo
                GROUP BY link_id, survey_code, length_chainage, length_km
                ORDER BY survey_code, link_id
            ) AS bar
            WHERE status IS NOT NULL
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
            print(f'     \u26A0 ตรวจสอบพบค่า KM ติดลบ ด้วยการสังเกตุ length_chainage และ length_km \n{my_table4}')
            print('____________________________________________________________________________________________________________________________________________')
            # return
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
            print(f'     \U0001F6AB ตรวจสอบ link_id กรณี เลน ขัดกับ lane_no ไฟล์นั้น ๆ ก่อนนะจ๊ะ !!! \n{table_check8}')
            print('____________________________________________________________________________________________________________________________________________')
            #return
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
            print(f'     \U0001F6AB ตรวจสอบพบ link_id กรณี ผิว ขัดกับ EVENT และ EVENT_DESC ไฟล์นั้น ๆ ก่อนนะจ๊ะ !!! \n{table_check9}')
            print('____________________________________________________________________________________________________________________________________________')
            #return
        print('')

    except Exception as e:
        print(f" \U0001F6AB An error occurred: {str(e)}")

def plot_and_show_statistics(data, base, proj, device):
    data = data.sort_values(by='chainage')
    
    # Create a figure with two subplots: one for the main plot and one for the statistics table
    fig, (ax_main, ax_stats) = plt.subplots(nrows=2, figsize=(10, 7), gridspec_kw={'height_ratios': [4, 1]})
    
    # Plot the data on the main subplot
    line_iri, = ax_main.plot(data['chainage'], data['iri'], label='IRI')
    line_rutting, = ax_main.plot(data['chainage'], data['rutting'], label='Rutting')
    line_texture, = ax_main.plot(data['chainage'], data['texture'], label='Texture')
    ax_main.set_xlabel('Chainage')
    ax_main.set_ylabel('Value')
    ax_main.set_title(f'Survey Data on {base} ({proj}-{device})')
    ax_main.grid(True)

    # Add cursor annotations
    cursor = mplcursors.cursor([line_iri, line_rutting, line_texture], hover=True)
    cursor.connect("add", lambda sel: sel.annotation.set_text(
        f"Chainage: {sel.target[0]:.2f}"
    ))

    # Display statistics
    max_iri = data['iri'].max()
    min_iri = data['iri'].min()
    mean_iri = data['iri'].mean()
    max_rutting = data['rutting'].max()
    min_rutting = data['rutting'].min()
    mean_rutting = data['rutting'].mean()
    max_texture = data['texture'].max()
    min_texture = data['texture'].min()
    mean_texture = data['texture'].mean()
    
    # Create a table for statistics on the second subplot
    table_data = [
        ["Parameter", "Max", "Min", "Mean"],
        ["IRI", f"{max_iri:.2f}", f"{min_iri:.2f}", f"{mean_iri:.2f}"],
        ["Rutting", f"{max_rutting:.2f}", f"{min_rutting:.2f}", f"{mean_rutting:.2f}"],
        ["Texture", f"{max_texture:.2f}", f"{min_texture:.2f}", f"{mean_texture:.2f}"]
    ]
    ax_stats.axis('off')  # Turn off axis for the stats table
    ax_stats.table(cellText=table_data, loc='center', cellLoc='center', colWidths=[0.2, 0.1, 0.1, 0.1])
    
    # Create checkboxes for each line
    lines = [line_iri, line_rutting, line_texture]
    labels = ['IRI', 'Rutting', 'Texture']
    
    # Create an Axes instance for checkboxes and position it
    ax_checkbox = fig.add_axes([0.85, 0.8, 0.1, 0.1])  
    checkboxes = CheckButtons(ax_checkbox, labels, [True] * len(lines))

    # Modify the checkbox rectangles
    for rect, line in zip(checkboxes.rectangles, lines):
        rect.set_facecolor(line.get_color())  
        rect.set_edgecolor(line.get_color())   
        rect.set_linewidth(1.5)
        rect.set_width(0.13)  
        rect.set_height(0.15)

    # Function to handle checkbox toggles
    def toggle_line(label):
        index = labels.index(label)
        lines[index].set_visible(not lines[index].get_visible())
        # Update y-axis limits based on visible data
        visible_data = [line.get_ydata() for line in lines if line.get_visible()]
        if visible_data:
            min_y = min(min(data) for data in visible_data)
            max_y = max(max(data) for data in visible_data)
            ax_main.set_ylim(min_y, max_y)
        plt.draw()
    
    checkboxes.on_clicked(toggle_line)
    
    # Adjust layout to prevent overlap
    plt.tight_layout()
    
    # Show the plot
    plt.show()

def create_survey_local(conPG, s_id, device, proj, interval):
    try:
        print('Create table "survey_local"...')
        step7 = '''
            create table survey_local as
            select
                row_number() over (order by split_part(file_name,'_',2)::int,chainage_str::int,link_id)+%s as survey_id,
                link_id::character(20) as link_id,
                (split_part(file_name,'_',2)::int)::character(25) as run_code,
                case
                    when left(right(link_id,6),2) = 'L1' then 1
                    when left(right(link_id,6),2) = 'L2' then 2
                    when left(right(link_id,6),2) = 'L3' then 3
                    when left(right(link_id,6),2) = 'L4' then 4
                    when left(right(link_id,6),2) = 'L5' then 5
                    when left(right(link_id,6),2) = 'L6' then 6
                    when left(right(link_id,6),2) = 'R1' then -1
                    when left(right(link_id,6),2) = 'R2' then -2
                    when left(right(link_id,6),2) = 'R3' then -3
                    when left(right(link_id,6),2) = 'R4' then -4
                    when left(right(link_id,6),2) = 'R5' then -5
                    when left(right(link_id,6),2) = 'R6' then -6
                else 0	end lane_group, 	---lane_group ==> 1=L , -1=R, 2=FL, -2=FR, 3=IL, -3 = IR, 4 = UL, -4 = UR, 5=BL,  -5 = BR, 6 = TL, -6 = TR
                right(lane_no,1)::int as lane_no, 
                km_start,km_end,
                ((abs(km_start::real-km_end::real))/1000)::numeric(8,3)  as length_km,
                ((abs(chainage_str::real-chainage_end::real))/1000)::numeric(8,3) as distance_odo,
                (st_length(the_geom::geography)/1000)::numeric(8,3) as distance_gps, left(date::text,4)::int as year,
                case	when left(right(link_id,4),2) = 'AC' then 2
                        when left(right(link_id,4),2) = 'CC' then 1
                end survey_type, 						---1= CC , 2 =AC
                date, the_geom, 'CU_%s'::character(10) as remark, '%s'::character(10) as run_new,
                '%s'::int as interval,
                null::text as ramp_code,
                null::text as name,
                null::int as road_id,
                chainage_str,
                chainage_end,
                file_name, section_id
            from
                (select -- generate เส้น survey ด้วย ST_MakeLine
                    min(chainage) as chainage_str, max(chainage) as chainage_end, link_id, section_id,
                    km_start, km_end, length, length_odo ,
                    case when right(lane_no,1) = 'L' then lane_no||'2'
                        when right(lane_no,1) = 'R' then lane_no||'2' else lane_no end lane_no, date, file_name,
                    st_setsrid(ST_MakeLine(the_geom ORDER BY file_name,link_id,chainage),4326) AS the_geom
                from
                    (select --นำ SubQ_a ที่เตรียมไว้มา joint กับ data_suvey เพื่อเอา the_geom
                        a.*  ,b.the_geom
                    from
                        (select -- Row ข้อมูล chainage เตรียม joint กลับ data_suvey เพื่อเอา geom
                        *
                        from
                            (--เอา min max chainage ในช่วงนั้น มา Union
                                select
                                    min(chainage) as chainage,link_id,section_id,km_start,km_end,
                                    lane_no,date,length,length_odo,file_name
                                from data_suvey
                                group by link_id,section_id,km_start,km_end,
                                    lane_no,date,length,length_odo,file_name
                                union
                                select
                                    max(chainage) as chainage,link_id,section_id,km_start,km_end,
                                    lane_no,date,length,length_odo,file_name
                                from data_suvey
                                group by link_id,section_id,km_start,km_end,
                                    lane_no,date,length,length_odo,file_name
                                order by file_name,chainage
                            -- เอา min max chainage ในช่วงนั้น มา Union
                            ) even
                        union
                        select -- ดึง chainage ที่หาร interval แล้วลงตัวมาใช้
                            chainage, link_id, section_id, km_start, km_end,
                            lane_no, date,length,length_odo, file_name
                        from data_suvey
                        where chainage between event_str and event_end and (st_x(the_geom) > 0 or st_y(the_geom) > 0)
                            and split_part((chainage/ CAST(%s AS float))::text, '.', 2) = ''
                        order by file_name,chainage
                        -- ดึง chainage ที่หาร interval แล้วลงตัวมาใช้
                        ) a -- Row ข้อมูล chainage เตรียม joint กลับ data_suvey เพื่อเอา geom
                    left join data_suvey b
                    on a.file_name = b.file_name and a.link_id = b.link_id and a.chainage = b.chainage
                    order by a.file_name,a.chainage
                    ) foo --นำ SubQ_a ที่เตรียมไว้มา joint กับ data_suvey เพื่อเอา the_geom
                    group by link_id, section_id, km_start, km_end, length, lane_no, date, file_name ,length_odo
                ) foo -- generate เส้น survey ด้วย ST_MakeLine
            order by split_part(file_name,'_',2)::int,chainage_str::int,link_id
            ''' %(s_id,device,proj,interval,interval)
        cur_step7 = conPG.cursor()
        cur_step7.execute("DROP TABLE IF EXISTS survey_local")
        cur_step7.execute(step7)
        conPG.commit()
        print(" ✓ Create table 'data_suvey' successfully\n")
    except Exception as e:
        print(f" \U0001F6AB An error occurred: {str(e)}")

def create_survey_local_point(conPG, s_point_id, path_direc):
    try:
        print('Create table "survey_local_point"...')
        step8 = '''
            create table survey_local_point as
            select
                row_number() over (order by file_name,chainage)+{} as survey_point_id,survey_id,order_row,km,
                case when lane_group > 0 and order_row   = 0 then km_start
                    when lane_group > 0 and order_row  != 0 then floor((km+0.1)/interval)*interval   
                    when lane_group < 0 and order_row   = 0 then km_start
                    when lane_group < 0 and order_row  != 0 then ceil((km-0.1)/interval)*interval
                end km_o,
                case when lane_group > 0 and order_desc  = 0 then km_end  
                    when lane_group > 0 and order_desc != 0 then ceil((km+0.1)/interval)*interval
                    when lane_group < 0 and order_desc  = 0 then km_end   
                    when lane_group < 0 and order_desc != 0 then floor((km-0.1)/interval)*interval 
                end km_d,link_id,
                km_start,km_end,lane_group,lane_no,frame_number,image_name,
                case 
                when remark like '%lcms%' then 'https://infraplus-ru.org/web-proj/'||run_new||'/'||split_part(remark,'_',2)||'/survey_data_'||split_part(file_name,'_',1)||'/Video/'||file_name||'/ROW-0/'||image_name 
                when remark like '%tpl%'  then 'https://infraplus-ru.org/web-proj/'||run_new||'/'||split_part(remark,'_',2)||'/survey_data_'||split_part(file_name,'_',1)||'/ROW/'||file_name||'/ROW-0/'||image_name
                end as url_image,
                iri_left, iri_right, iri, iri_lane, rutt_right, rutt_left, rutting, texture, etd_texture, 
                gn, load, speed, flow, sp, fr60, f60ifi,
                file_name,run_code,chainage,event_str,event_end,the_geom,interval
            from 
                (select -- group เป็น ทุก ๆ 25
                    survey_id,link_id,
                    row_number() over (partition by run_code,event_str,event_end order by min(chainage))-1 as order_row,
                    row_number() over (partition by run_code,event_str,event_end order by min(chainage) desc)-1  as order_desc,
                    case when lane_group > 0 then min(km) else max(km) end as km,
                    km_start,km_end,
                    lane_group,lane_no,
                    min(frame_number)::int as frame_number,
                    file_name||'-ROW-0-'||case when length(min(frame_number)::text) = 1 then '0000'||min(frame_number)
                    when length(min(frame_number)::text) = 2 then '000'||min(frame_number)::text
                    when length(min(frame_number)::text) = 3 then '00'||min(frame_number)::text
                    when length(min(frame_number)::text) = 4 then '0'||min(frame_number)::text
                    when length(min(frame_number)::text) > 4 then min(frame_number)::text end||'.jpg' as image_name,
                    file_name,run_code,
                    (split_part(ARRAY_TO_STRING(ARRAY_AGG(the_geom ORDER BY run_code,chainage)::text[], '&'),'&',1))::geometry as the_geom,
                    min(chainage) as chainage,interval,event_str,event_end,
                    avg(iri_left)::numeric(8,2) as iri_left,
                    avg(iri_right)::numeric(8,2) as iri_right,
                    avg(iri)::numeric(8,2) as iri ,
                    avg(iri_lane)::numeric(8,2) as iri_lane,
                    avg(rutt_right)::numeric(8,2) as rutt_right,
                    avg(rutt_left)::numeric(8,2) as rutt_left,
                    avg(rutting)::numeric(8,2) as rutting,
                    avg(texture)::numeric(8,2) as texture,
                    avg(etd_texture)::numeric(8,2) as etd_texture,
                    null::numeric(8,2) as gn,
                    null::numeric(8,2) as load,
                    null::numeric(8,2) as speed,
                    null::numeric(8,2) as flow,
                    null::numeric(8,2) as sp,
                    null::numeric(8,2) as fr60,
                    null::numeric(8,2) as f60ifi,
                    run_new,remark
                from
                    (select -- group จุด join survey_id 
                        chainage,event_str,event_end,b.link_id, -- chainage
                        km,km_start,km_end, -- กม ที่ run จาก order_row*25
                        order_row,floor((order_row)/(b.interval/5))*(b.interval/5) as group_km,b.run_code,
                        frame_number,
                        a.file_name,the_geom,b.survey_id,b.lane_group,b.lane_no,b.interval,
                        iri_left,iri_right,iri,iri_lane,rutt_right,rutt_left,rutting,texture,etd_texture,b.run_new,b.remark
                    from data_suvey a
                    left join
                        (select
                            survey_id,link_id,replace(date::text,'-','')||'_'||run_code as file_name,lane_group,
                            lane_no,interval,run_code::int,remark,run_new
                        from survey_local) b
                    on a.link_id = b.link_id and a.file_name = b.file_name
                    ) foo -- group จุด join survey_id 
                group by survey_id,event_str,event_end,link_id,lane_group,lane_no,
                        group_km,file_name,run_code,km_start,km_end,interval,run_new,remark
                order by run_code,min(chainage)
                ) foo  -- group เป็น ทุก ๆ 25
            order by run_code,chainage
            ''' .format(s_point_id)
        cur_step8 = conPG.cursor()
        cur_step8.execute("DROP TABLE IF EXISTS survey_local_point")
        cur_step8.execute(step8)
        conPG.commit()
        print(" ✓ Create table 'survey_local_point' successfully\n")
    except Exception as e:
        print(f" \U0001F6AB An error occurred: {str(e)}")

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
        print(f" \U0001F6AB An error occurred: {str(e)}")

def update_distrass(conPG):
    try:
        print("Update table 'survey_local_pave'...")
        # 1. icrack
        icrack = '''
            update survey_local_pave a set icrack = b.icrack
            from
                (select
                    chainage,
                    sum(icrack) as icrack,
                    event_name,run_code,imgae_file
                from
                    (select
                        (floor(chainage::real/5)*5)::int as chainage,
                        area_crack::real as icrack,
                        event_name,
                        run_code::int,
                        imgae_file,
                        class
                    from access_crack
                    where class like '%Region%' and event_name = 'ac'
                    group by (floor(chainage::real/5)*5)::int,event_name,run_code,class,imgae_file,area_crack
                    order by run_code::int,(floor(chainage::real/5)*5)::int)foo
                group by chainage,event_name,run_code,imgae_file
                order by run_code,chainage) b
            where a.chainage = b.chainage and split_part(a.filename,'_',2)::int = b.run_code
            '''
        cur_icrack = conPG.cursor()
        cur_icrack.execute(icrack)
        conPG.commit()

        # 2. ucrack
        ucrack = '''
            update survey_local_pave a set ucrack = b.ucrack
            from
                (select
                    *
                from
                    (select
                        chainage,
                        (sum(ucrack)::numeric(8,2))::real as ucrack,
                        STRING_AGG(class,',') as class,
                        event_name,run_code,imgae_file
                    from
                        (select
                            (floor(chainage::real/5)*5)::int as chainage,
                            case when event_name = 'ac' then sum((length_crack::real)/1000)::real else 0 end as ucrack,
                            event_name,
                            run_code::int,
                            imgae_file,
                            class
                        from access_crack
                        where event_name = 'ac' and class not like '%Region%'
                        group by (floor(chainage::real/5)*5)::int,event_name,run_code,class,imgae_file,sev
                        order by run_code::int,(floor(chainage::real/5)*5)::int
                        ) foo
                    group by chainage,event_name,run_code,imgae_file
                    order by run_code,chainage
                    ) foo
                where class not like '%Region%') b
            where LOWER(a.pave_type) = b.event_name and a.chainage = b.chainage and split_part(a.filename,'_',2)::int = b.run_code and icrack is null
            '''
        cur_ucrack = conPG.cursor()
        cur_ucrack.execute(ucrack)
        conPG.commit()

        # 3. tranversal/non
        tranversal = '''
            update survey_local_pave a set transverse_crack = b.transverse_crack , non_transverse_crack=b.non_transverse_crack
            from
                (select
                    *
                from
                    (select
                        chainage,
                        (sum(transverse_crack)::numeric(8,2))::real as transverse_crack,
                        (sum(non_transverse_crack)::numeric(8,2))::real as non_transverse_crack,
                        STRING_AGG(class,',') as class,
                        event_name,run_code,imgae_file
                    from
                        (select
                            (floor(chainage::real/5)*5)::int as chainage,
                            case when event_name = 'cc' and (class like '%Transversel%' or class like '%Multiple%') then  sum((length_crack::double precision)/1000)::real else 0 end as transverse_crack,
                            case when event_name = 'cc' and class like '%Longitudinal%' 	then  sum((length_crack::double precision)/1000)::real else 0 end as non_transverse_crack,
                            event_name,
                            run_code::int,
                            imgae_file,
                            class
                        from access_crack
                        where event_name = 'cc' and (class not like '%Alligator%' and class not like '%Unclassified%' )  and sev not like 'Very Weak'
                        group by (floor(chainage::real/5)*5)::int,event_name,run_code,class,imgae_file,sev
                        order by run_code::int,(floor(chainage::real/5)*5)::int
                        ) foo
                    group by chainage,event_name,run_code,imgae_file
                    order by run_code,chainage
                    ) foo
                where class not like '%Alligator%') b
            where LOWER(a.pave_type) = b.event_name and a.chainage = b.chainage and split_part(a.filename,'_',2)::int = b.run_code
            '''
        cur_tranversal = conPG.cursor()
        cur_tranversal.execute(tranversal)
        conPG.commit()

        # 4. rav
        rav = '''
        update survey_local_pave a set rav = b.rav 
        from  (
            select
                (floor(chainage::real/5)*5)::int as chainage,
                case when event_name = 'ac' then  sum((high_area::real)+(medium_area::real)) else 0 end as rav,
                case when event_name = 'cc' then  sum((high_area::real)+(medium_area::real)) else 0 end as rav_cc,
                event_name,
                run_code::int,
                imgae_file
            from access_rav
            group by (floor(chainage::real/5)*5)::int,event_name,run_code,imgae_file
            order by run_code::int,(floor(chainage::real/5)*5)::int
            ) b
        where LOWER(a.pave_type) = b.event_name and a.chainage = b.chainage and split_part(a.filename,'_',2)::int = b.run_code
        '''
        cur_rav = conPG.cursor()
        cur_rav.execute(rav)
        conPG.commit()

        # 5. pothole
        pothole = '''
            update survey_local_pave a set phole_area = b.phole_area , spalling = b.spalling, phole_count = b.phole_count
            from  (
                select
                    (floor(chainage::real/5)*5)::int as chainage,
                    case when event_name = 'ac' then  sum(area::real) else 0 end as phole_area,
                    case when event_name = 'cc' then  sum(area::real) else 0 end as spalling,
                    case when event_name = 'ac' then  (sum((area::real)/(area::real)))::int else 0 end as phole_count,
                    event_name,
                    run_code::int,
                    imgae_file,
                    sev
                from access_pothole
                where sev like '%Moderate%' or sev like '%High%'
                group by (floor(chainage::real/5)*5)::int,event_name,run_code,imgae_file,sev
                order by run_code::int,(floor(chainage::real/5)*5)::int
                ) b
            where LOWER(a.pave_type) = b.event_name and a.chainage = b.chainage and split_part(a.filename,'_',2)::int = b.run_code
            '''
        cur_pothole = conPG.cursor()
        cur_pothole.execute(pothole)
        conPG.commit()

        # 6. bleeding
        bleeding = '''
            update survey_local_pave a set bleeding = b.bleeding
            from  (
                    select
                        chainage,sum(bleeding::real)::real as bleeding,event_name,run_code,imgae_file
                    from
                        (select
                            (floor(chainage::real/5)*5)::int as chainage,
                            case when sev_left like '%No Bleeding%' 	and sev_right like '%No Bleeding%' 		then 0
                                when sev_left like '%Light Bleeding%' 	and sev_right like '%Light Bleeding%'	then 0
                                when sev_left like '%High Bleeding%' 	and sev_right like '%High Bleeding%'	then 1
                                when sev_left like '%Medium Bleeding%' and sev_right like '%Medium Bleeding%'	then 1
                                when sev_left like '%High Bleeding%' 	and sev_right like '%Medium Bleeding%'	then 1
                                when sev_left like '%Medium Bleeding%' and sev_right like '%High Bleeding%'	then 1
                            else 0.5
                            end as bleeding,sev_left,sev_right,
                            event_name,
                            run_code::int,
                            imgae_file
                        from access_bleeding
                        order by run_code::int,(floor(chainage::real/5)*5)::int
                        ) foo
                    group by chainage,event_name,run_code,imgae_file
                    order by run_code::int,chainage
                ) b
            where LOWER(a.pave_type) = b.event_name and a.chainage = b.chainage and split_part(a.filename,'_',2)::int = b.run_code
            '''
        cur_bleeding = conPG.cursor()
        cur_bleeding.execute(bleeding)
        conPG.commit()

        print(" ✓ Update table 'survey_local_pave' with 'icrack', 'ucrack', 'tranvertsal', 'rav', 'pothole', 'bleeding' successfully")
        print('____________________________________________________________________________________________________________________________________________\n')
    except Exception as e:
        print(f" \U0001F6AB An error occurred: {str(e)}")

def check_pic(path_direc):
        
            # ตรวจสอบรูปกล้องหน้า survey_local_point
            local_point_pic = query_table("""
                SELECT 
                    image_name
                FROM 
                    public.survey_local_point 
                """, conPG)
            local_point_pic = pd.DataFrame(local_point_pic, columns=['image_name_1'])

            # Get file names from the ROW directory and its subdirectories
            path_Row = os.path.join(path_direc, "ROW")
            file_names = []
            for root, dirs, files in os.walk(path_Row):
                for file in files:
                    if file.endswith(".jpg"):
                        file_names.append(file)

            # Create a DataFrame with the file names
            row_pic = pd.DataFrame({'image_name_2': file_names})

            # Merge the two dataframes based on matching values
            merged_df = pd.merge(local_point_pic, row_pic, left_on='image_name_1', right_on='image_name_2', how='left')
            
            # Filter the merged dataframe to only show rows where 'image_name_2' is NaN
            nan_values = merged_df[merged_df['image_name_2'].isnull()]
            nan_values.drop('image_name_2', axis=1, inplace=True)
            nan_values.rename(columns={'image_name_1': 'image_name'}, inplace=True)

            print(' 8.) ตรวจสอบไฟล์รูปกล้องหน้าว่ามีรูปหรือไม่ ?')
            if nan_values.empty == True:
                print('     \u2713 ตรวจสอบเรียบร้อย :: ไม่ต้องอัพโหลดไฟล์กล้องหน้าใหม่')
                print('____________________________________________________________________________________________________________________________________________')
            elif nan_values.empty == False:
                print(f'     \U0001F6AB โปรดอัพโหลดไฟล์กล้องหน้าใหม่อีกครั้ง')#\n{nan_values}
                for row in nan_values.values:
                    print(f'        ❌ {"".join(row)}')
                print('____________________________________________________________________________________________________________________________________________')
                #return
            print('')

            # ตรวจสอบรูปกล้องหลัง survey_local_pave
            local_point_pic = query_table("""
                SELECT 
                    filename
                FROM 
                    public.survey_local_pave
                """, conPG)
            local_point_pic = pd.DataFrame(local_point_pic, columns=['image_name_1'])

            # Get file names from the ROW directory and its subdirectories
            path_Pave = os.path.join(path_direc, "PAVE")
            file_names = []
            for root, dirs, files in os.walk(path_Pave):
                for file in files:
                    if file.endswith(".jpg"):
                        file_names.append(file)

            # Create a DataFrame with the file names
            row_pic = pd.DataFrame({'image_name_2': file_names})

            # Merge the two dataframes based on matching values
            merged_df = pd.merge(local_point_pic, row_pic, left_on='image_name_1', right_on='image_name_2', how='left')
            
            # Filter the merged dataframe to only show rows where 'image_name_2' is NaN
            nan_values = merged_df[merged_df['image_name_2'].isnull() & merged_df['image_name_1'].notnull()]

            nan_values.drop('image_name_2', axis=1, inplace=True)
            nan_values.rename(columns={'image_name_1': 'image_name'}, inplace=True)

            print(' 9.) ตรวจสอบไฟล์รูปกล้องหลังว่ามีรูปหรือไม่ ?')
            if nan_values.empty == True:
                print('     \u2713 ตรวจสอบเรียบร้อย :: ไม่ต้องอัพโหลดไฟล์กล้องหลังใหม่')
                print('____________________________________________________________________________________________________________________________________________')
            elif nan_values.empty == False:
                print(f'     \U0001F6AB โปรดอัพโหลดไฟล์กล้องหลังใหม่อีกครั้ง') #\n{nan_values}
                for row in nan_values.values:
                    print(f'        ❌ {"".join(row)}')
                print('____________________________________________________________________________________________________________________________________________')
                #return
            print('')

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
        print(f" \U0001F6AB An error occurred while dumping {table_name} table to SQL file: {str(e)}")

def main(date_survey, path_direc, path_out, user, password, host, port, database):
    # Define global variables
    access_crack = None
    access_pothole = None
    access_bleeding = None
    access_rav = None

    try:
        print(f"\nProcessing on '{proj}' project, '{device.upper()}' device, interval '{interval}' meters")
        # Establish connection
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

        if device.upper() == "TPL":
            access_valuelaser, access_key, access_distress_pic = extract_survey_data_tpl(file_dir, path_out, date_survey)
        elif device.upper() == "LCMS":
            access_valuelaser, access_key, access_distress_pic, access_crack, \
                access_pothole, access_bleeding, access_rav = extract_survey_data_lcms(file_dir, path_Data, path_out, date_survey)

        # Insert data into PostgreSQL
        dataframes_tpl = {
            'access_valuelaser': access_valuelaser,
            'access_key': access_key,
            'access_distress_pic': access_distress_pic
        }

        dataframes_lcms = {
            'access_valuelaser': access_valuelaser,
            'access_key': access_key,
            'access_distress_pic': access_distress_pic,
            'access_crack': access_crack,
            'access_pothole': access_pothole,
            'access_bleeding': access_bleeding,
            'access_rav': access_rav,
        }

        insert_to_postgres(dataframes_tpl, dataframes_lcms, user, password, host, port, database)

        # Perform preprocessing steps
        preprocessing_1(conPG)
        process_data_and_update(conPG, interval)
        preprocessing_2(conPG, interval)

        # Generate survey tables
        if survey_local_var.get():
            create_survey_local(conPG, s_id, device, proj, interval)
        else:
            pass

        if survey_local_point_var.get():
            create_survey_local_point(conPG, s_point_id, path_direc)
        else:
            pass

        if survey_local_pave_var.get():
            create_survey_local_pave(conPG, gid, proj)
            if device.upper() == "LCMS":
                update_distrass(conPG)
        else:
            pass

        # Generate survey tables
        if pic_var.get():
            # Check if survey_local_var.get() is false and set it to true if needed
            if not survey_local_var.get():
                survey_local_var.set(True)
                create_survey_local(conPG, s_id, device, proj, interval)
            
            # Check if survey_local_point_var.get() is false and set it to true if needed
            if not survey_local_point_var.get():
                survey_local_point_var.set(True)
                create_survey_local_point(conPG, s_point_id, path_direc)
            
            # Check if survey_local_pave_var.get() is false and set it to true if needed
            if not survey_local_pave_var.get():
                survey_local_pave_var.set(True)
                create_survey_local_pave(conPG, gid, proj)
            
            if device.upper() == "LCMS":
                update_distrass(conPG)

            check_pic(path_direc)
        else:
            pass

        # Dump tables to SQL
        if dump_sql_var.get():
            dump_table_to_sql(conPG, 'survey_local', path_out)
            dump_table_to_sql(conPG, 'survey_local_point', path_out)
            dump_table_to_sql(conPG, 'survey_local_pave', path_out)
        else:
            pass

        # Close connection
        conPG.close()

        print("-------------------------------------------------- รายการเสร็จสิ้น อย่าลืมตรวจสอบใน QGIS !!!!! --------------------------------------------------")

    except Exception as e:
        print(f" \U0001F6AB Error occurred in main function: {str(e)}")

def iterate_folders(path_file):
    try:
        # Check if the source folder exists
        if not os.path.exists(path_file):
            print(f" \U0001F6AB Source directory '{path_file}' does not exist.")
            return

        # Check if there are any folders inside the source directory
        folders = [folder for folder in os.listdir(path_file) if os.path.isdir(os.path.join(path_file, folder))]
        if not folders:
            print(f" \U0001F6AB No folders found inside '{path_file}'.")
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
                main(date_survey, path_direc, path_out, user, password, host, port, database)

                print("############################################################################################################################################\n\n")

    except FileNotFoundError:
        print(f" \U0001F6AB Directory '{path_file}' not found.")
    except Exception as e:
        print(f" \U0001F6AB Error occurred while iterating folders: {str(e)}")

def redirect_output_to_text_widget(widget):
    class TextRedirector:
        def __init__(self, widget):
            self.widget = widget

        def write(self, text):
            if "\U0001F6AB" in text or "❌" in text:  # Check if the warning symbol is present
                # Insert the text with warning symbol in red color
                self.widget.insert(tk.END, text, "red_alert")
            elif "\u26A0" in text:  # Check if the warning symbol is present
                # Insert the text with warning symbol in orange color
                self.widget.insert(tk.END, text, "warning")
            else:
                # Insert the text in green color
                self.widget.insert(tk.END, text, "normal")
            # Scroll to the end to show the latest result
            self.widget.see(tk.END)
            # Update the display immediately
            self.widget.update_idletasks()

    # Configure tags for text colors
    widget.tag_configure("red_alert", foreground="red")
    widget.tag_configure("warning", foreground="orange")
    widget.tag_configure("normal", foreground="green")

    sys.stdout = TextRedirector(widget)

def run_script():
    global source_folder, proj, interval, device 
    folder_path = source_folder
    #set_project_name = proj
    
    iterate_folders(source_folder)

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
        project_name_var = tk.StringVar()
        project_name = ["S24", "S24_DRR"]
        project_name_dropdown = ttk.Combobox(root, textvariable=project_name_var, values=project_name, width=147)
        project_name_dropdown.grid(row=1, column=1, padx=5, pady=5)
        project_name_dropdown.bind("<<ComboboxSelected>>", set_project_name) 

        # Interval dropdown
        interval_label = ttk.Label(root, text="Interval:")
        interval_label.grid(row=2, column=0, sticky="w")
        interval_var = tk.IntVar()
        interval_dropdown = ttk.Combobox(root, textvariable=interval_var, values=[10, 25, 50, 100], width=147)
        interval_dropdown.grid(row=2, column=1, padx=5, pady=5)

        # Set default value to 25
        interval_dropdown.current(1)  # 25 is at index 1 in the values list
        interval_dropdown.bind("<<ComboboxSelected>>", set_interval) 

        # Devices dropdown
        device_label = ttk.Label(root, text="Devices:")
        device_label.grid(row=3, column=0, sticky="w")
        device_var = tk.StringVar()
        devices = ["TPL", "LCMS"]
        device_dropdown = ttk.Combobox(root, textvariable=device_var, values=devices, width=147)
        device_dropdown.grid(row=3, column=1, padx=5, pady=5)
        device_dropdown.bind("<<ComboboxSelected>>", set_device) 

        # Define a common column and padx value for consistent alignment and spacing
        column_value = 1
        padx_value = 15

        survey_local_var = tk.BooleanVar(value=False) 
        survey_local_checkbox = ttk.Checkbutton(root, text="Survey Local", variable=survey_local_var )
        survey_local_checkbox.grid(row=4, column=column_value, sticky="w", padx=padx_value, pady=5)

        survey_local_point_var = tk.BooleanVar(value=False) 
        survey_local_point_checkbox = ttk.Checkbutton(root, text="Survey Local Point", variable=survey_local_point_var )
        survey_local_point_checkbox.grid(row=4, column=column_value, sticky="w", padx=padx_value+100, pady=5)

        survey_local_pave_var = tk.BooleanVar(value=False) 
        survey_local_pave_checkbox = ttk.Checkbutton(root, text="Survey Local Pave", variable=survey_local_pave_var )
        survey_local_pave_checkbox.grid(row=4, column=column_value, sticky="w", padx=padx_value+230, pady=5)

        dump_sql_var = tk.BooleanVar(value=False)  
        dump_sql_checkbox = ttk.Checkbutton(root, text="Genearte SQL", variable=dump_sql_var )
        dump_sql_checkbox.grid(row=4, column=column_value, sticky="w", padx=padx_value+350, pady=5)

        graph_var = tk.BooleanVar(value=False)  
        graph_checkbox = ttk.Checkbutton(root, text="Plot Graph", variable=graph_var )
        graph_checkbox.grid(row=5, column=column_value, sticky="w", padx=padx_value, pady=5)

        pic_var = tk.BooleanVar(value=False)  
        pic_checkbox = ttk.Checkbutton(root, text="Check Picture", variable=pic_var )
        pic_checkbox.grid(row=5, column=column_value, sticky="w", padx=padx_value+100, pady=5)

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
                print(" \U0001F6AB Unable to connect to the database:", e)

        except Exception as e:
            print(f" \U0001F6AB Error occurred in __main__: {str(e)}")

        # Run button
        run_button = ttk.Button(root, text="Run", command=run_script)
        run_button.grid(row=6, column=1, pady=10)

        # Text widget to display output
        result_text = tk.Text(root, wrap="word", width=140, height=30)
        result_text.grid(row=7, columnspan=3, padx=5, pady=5)

        # Redirect print output to the Text widget
        redirect_output_to_text_widget(result_text)

        root.mainloop()