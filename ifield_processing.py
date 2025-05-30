import os
import shutil
import pandas as pd
import numpy as np
from tqdm import tqdm
import re
import json
import glob
import win32com.client as w32
import xml.etree.ElementTree as ET
from object.metadata import Metadata
from object.enumerations import dataTypeConstants
from object.iSurvey import iSurvey

import collections.abc

# hyper needs the four following aliases to be done manually.
collections.Iterable = collections.abc.Iterable
collections.Mapping = collections.abc.Mapping
collections.MutableSet = collections.abc.MutableSet
collections.MutableMapping = collections.abc.MutableMapping

project_name = "VNXXXXXXX_TEMPLATE"

os.chdir("projects\\{}".format(project_name))

def remove_files(extensions=list()):
    for ext in extensions:
        files = glob.glob(os.path.join("data", ext))
        for f in files:
            if os.path.isfile(f):
                os.remove(f)
                
# Read the config file
f = open('config.json', mode = 'r', encoding="utf-8")
config = json.loads(f.read())
f.close()

# Check if the config file is valid
try:
    isurveys = {}

    csv_files = glob.glob(os.path.join("source\\csv", "*.csv"))
    csv_files = sorted(csv_files, key=lambda x: os.path.getctime(x), reverse=False)

    for csv_file in tqdm(csv_files, desc="Read the csv file"):
        df = pd.read_csv(csv_file, encoding="utf-8", low_memory=False)
        
        for proto_id in list(np.unique(list(df["ProtoSurveyID"]))):
            if proto_id not in isurveys.keys():
                isurveys[proto_id] = {
                    "csv_files" : [csv_file],
                    "survey" : None
                }
            else:
                isurveys[proto_id]["csv_files"].append(csv_file)
    
    if int(config["main"]["protoid_final"]) not in isurveys.keys():
        raise Exception("Config Error: " "ProtoID {} has no data in the CSV file".format(int(config["main"]["protoid_final"])))

    main_protoid_final = int(config["main"]["protoid_final"])

    # Read the xml file for the main section
    try:
        for proto_id, xml_file in tqdm(config["main"]["xmls"].items(), desc="Convet the xml file for the main section"):
            if os.path.exists(f'source\\xml\\{xml_file}'):
                try:
                    isurveys[int(proto_id)]["survey"] = iSurvey(f'source\\xml\\{xml_file}') 
                except Exception as ex:
                    raise Exception(ex.args[0])
            else:
                raise Exception("Config Error: ", "ProtoID {} has no data in the CSV file.".format(proto_id))
    except Exception as ex:
        raise Exception(ex.args[0])
    
    # Read the xml file for the placement + recall section
    follow_up_questions = dict()

    # Create a dictionary to store the follow-up questions
    follow_up_globalization = dict()

    for stage_id, stage_obj in tqdm(config["stages"].items(), desc="Convet the xml file for the placement + recall section"):
        try:
            for proto_id, xml_file in stage_obj["xmls"] .items():
                if "Enter a protodid" in proto_id:
                    break
                
                if os.path.exists(f'source\\xml\\{xml_file}'):
                    try:
                        isurveys[int(proto_id)]["survey"] = iSurvey(f'source\\xml\\{xml_file}') 
                    except Exception as ex:
                        raise Exception(ex.args)
                else:
                    raise Exception("Config Error: ", "ProtoID {} has no data in the CSV file.".format(proto_id))
        except Exception as ex:
            raise Exception(ex.args)
        
        if proto_id == stage_obj["protoid_final"]:
            for key, question in isurveys[int(proto_id)]["survey"]["questions"].items():
                # if key not in isurveys[main_protoid_final]["survey"]["questions"].keys():
                if key not in follow_up_questions.keys():
                    follow_up_questions[key] = dict()
                    follow_up_questions[key]["stages"] = list()
                
                    follow_up_questions[key]["question"] = question

                follow_up_questions[key]["stages"].append(stage_id)

            for key, translate in isurveys[int(proto_id)]["survey"]["globalization"].items():
                if key not in follow_up_globalization.keys():
                    key = f"Phase[].{key}"

                    follow_up_globalization[key] = dict()
                    follow_up_globalization[key] = translate
    
    source_mdd_file = "..\\..\\template\\TemplateProject.mdd"
    current_mdd_file = "data\\{}.mdd".format(config["project_name"])
    source_dms_file = "..\\..\\dms\OutputDDFFile.dms"
    
    if config["run_mdd_source"]:
        # Remove all mdd/ddf files in the data folder
        remove_files(extensions=["*.mdd", "*.ddf", "*.ivs", "*.xlsx"])
                    
        if not os.path.exists(current_mdd_file):
            shutil.copy(source_mdd_file, current_mdd_file)

        mdd_source = Metadata(mdd_file=current_mdd_file, dms_file=source_dms_file, default_language=config["source_initialization"]["default_language"])

        mdd_source.addScript("InstanceID", "InstanceID \"InstanceID\" text;")

        for question_name, question in tqdm(isurveys[main_protoid_final]["survey"]["questions"].items(), desc="Convert the mdd/ddf file for the main question"):
            if question["attributes"]["objectName"] in ["_PE10"]:
                a = ""

            if "syntax" in question.keys():
                
                if question["attributes"]["objectName"] in ["ID"]:
                    a = ""

                mdd_source.addScript(question["attributes"]["objectName"], question["syntax"], is_defined_list=question["is_defined_list"], parent_nodes=list() if "parents" not in question.keys() else [q["attributes"]["objectName"] for q in question["parents"]], globalization=isurveys[main_protoid_final]["survey"]["globalization"])

                if "comment_syntax" in question.keys():
                    mdd_source.addScript(f'{question["attributes"]["objectName"]}{question["comment"]["objectName"]}', question["comment_syntax"], parent_nodes=list() if "parents" not in question.keys() else [q["attributes"]["objectName"] for q in question["parents"]], globalization=isurveys[main_protoid_final]["survey"]["globalization"])
            else:
                if "comment_syntax" in question.keys():
                    mdd_source.addScript(question["attributes"]["objectName"], question["comment_syntax"], is_defined_list=question["is_defined_list"], parent_nodes=list() if "parents" not in question.keys() else [q["attributes"]["objectName"] for q in question["parents"]], globalization=isurveys[main_protoid_final]["survey"]["globalization"])

        # isurveys[main_protoid_final]["survey"]["globalization"].to_excel(file_name="translate\\{}_GLOBALIZATION.xlsx".format(config["project_name"]))

        if len(follow_up_questions):
            mdd_source.addScript("Phase", "Phase \"\" loop {%s} fields ()expand grid;" % (",".join([ "{} \"{}\"".format(stage.replace("-", "_"), stage)  for stage in config["stages"].keys()])))
            
            mdd_source.addScript("InstanceID", "InstanceID \"InstanceID\" text;", parent_nodes=["Phase"])
            
            for question_name, obj in tqdm(follow_up_questions.items(), desc="Convert the mdd/ddf file for the follow-up question"):
                if "syntax" in obj["question"].keys():
                    parent_nodes = list()
                    
                    if not obj["question"]["is_defined_list"]:
                        parent_nodes.append("Phase")

                    if "_PE10" in obj["question"]["attributes"]["objectName"]:
                        a = ""

                    if "parents" in obj["question"].keys():
                        parent_nodes.extend([q["attributes"]["objectName"] for q in obj["question"]["parents"]])

                    mdd_source.addScript(obj["question"]["attributes"]["objectName"], obj["question"]["syntax"], is_defined_list=obj["question"]["is_defined_list"], parent_nodes=parent_nodes, globalization=follow_up_globalization)

                    if "comment_syntax" in question.keys():
                        mdd_source.addScript(f'{obj["question"]["attributes"]["objectName"]}{obj["question"]["comment"]["objectName"]}', obj["question"]["comment_syntax"], parent_nodes=parent_nodes, globalization=follow_up_globalization)
    
        mdd_source.runDMS()
    else: 
        if config["run_mdd_export_source"]:
            # Remove all mdd/ddf files in the data folder
            remove_files(extensions=["*_EXPORT.mdd", "*_EXPORT.ddf", "*_EXPORT.ivs", "*_CE.mdd", "*_CE.ddf", "*_CE.ivs", "*_OE.mdd", "*_OE.ddf", "*_OE.ivs", "*.xlsx"])
            
            mdd_source = Metadata(mdd_file=current_mdd_file, dms_file=source_dms_file, default_language=config["source_initialization"]["default_language"])

            mdd_source.runDMS()

    ###################################################################################
    current_mdd_file = "data\\{}_EXPORT.mdd".format(config["project_name"])
    current_ddf_file = "data\\{}_EXPORT.ddf".format(config["project_name"])

    # Create mdd/ddf file based on xmls file
    respondent_file = "data\\{}_RESPONDENT.xlsx".format(config["project_name"])
    
    writer = pd.ExcelWriter(respondent_file, engine='xlsxwriter', mode='w')

    #Open the update data file
    df_update_data = pd.read_csv("source\\update_data.csv", encoding="utf-8")
    
    df_respondents = pd.DataFrame()

    if not os.path.isfile(current_mdd_file) or not os.path.isfile(current_ddf_file):
        raise Exception("File Error", "File mdd/ddf is not exist.")
    
    adoConn = w32.Dispatch('ADODB.Connection')
    conn = "Provider=mrOleDB.Provider.2; Data Source = mrDataFileDsc; Location={}; Initial Catalog={}; Mode=ReadWrite; MR Init Category Names=1".format(current_ddf_file, current_mdd_file)
    
    # Delete all data before inserting new data (default is FALSE)
    if config["source_initialization"]["delete_all"]:
        adoConn.Open(conn)
        sql_delete = "DELETE FROM VDATA"
        adoConn.Execute(sql_delete)
        adoConn.Close()

    #Lưu ID thuộc phần main
    shell_chain_ids = list()

    for proto_id, xml_file in config["main"]["xmls"].items():
        for csv_file in tqdm(isurveys[int(proto_id)]["csv_files"], desc="During the data insertion process"):
            df = pd.read_csv(csv_file, encoding="utf-8", low_memory=False)
            df.set_index(['InstanceID'], inplace=True)

            #Allow inserting dummy data
            if config["source_initialization"]["dummy_data_required"]:
                df = df.loc[df["System_LocationID"] == "_DefaultSP"]
            else:
                df = df.loc[df["System_LocationID"] != "_DefaultSP"]
                
            m = Metadata(mdd_file=current_mdd_file, ddf_file=current_ddf_file, sql_query="SELECT InstanceID, SHELL_CHAINID FROM VDATA")
            df_instanceids = m.convertToDataFrame(questions=["InstanceID","SHELL_CHAINID"])
            
            if not df_instanceids.empty:
                df_instanceids.set_index(['InstanceID'], inplace=True)

                shell_chain_ids = list(df_instanceids["SHELL_CHAINID"])
            
            ids = [id for id in list(df.index) if str(id) not in list(df_instanceids.index)]

            if len(ids) > 0:
                df_data = df.loc[ids]
                
                df_respondents = pd.concat([df_respondents, df_data[config["respondent_information_columns"].values()]], axis=0, ignore_index=False)

                #Allow inserting dummy data
                if config["source_initialization"]["dummy_data_required"]:
                    df_data = df_data.loc[df_data["System_LocationID"] == "_DefaultSP"]
                else:
                    df_data = df_data.loc[df_data["System_LocationID"] != "_DefaultSP"]
                
                if not df_data.empty:
                    adoConn.Open(conn)

                    for i, row in tqdm(df_data[list(df_data.columns)].iterrows(), desc="Insert {} instanceids into the mdd/ddf file".format(len(ids))):
                        try:
                            isurvey = isurveys[int(row["ProtoSurveyID"])]["survey"]
                        except Exception as ex:
                            raise Exception("Config Error", "ProtoID {} should be declare in the config file.".format(str(row["ProtoSurveyID"])))
                            
                        sql_insert = "INSERT INTO VDATA(InstanceID) VALUES(%s)" % (row.name)
                        adoConn.Execute(sql_insert)
                        
                        c = list()
                        v = list()

                        for key, question in isurvey["questions"].items():
                            if question["attributes"]["objectName"] in ["ID"]:
                                if row.name == 1609219:
                                    a = ""

                            if question["datatype"] not in [dataTypeConstants.mtNone, dataTypeConstants.mtLevel]:
                                for i in range(len(question["columns"])):
                                    for mdd_col, csv_obj in question["columns"][i].items():

                                        if csv_obj["csv"][0] not in config["respondent_information_columns_removed"]:
                                            if (question["datatype"].value == dataTypeConstants.mtCategorical.value) or (question["datatype"].value == dataTypeConstants.mtObject.value and csv_obj["datatype"].value == dataTypeConstants.mtCategorical.value):
                                                if question["answers"]["attributes"]["answerSetID"] != "8" and question["answers"]["attributes"]["answerSetID"] != "-1":
                                                    if bool(int(question["answers"]["answerref"]["attributes"]["isMultipleSelection"])):
                                                        if row[csv_obj["csv"]].sum() > 0:
                                                            c.append(mdd_col)
                                                            v.append("{%s}" % (",".join([k.split(".")[-1] for k, v in dict(row[csv_obj["csv"]]).items() if v == 1])))
                                                    else:
                                                        if not pd.isnull(row[csv_obj["csv"][0]]): 
                                                            c.append(mdd_col)
                                                            v.append("{%s}" % (question["answers"]["options"][str(int(row[csv_obj["csv"][0]]))]["objectname"]))
                                                
                                                    for mdd_other_col, csv_other_obj in csv_obj["others"].items():

                                                        if not pd.isnull(row[csv_other_obj["csv"][0]]):
                                                            c.append(mdd_other_col)

                                                            match int(csv_other_obj["datatype"]):
                                                                case 2:
                                                                    v.append("{}".format(row[csv_other_obj["csv"][0]]))
                                                                case 3:
                                                                    v.append("'{}'".format(". ".join(re.split('\n|\r', re.sub(pattern='[\']', repl="''", string=str(row[csv_other_obj["csv"][0]]))))))
                                                                case 4:
                                                                    v.append("'{}'".format(row[csv_other_obj["csv"][0]]))
                                            else:
                                                if not pd.isnull(row[csv_obj["csv"][0]]):
                                                    match question["datatype"].value:
                                                        case dataTypeConstants.mtText.value:
                                                            c.append(mdd_col)
                                                            v.append("'{}'".format(". ".join(re.split('\n|\r', re.sub(pattern='[\']', repl="''", string=str(row[csv_obj["csv"][0]]))))))
                                                        case dataTypeConstants.mtDate.value:
                                                            c.append(mdd_col)
                                                            v.append("'{}'".format(row[csv_obj["csv"][0]]))
                                                        case dataTypeConstants.mtDouble.value:
                                                            c.append(mdd_col)
                                                            v.append("{}".format(row[csv_obj["csv"][0]]))
                                                        case dataTypeConstants.mtObject.value:
                                                            c.append(mdd_col)
                                                            v.append("{}".format(row[csv_obj["csv"][0]]))
                        
                        sql_update = "UPDATE VDATA SET " + ','.join([cx + str(r" = %s") for cx in c]) % tuple(v) + " WHERE InstanceID = {}".format(row.name)
                        adoConn.Execute(sql_update)

                        shell_chain_ids.append(row["SHELL_CHAINID"])

                    adoConn.Close()

    if not df_respondents.empty:
        try:
            for i, row in df_update_data.iterrows():
                if row["Question Name"] in config["respondent_information_columns_removed"]:
                    df_respondents.loc[row["InstanceID"], row["Question Name"]] = row["Current Value"]

            df_respondents.to_excel(writer, sheet_name="RESPONDENT")
        except Exception as ex:
            pass
        finally:    
            writer.close()
    
    if len(follow_up_questions):
        for stage, obj_stage in config["stages"].items(): 
            for proto_id, xml_file in obj_stage["xmls"].items():
                if int(proto_id) in isurveys.keys():
                    for csv_file in tqdm(isurveys[int(proto_id)]["csv_files"], desc="During the data insertion process to the {}".format(stage)):
                        df = pd.read_csv(csv_file, encoding="utf-8", low_memory=False)
                        df.set_index(['SHELL_CHAINID'], inplace=True)

                        #Allow inserting dummy data
                        if config["source_initialization"]["dummy_data_required"]:
                            df = df.loc[df["System_LocationID"] == "_DefaultSP"]
                        else:
                            df = df.loc[df["System_LocationID"] != "_DefaultSP"]

                        recall_shell_chain_ids = [id for id in list(df.index) if any([id in shell_chain_ids, str(id) in shell_chain_ids])]
                        
                        if len(recall_shell_chain_ids) > 0:
                            df_data = df.loc[recall_shell_chain_ids]

                            #Allow inserting dummy data
                            if config["source_initialization"]["dummy_data_required"]:
                                df_data = df_data.loc[df_data["System_LocationID"] == "_DefaultSP"]
                            else:
                                df_data = df_data.loc[df_data["System_LocationID"] != "_DefaultSP"]

                            if not df_data.empty:
                                adoConn.Open(conn)

                                for i, row in tqdm(df_data[list(df_data.columns)].iterrows(), desc="Insert {} instanceids into the mdd/ddf file".format(len(recall_shell_chain_ids))):
                                    try:
                                        isurvey = isurveys[int(row["ProtoSurveyID"])]["survey"]
                                    except Exception as ex:
                                        raise Exception("Config Error", "ProtoID {} should be declare in the config file.".format(str(row["ProtoSurveyID"])))
                                    
                                    c = list()
                                    v = list()

                                    c.append("Phase[{%s}].InstanceId" % (stage.replace("-", "_")))
                                    v.append(row["InstanceID"])
                                    
                                    for key, question in isurvey["questions"].items():
                                        if question["attributes"]["objectName"] not in ["SHELL_CHAINID"]:
                                            if question["datatype"] not in [dataTypeConstants.mtNone, dataTypeConstants.mtLevel]:
                                                for i in range(len(question["columns"])):
                                                    for mdd_col, csv_obj in question["columns"][i].items():
                                                        mdd_col = "Phase[{%s}].%s" % (stage.replace("-", "_"), mdd_col)

                                                        if (mdd_col == 'Phase[{stage_2}]._BLOCK_USER_2._PE_BLOCK._PE2'):
                                                            a = ""

                                                        if (question["datatype"].value == dataTypeConstants.mtCategorical.value) or (question["datatype"].value == dataTypeConstants.mtObject.value and csv_obj["datatype"].value == dataTypeConstants.mtCategorical.value):
                                                            if bool(int(question["answers"]["answerref"]["attributes"]["isMultipleSelection"])):
                                                                if row[csv_obj["csv"]].sum() > 0:
                                                                    c.append(mdd_col)
                                                                    v.append("{%s}" % (",".join([k.split(".")[-1] for k, v in dict(row[csv_obj["csv"]]).items() if v == 1])))
                                                            else:
                                                                if not pd.isnull(row[csv_obj["csv"][0]]): 
                                                                    c.append(mdd_col)
                                                                    v.append("{%s}" % (question["answers"]["options"][str(int(row[csv_obj["csv"][0]]))]["objectname"]))
                                                            
                                                            for mdd_other_col, csv_other_obj in csv_obj["others"].items():
                                                                mdd_other_col = "Phase[{%s}].%s" % (stage.replace("-", "_"), mdd_other_col)

                                                                if not pd.isnull(row[csv_other_obj["csv"][0]]):
                                                                    c.append(mdd_other_col)

                                                                    match int(csv_other_obj["datatype"]):
                                                                        case 2:
                                                                            v.append("{}".format(row[csv_other_obj["csv"][0]]))
                                                                        case 3:
                                                                            v.append("'{}'".format(". ".join(re.split('\n|\r', re.sub(pattern='[\']', repl="''", string=str(row[csv_other_obj["csv"][0]]))))))
                                                                        case 4:
                                                                            v.append("'{}'".format(row[csv_other_obj["csv"][0]]))
                                                        else:
                                                            if not pd.isnull(row[csv_obj["csv"][0]]):
                                                                match question["datatype"].value:
                                                                    case dataTypeConstants.mtText.value:
                                                                        c.append(mdd_col)
                                                                        v.append("'{}'".format(". ".join(re.split('\n|\r', re.sub(pattern='[\']', repl="''", string=str(row[csv_obj["csv"][0]]))))))
                                                                    case dataTypeConstants.mtDate.value:
                                                                        c.append(mdd_col)
                                                                        v.append("'{}'".format(row[csv_obj["csv"][0]]))
                                                                    case dataTypeConstants.mtDouble.value:
                                                                        c.append(mdd_col)
                                                                        v.append("{}".format(row[csv_obj["csv"][0]]))
                                                                    case dataTypeConstants.mtObject.value:
                                                                        c.append(mdd_col)
                                                                        v.append("{}".format(row[csv_obj["csv"][0]]))
                                    
                                    sql_update = "UPDATE VDATA SET " + ','.join([cx + str(r" = %s") for cx in c]) % tuple(v) + " WHERE SHELL_CHAINID = '{}'".format(row.name)
                                    adoConn.Execute(sql_update)

                                adoConn.Close()
            
    if not df_update_data.empty:
        adoConn.Open(conn)

        df_update_data.set_index(["InstanceID"], inplace=True)

        for id in list(set(df_update_data.index)):
            try:
                df_update_data_by_id = df_update_data[["Question Name", "Current Value"]].loc[id]

                if not df_update_data_by_id.empty:
                    sql_update = "UPDATE VDATA SET %s WHERE InstanceID = %s" % (
                        "%s = %s" % (
                            df_update_data_by_id[0], 
                            'NULL' if pd.isnull(df_update_data_by_id[1]) else (df_update_data_by_id[1] if str(df_update_data_by_id[1]).isnumeric() else "'%s'" % (df_update_data_by_id[1]))
                        ) if isinstance(df_update_data_by_id,pd.Series) else ",".join(["%s = %s" % (
                            x[0],
                            'NULL' if pd.isnull(x[1]) else (x[1] if str(x[1]).isnumeric() else "'%s'" % (x[1]))
                        ) for x in [tuple(x) for x in df_update_data[["Question Name","Current Value"]].loc[id].to_numpy()]]),
                        id
                    )
                    adoConn.Execute(sql_update)
            except Exception as error:
                print("[Bug]: ID {} - {}".format(id, error.excepinfo[2]))
                continue

        adoConn.Close()

    #Delete all data before inserting new data (default is FALSE)
    if config["source_initialization"]["remove_all_ids"]:
        try:
            adoConn.Open(conn)

            sql_delete = "DELETE FROM VDATA WHERE Not _LoaiPhieu.ContainsAny({_1,_5})"
            adoConn.Execute(sql_delete)
        except:
            pass
        finally:
            adoConn.Close()

    ###################################################################################
    try:
        if config["processing_data"]["run_ce_source"]:
            remove_files(extensions=["*_CE.mdd", "*_CE.ddf", "*_CE.ivs", "*_OE.mdd", "*_OE.ddf", "*_OE.ivs"])

            current_mdd_file = "data\\{}_EXPORT.mdd".format(config["project_name"])
            current_ddf_file = "data\\{}_EXPORT.ddf".format(config["project_name"])
            cleandata_dms_file = "dms\CLEAN_DATA_SCRIPT.dms"

            if os.path.isfile(current_mdd_file) or os.path.isfile(current_ddf_file):
                mdd_source = Metadata(mdd_file=current_mdd_file, ddf_file=current_ddf_file, dms_file=cleandata_dms_file, default_language=config["source_initialization"]["default_language"], sql_query="SELECT * FROM VDATA")

                mdd_source.addAxisExpressions(axisexpressions=config["axis_expression"])
                
                mdd_source.runCleanDataDMS(mode="CE")
            else:
                raise Exception("File Error", "File mdd/ddf is not exist.")

    except Exception as ex:
        print(repr(ex))

    ###################################################################################
    try:
        if config["processing_data"]["run_oe_source"]:
            # Remove all mdd/ddf files in the data folder
            remove_files(extensions=["*_OE.mdd", "*_OE.ddf", "*_OE.ivs"])

            current_mdd_file = "data\\{}_CE.mdd".format(config["project_name"])
            current_ddf_file = "data\\{}_CE.ddf".format(config["project_name"])
            cleandata_dms_file = "dms\CLEAN_DATA_PROCESSING.dms"

            if os.path.isfile(current_mdd_file) or os.path.isfile(current_ddf_file):
                mdd_source = Metadata(mdd_file=current_mdd_file, ddf_file=current_ddf_file, dms_file=cleandata_dms_file, default_language=config["source_initialization"]["default_language"], sql_query="SELECT * FROM VDATA")

                mdd_source.runCleanDataDMS(mode="OE")
            else:
                raise Exception("File Error", "File CE mdd/ddf is not exist.")

    except Exception as ex:
        print(repr(ex))

except Exception as ex:
    print(repr(ex))
    #sys.exit(repr(error))


