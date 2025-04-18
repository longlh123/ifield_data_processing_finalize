from object.metadata import mrDataFileDsc
from object.enumerations import objectTypeConstants, dataTypeConstants
from datetime import datetime
import savReaderWriter
import pandas as pd
import numpy as np
import re

class SPSSObject_Dataframe(mrDataFileDsc):
    def __init__(self, mdd_path, ddf_path, sql_query, questions, groups=list()):
        mrDataFileDsc.__init__(self, mdd_file=mdd_path, ddf_file=ddf_path, sql_query=sql_query)

        self.df = pd.DataFrame()
        self.df_spss = pd.DataFrame()

        self.records = list()
        self.varNames = list()
        self.varMissingValues = dict() 
        self.varLabels = dict()
        self.varTypes = dict()
        
        self.valueLabels = dict()
        self.measureLevels = dict()
        self.var_date_formats = dict()
        self.var_dates = list()

        #Group of columns with format A_suffix1, A_suffix2...B_suffix1, B_suffix2
        self.groups = groups
        #Columns to use as id variable
        self.id_vars = list()
        self.value_groupnames = list()
        self.groupname_labels = list()

        self.init(questions)

    def init(self, questions):
        self.openMDM()
        self.openDataSource()
        
        if len(self.groups) > 0:
            field = self.get_nested_field(self.groups)
            #self.value_groupnames.extend(self.getValueLabels(self.MDM.Fields[self.group_name]).keys())

        data = self.adoRS.GetRows()

        columns = [f.Name for f in self.adoRS.Fields]
        
        self.df = pd.DataFrame(data=np.array(data).T, columns=columns)

        self.transform(questions)

        self.closeMDM()
        self.closeDataSource()

    def get_nested_field(self, groups=list(), parent_field=None):
        if len(groups) == 1:
            return self.MDM.Fields[groups[0]] if parent_field is None else parent_field.Fields[groups[0]]
        else:
            return self.get_nested_field(groups[1:], self.MDM.Fields[groups[0]] if parent_field is None else parent_field.Fields[groups[0]])

    def get_columns(self, questions):
        columns = list()

        for question in questions:
            field = self.MDM.Fields[question]

            if field.Properties["py_isHidden"] is None or field.Properties["py_isHidden"] == False:
                columns.extend([variable.FullName for variable in field.Variables])

        return columns

    def transform(self, questions):
        
        for question in questions:
            field = self.MDM.Fields[question]

            if field.Properties["py_isHidden"] is None or field.Properties["py_isHidden"] == False:
                self.generate_variable(field)

    def get_iterations(self, field, indexes):
        indexes = indexes.split(',')
        iterations = list()

        def findIteration(f, index):
            if index < 0:
                return
            
            iterations.append(f.Categories[re.sub(pattern="[\{\}]", repl="", string=indexes[index])])
            findIteration(f.Parent.Parent, index - 1)

        findIteration(field, len(indexes) - 1)
        return iterations
        
    def generate_variable(self, field, iterations_list=list(), iterations=list()):

        if str(field.ObjectTypeValue) == objectTypeConstants.mtVariable.value:
            if field.LevelDepth > 1:
                if field.Parent.Parent.Name == "@class":
                    iterations = self.get_iterations(field.Parent.Parent.Parent, field.CurrentIndexPath) 
                else:
                    iterations = self.get_iterations(field.Parent.Parent, field.CurrentIndexPath) 

            if field.DataType in [ dataTypeConstants.mtLong.value, dataTypeConstants.mtDouble.value, dataTypeConstants.mtText.value, dataTypeConstants.mtBoolean.value ]:
                self.transform_simple_data(field, iterations=iterations)
            elif field.DataType == dataTypeConstants.mtCategorical.value:
                self.transform_categorical_data(field, iterations=iterations)
        else:
            iterations_list = list()

            if field.Properties["py_setCategoriesList"]:
                iterations_list = field.Properties["py_setCategoriesList"].split(',')
            else:
                iterations_list = [cat.Name for cat in field.Categories]

            for f in field.Fields:
                if f.Properties["py_isHidden"] is None or f.Properties["py_isHidden"] == False:
                    full_name = f.FullName.replace("..","%s") % tuple(field.CurrentIndexPath.split(','))
                    for iteration in iterations_list:
                        iteration_full_name = full_name.replace("..", "{%s}" % iteration)
                        self.generate_variable(self.MDM.Fields[iteration_full_name], iterations=field.Categories[iteration])
                    
    def transform_simple_data(self, field, iterations=list()):
        var_name = self.get_variable_name(field, iterations=iterations)
        var_label = self.replaceLabel(self.get_variable_label(field, iterations=iterations))

        self.varNames.append(var_name)
        self.varLabels[var_name] = var_label.encode('utf-8')
        self.varTypes[var_name] = self.get_datatype(field)
        self.measureLevels[var_name] = self.get_measure_level(field)

        if field.LevelDepth == 1:
            self.df_spss[var_name] = self.df[field.FullName]
        else:
            self.df_spss[var_name] = self.df[field.FullName.replace("..", field.CurrentIndexPath)]
    
    def transform_categorical_data(self, field, iterations=list()):
        var_name = self.get_variable_name(field, iterations=iterations)
        var_label = self.replaceLabel(self.get_variable_label(field, iterations=iterations))

        value_labels = self.get_categories_list(field, show_punching_data=field.Properties["py_showPunchingData"])
        
        categories_list = list()
        remaining_categories_list = list()
        other_categories_list = list()

        if field.Properties["py_setCategoriesList"]:
            categories_list = field.Properties["py_setCategoriesList"].split(',')
        else:
            categories_list = [cat.Name for cat in field.Categories if not cat.IsOtherLocal]

        remaining_categories_list = [cat.Name for cat in field.Categories if cat.Name not in categories_list and not cat.IsOtherLocal]
        
        try:
            if field.OtherCategories.Count > 0:
                other_categories_list = [cat.Name for cat in field.OtherCategories]
        except AttributeError as e:
            pass

        if field.Properties["py_showPunchingData"]:
            var_fullname = field.FullName if len(iterations) == 0 else field.FullName.replace("..", "%s") % tuple(field.CurrentIndexPath.split(","))

            df_updated = self.df[var_fullname].replace(to_replace="[\{\}]", value="", regex=True)

            for cat_name in categories_list:
                category = field.Categories[cat_name]
                
                attr_name = re.search(pattern="_\d+$", string=var_name)[0]
                q_name = var_name.replace(attr_name, "")

                var_name_temp = "{}_C{}{}".format(q_name, re.sub(pattern="^_", repl="", string=category.Name), attr_name)

                #var_name_temp = "{}_C{}".format(var_name, re.sub(pattern="^_", repl="", string=category.Name))

                self.varNames.append(var_name_temp)
                self.varLabels[var_name_temp] = "{}_{}".format(var_label, category.Label).encode('utf-8')     
                self.varTypes[var_name_temp] = self.get_datatype(field)
                self.measureLevels[var_name_temp] = self.get_measure_level(field)
                self.valueLabels[var_name_temp] = value_labels
                
                self.df_spss = pd.concat([self.df_spss, df_updated], ignore_index=False, axis=1)
                
                match field.Properties["py_setVariableValues"]:
                    case "Values":
                        self.df_spss[var_name_temp] = self.df_spss[df_updated.name].apply(lambda x: None if x is None else 1 if category.Name.lower() in x.split(',') else 0)
                    case "Labels":
                        self.df_spss[var_name_temp] = self.df_spss[df_updated.name].apply(lambda x: None if x is None else "Yes" if category.Name.lower() in x.split(',') else "No")

                self.df_spss.drop(columns=[df_updated.name], inplace=True)

            if len(remaining_categories_list) > 0:
                var_name_temp = "{}_C{}".format(var_name, re.sub(pattern="^_", repl="", string='_97'))

                self.varNames.append(var_name_temp)
                self.varLabels[var_name_temp] = "{}_{}".format(var_label, category.Label).encode('utf-8')     
                self.varTypes[var_name_temp] = self.get_datatype(field)
                self.measureLevels[var_name_temp] = self.get_measure_level(field)
                self.valueLabels[var_name_temp] = value_labels

                self.df_spss = pd.concat([self.df_spss, df_updated], ignore_index=False, axis=1)
                
                match field.Properties["py_setVariableValues"]:
                    case "Values":
                        self.df_spss[var_name_temp] = self.df_spss[df_updated.name].apply(lambda x: None if x is None else 1 if any([y in remaining_categories_list for y in x.split(',')]) else 0)
                    case "Labels":
                        self.df_spss[var_name_temp] = self.df_spss[df_updated.name].apply(lambda x: None if x is None else "Yes" if any([y in remaining_categories_list for y in x.split(',')]) else "No")

                self.df_spss.drop(columns=[df_updated.name], inplace=True)

            if field.Properties["py_showHelperFields"]:
                if field.HelperFields.Count > 0:
                    df_others = pd.DataFrame()
                    df_others["Other"] = self.df[[helperfield.FullName for helperfield in field.HelperFields]].fillna('').sum(1).replace('', np.nan)
                    
                    if field.Properties["py_combibeHelperFields"]:
                        var_name_temp = "{}_C{}".format(var_name, re.sub(pattern="^_", repl="", string='_997'))
                        
                        self.varNames.append(var_name_temp)
                        self.varLabels[var_name_temp] = "{}_{}".format(var_label, category.Label).encode('utf-8')     
                        self.varTypes[var_name_temp] = self.get_datatype(field)
                        self.measureLevels[var_name_temp] = self.get_measure_level(field)
                        self.valueLabels[var_name_temp] = value_labels

                        self.df_spss[var_name_temp] = df_others["Other"].apply(lambda x: 0 if pd.isna(x) else 1)

                        var_name_temp = "{}_{}_Verbatim".format(var_name, re.sub(pattern="^_", repl="", string='_997'))
                        self.varNames.append(var_name_temp)
                        self.varLabels[var_name_temp] = var_label.encode('utf-8')
                        self.varTypes[var_name_temp] = self.get_datatype(field.HelperFields[0])

                        if self.varTypes[var_name_temp] == 0:
                            self.varMissingValues[var_name_temp] = {"values": [999, -1, -2]}

                        self.measureLevels[var_name_temp] = self.get_measure_level(field.HelperFields[0])

                        self.df_spss[var_name_temp] = df_others["Other"].apply(lambda x: '' if pd.isna(x) else x)
                    else:
                        for helperfield in field.HelperFields:
                            var_name_temp = "{}_C{}".format(var_name, re.sub(pattern="^_", repl="", string=helperfield.Name))
                            self.varNames.append(var_name_temp)
                            self.varLabels[var_name_temp] = "{}_{}".format(var_label, category.Label).encode('utf-8')     
                            self.varTypes[var_name_temp] = self.get_datatype(field)
                            self.measureLevels[var_name_temp] = self.get_measure_level(field)
                            self.valueLabels[var_name_temp] = value_labels

                            self.df_spss[var_name_temp] = self.df[helperfield.FullName].apply(lambda x: 0 if pd.isna(x) else 1)

                            var_name_temp = "{}_C{}_Verbatim".format(var_name, re.sub(pattern="^_", repl="", string=helperfield.Name))
                            self.varNames.append(var_name_temp)
                            self.varLabels[var_name_temp] = var_label.encode('utf-8')
                            self.varTypes[var_name_temp] = self.get_datatype(helperfield)
                            self.measureLevels[var_name_temp] = self.get_measure_level(helperfield)

                            self.df_spss[var_name_temp] = self.df[helperfield.FullName].apply(lambda x: '' if pd.isna(x) else x)
        elif field.Properties["py_showVariableNames"] and field.Properties["py_showVariableLabels"]:
            replaced_categories_list = {}
            replaced_values_list = {}

            for cat in field.Categories:
                replaced_categories_list[cat.Name] = cat.Label
                replaced_values_list[cat.Name] = cat.Name if cat.Properties["value"] is None else cat.Properties["value"]

            if field.LevelDepth == 1:
                match field.Properties["py_setVariableValues"]:
                    case "Values":
                        self.df_spss[var_name] = self.df[field.FullName].apply(lambda x: np.nan if x is None else ';'.join(map(replaced_values_list.get, x[1:len(x)-1].split(','))))
                    case _:
                        self.df_spss[var_name] = self.df[field.FullName].replace(to_replace=",_", value=";", regex=True)
                        self.df_spss[var_name] = self.df_spss[var_name].replace(to_replace="[\{\}_]", value="", regex=True)

                self.df_spss[f"{var_name}_Label"] = self.df[field.FullName].apply(lambda x: np.nan if x is None else ';'.join(map(replaced_categories_list.get, x[1:len(x)-1].split(','))))
            else:
                #var_name = "{} - {}".format(var_name, field.CurrentIndexPath)
                var_fullname = field.FullName.replace("..", "%s") % tuple(field.CurrentIndexPath.split(","))
                
                match field.Properties["py_setVariableValues"]:
                    case "Values":
                        self.df_spss[var_name] = self.df[var_fullname].apply(lambda x: np.nan if x is None else ';'.join(map(replaced_values_list.get, x[1:len(x)-1].split(','))))
                    case _:
                        self.df_spss[var_name] = self.df[var_fullname].replace(to_replace=",_", value=";", regex=True)
                        self.df_spss[var_name] = self.df_spss[var_name].replace(to_replace="[\{\}_]", value="", regex=True)
                
                self.df_spss[f"{var_name}_Label"] = self.df[var_fullname].apply(lambda x: np.nan if x is None else ';'.join(map(replaced_categories_list.get, x[1:len(x)-1].split(','))))
        else:
            replaced_categories_list = {}

            for cat in field.Categories:
                cat_name = cat.Name[1:len(cat.Name)] if cat.Name[0:1] in ["_"] else cat.Name

                match field.Properties["py_setVariableValues"]:
                    case "Values":
                        replaced_cat_name = int(cat_name if cat_name.isnumeric() else field.Categories[cat.Name].Properties["value"])
                        replaced_categories_list[cat_name.lower()] = replaced_cat_name
                    case "Labels":
                        replaced_categories_list[cat_name.lower()] = cat.Label
            
            var_fullname = field.FullName if len(iterations) == 0 else field.FullName.replace("..", "%s") % tuple(field.CurrentIndexPath.split(","))

            df_updated = self.df[var_fullname].replace(to_replace="[\{\}]", value="", regex=True)
            df_updated = df_updated.str.split(',', expand=True)
            df_updated = df_updated[list(df_updated.columns)].replace(to_replace="^_", value="", regex=True)

            column_renamed = dict()

            for i in list(df_updated.columns):
                var_name_temp = var_name if field.MinValue == 1 and field.MaxValue == 1 else "{}_{}".format(var_name, i + 1)
                var_name_temp = re.sub(pattern="^_", repl="", string=var_name_temp)
                self.varNames.append(var_name_temp)
                self.varLabels[var_name_temp] = var_label.encode('utf-8')
                self.varTypes[var_name_temp] = self.get_datatype(field)
                self.measureLevels[var_name_temp] = self.get_measure_level(field)
                self.valueLabels[var_name_temp] = value_labels

                column_renamed[i] = var_name_temp
            
            df_updated.rename(columns=column_renamed, inplace=True)
            df_updated.replace(replaced_categories_list, inplace=True)

            self.df_spss = pd.concat([self.df_spss, df_updated], ignore_index=False, axis=1)

            if field.Properties["py_showHelperFields"]:
                if field.HelperFields.Count > 0:
                    df_others = pd.DataFrame()
                    df_others["Other"] = self.df[[helperfield.FullName for helperfield in field.HelperFields]].fillna('').sum(1).replace('', np.nan)

                    if field.Properties["py_combibeHelperFields"]:
                        var_name_temp = "{}_C{}_Verbatim".format(var_name, re.sub(pattern="^_", repl="", string=helperfield.Name))
                        self.varNames.append(var_name_temp)
                        self.varLabels[var_name_temp] = var_label.encode('utf-8')
                        self.varTypes[var_name_temp] = self.get_datatype(helperfield)
                        self.measureLevels[var_name_temp] = self.get_measure_level(helperfield)

                        self.df_spss[var_name_temp] = self.df[helperfield.FullName].apply(lambda x: '' if pd.isna(x) else x)
                    else:
                        for helperfield in field.HelperFields:
                            var_name_temp = "{}_C{}".format(var_name, re.sub(pattern="^_", repl="", string=helperfield.Name))
                            self.varNames.append(var_name_temp)
                            self.varLabels[var_name_temp] = var_label.encode('utf-8')
                            self.varTypes[var_name_temp] = self.get_datatype(helperfield)
                            self.measureLevels[var_name_temp] = self.get_measure_level(helperfield)

                            self.df_spss[var_name_temp] = self.df[helperfield.FullName]

    def get_categories_list(self, field, show_punching_data=False):
        categories_list = dict()

        if show_punching_data:
            categories_list[0] = "No".encode('utf-8')
            categories_list[1] = "Yes".encode('utf-8')            
        else: 
            for cat in field.Categories:
                if not cat.IsOtherLocal:
                    cat_name = cat.Name[1:len(cat.Name)] if cat.Name[0:1] in ["_"] else cat.Name
                    cat_name = int(cat_name if cat_name.isnumeric() else field.Categories[cat.Name].Properties["value"])

                    #cat_name = int(cat.Name[1:len(cat.Name)] if cat.Name[0:1] in ["_"] else cat.Name)
                    categories_list[cat_name] = cat.Label.encode('utf-8')
        
        return categories_list

    def get_measure_level(self, field):
        match field.DataType:
            case dataTypeConstants.mtLong.value | dataTypeConstants.mtDouble.value:
                return "scale"
            case dataTypeConstants.mtText.value:
                return "nominal"
            case _:
                return "nominal"
            
    def get_datatype(self, field):
        match field.DataType:
            case dataTypeConstants.mtLong.value | dataTypeConstants.mtDouble.value:
                return 0
            case dataTypeConstants.mtText.value:
                if field.LevelDepth == 1:
                    return int(self.df[field.FullName].str.encode('utf-8').replace(r'\xcd',r'\xcc\xa6').str.decode('utf-8').str.len().max())
                else:
                    return int(self.df[field.FullName.replace("..", field.CurrentIndexPath)].fillna("").str.len().max())
            case dataTypeConstants.mtDate.value:
                return 0
            case _:
                return 0

    def get_iteration_name(self, iterations=list()):
        labels = list()

        def get_label(index):
            if index < 0:
                return

            labels.append(iterations[index].Name)
            get_label(index - 1) 
        
        get_label(len(iterations) - 1)

        ls = labels[1:len(labels)]
        ls.append(labels[0])
        return "_".join([l if l[0:1] not in ["_"] else l[1:len(l)] for l in ls])
          
    def get_variable_name(self, field, iterations=list()):
        if len(iterations) == 0:
            var_name = field.FullName if field.Properties["py_setColumnName"] is None else field.Properties["py_setColumnName"]
        else:
            if field.Properties["py_setColumnName"] is None:
                var_name = field.FullName.replace("..","%s") % tuple(field.CurrentIndexPath.split(','))
            else:
                var_name = field.Properties["py_setColumnName"] % tuple(re.sub(pattern="[\{\}]",repl='',string=field.CurrentIndexPath).split(','))
            
            #var_name = "{}_{}".format(var_name, self.get_iteration_name(iterations))

        #var_name = var_name if var_name[0:1] not in ["_"] else var_name[1:len(var_name)]
        
        #if field.UsageType == categoryUsageConstants.vtVariable.value:
        #    var_name = var_name.replace(".", "_")
                
        return var_name
        
    def replaceLabel(self, label):
        blacklist = ["SHOWTABLET", "SHOW TABLET", "SHOWTABLET THANG ĐIỂM", "SHOWPHOTO"]
        s = re.sub(pattern=".*(?=({}))".format("|".join(blacklist)), repl="", string=label)
        s = re.sub(pattern="({})".format("|".join(blacklist)), repl="", string=s)
        return s 

    def get_iteration_label(self, iterations=list()):
        labels = list()

        def get_label(index):
            if index < 0:
                return

            labels.append(iterations[index].Label)
            get_label(index - 1) 
        
        get_label(len(iterations) - 1)
        return "_".join(labels)

    def get_variable_label(self, field, iterations=list()):
        m = re.match(pattern="^(.*)[\.](.*)\?", string= field.Label)
        s = field.Label if m is None else field.Label[m.regs[0][0]:m.regs[0][1]]
        
        if len(iterations) > 0:
            s = "{} - {}".format(s, self.get_iteration_label(iterations=iterations))
        return s

    def to_spss(self):
        with savReaderWriter.SavWriter(self.mdd_file.replace(".mdd", "_SPSS.sav"), varNames=self.varNames, missingValues=self.varMissingValues, varTypes=self.varTypes, formats=self.var_date_formats, varLabels=self.varLabels, measureLevels=self.measureLevels, valueLabels=self.valueLabels, ioUtf8=True) as writer:
            for i, row in self.df_spss.iterrows():
                for v in self.var_dates:    
                    if re.match(pattern="(.*)DATE(.*)", string=v):
                        try:
                            d = datetime.strptime(row[v], "%m/%d/%Y")
                            row[v] = writer.spssDateTime(datetime.strftime(d, "%m/%d/%Y").encode('utf-8'), "%m/%d/%Y")
                        except:
                            row[v] = np.nan
                        
                    if re.match(pattern="(.*)TIME(.*)", string=v):
                        try:
                            d = datetime.strptime(row[v], "%H:%M:%S")
                            row[v] = writer.convertTime(d.day, d.hour, d.minute, d.second)
                        except:
                            row[v] = np.nan

                writer.writerow(list(row))