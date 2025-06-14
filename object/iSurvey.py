import numpy as np
import pandas as pd
import re
import xml.etree.ElementTree as ET
from object.enumerations import dataTypeConstants

class iSurvey(dict):
    def __init__(self, xml_file):
        self.__dict__ = dict()
        self.openXML(xml_file)

    def openXML(self, xml_file):
        tree = ET.parse(xml_file)
        root = tree.getroot()

        header = root.find('header')
        
        self["properties"] = header.find('surveyProperties').attrib
        self["title"] = header.find('title').text
        self["subtitle"] = header.find('subTitle').text 
        self["survey_family"] = header.find('surveyFamily').text 

        answersref = root.find('answersRef')
        self["answersref"] = iAnswersRef(answersref)

        body = root.find('body')
        self["definesref"] = iDefines(body, self["answersref"])

        try:
            self["questions"] = iQuestions(body, self["answersref"], self["definesref"])

            globalization = root.find('globalization')
            self["globalization"] = iGlobalization(globalization)
        except Exception as e:
            raise Exception("Error in iSurvey: {}".format(e))

class iGlobalization(dict):
    def __init__(self, globalization):
        self.languages = list()
        self.generate(globalization)

    def generate(self, globalization):
        
        for g in globalization.findall('.//g[@o]'):
            variable_map = g.attrib["o"]
            
            if len(variable_map) > 0:
                if variable_map not in self.keys():
                    self[variable_map] = dict()

                for l in globalization.find('languages'):
                    l_text = g.find(f't[@l="{l.attrib["description"]}"]').text.strip() if g.find(f't[@l="{l.attrib["description"]}"]') is not None else ""
            
                    if l.attrib["description"] not in self[variable_map].keys():
                        self[variable_map][l.attrib["description"]] = self.get_text(l_text)

                    if l.attrib["description"] not in self.languages:
                        self.languages.append(l.attrib["description"])
    
    def get_text(self, text):
        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()
        # Remove HTML tags
        text = re.sub(pattern='\<(?:\"[^\"]*\"[\'\"]*|\'[^\']*\'[\'\"]*|[^\'\">])+\>', repl="", string=text) 
        # Remove quotes
        text = re.sub(pattern='[\"\']', repl="", string=text)
        # Remove new lines
        text = re.sub(pattern='\n', repl="", string=text)
        # Remove SHOWTABLE | SHOWPHOTO
        text = re.sub(pattern='SHOW\s*(TABLET|PHOTO|SCREEN)', repl="", string=text)

        arrPatterns = [
            r"\w+(?:[\._]\d)*\.?\s+(.+?)\?",
            r"\w+([\._]\d)*(\.\s).+(\?|(\[|\()(OE|SA|MA)(\]|\)))"
        ]

        for pattern in arrPatterns:
            if re.search(pattern=pattern, string=text.lower()):
                m = re.search(pattern=pattern, string=text.lower())

                text = text[m.span()[0]:m.span()[1]]
                break
        
        # Remove resource tags
        m = re.match(pattern="{#resource:(.+)}", string=text)

        if m is not None:
            if len(re.sub(pattern="{#resource:(.+)}", repl="", string=text)) == 0:
                text = re.sub(pattern="{#resource:|#}", repl="", string=text)
            else:
                text = re.sub(pattern="{#resource:(.+)}", repl="", string=text)
        
        return text
    
    def to_excel(self, file_name="globalization.xlsx"):
        df = pd.DataFrame(self)
        df.to_excel(file_name, index=False)

class iDefines(list):
    def __init__(self, body, answersref):
        self.__dict__ = dict()
        self.generate(body, answersref)
    
    def generate(self, body, answersref):
        for question in body.findall('*'):
            if question.tag not in ["sectionStart", "loopStart","sectionEnd", "loopEnd"]:
                if question.attrib["surveyBuilderV3CMSObjGUID"] in ["F620C65C-1072-4CF0-B293-A9C9012F5BE8"]:
                    self.append(question.attrib["objectName"])

class iQuestions(dict):
    def __init__(self, body, answersref, definesref):
        self.__dict__ = dict()
        self.generate(body, answersref, definesref)

    def generate(self, body, answersref, definesref):
        try:
            parent_nodes = list()
            
            elements = list(enumerate(body.findall('*')))

            for i, question in enumerate(body.findall('*')):
                if question.attrib['pos'] == "258":
                    a = ""

                if question.tag in ["sectionEnd", "loopEnd"]:
                    if len(parent_nodes) > 0: parent_nodes.pop()
                else:
                    if question.attrib["surveyBuilderV3CMSObjGUID"] not in ["101622D0-8B7C-4DE5-B97B-67D33C2E51D7","0AB35540-8549-42F2-A4C4-EA793334170F"]:
                        object_name = question.attrib["objectName"]
                        
                        if len(parent_nodes) > 0:
                            if i < len(elements) - 1:
                                if elements[int(question.attrib['pos'])][1].tag in ["loopEnd"]:
                                    self[".".join([q["attributes"]["objectName"] for q in parent_nodes])]["is_grid"] = True
                                         
                            object_name = "{}.{}".format(".".join([q["attributes"]["objectName"] for q in parent_nodes]), object_name)

                        self[object_name] = iQuestion(question, answersref, definesref, parent_nodes=parent_nodes)
                        
                        if len(parent_nodes) > 0:
                            self[".".join([q["attributes"]["objectName"] for q in self[object_name]["parents"]])]["number_of_question"] += 1

                        if question.tag in ["sectionStart", "loopStart"]:
                            parent_nodes.append(self[object_name])
 
        except:
            raise Exception("Error {}".format(object_name))

class iQuestion(dict):
    def __init__(self, question, answersref, definesref, parent_nodes=None):
        self.__dict__ = dict()
        self.generate(question, answersref, definesref, parent_nodes=parent_nodes)
    
    def generate(self, question, answersref, definesref, parent_nodes=None):
        if question.attrib['pos'] == "79":
            a = ""

        self["text"] = self.get_text(question, parent_nodes=parent_nodes)  

        self["attributes"] = question.attrib
        
        if question.find('answers') is not None:
            self["answers"] = iAnswers(question.find('answers'), answersref, definesref)
            
        if len(parent_nodes) > 0:
            self["parents"] = list()
            self["parents"].extend(parent_nodes)
        
        self["is_defined_list"] = False
        
        if question.tag in ["sectionStart", "loopStart"]:
            self["number_of_question"] = 0
        
        self["is_loop"] = question.tag in ["loopStart"]
        self["is_section"] = question.tag in ["sectionStart"]
        self["is_grid"] = False    

        match question.attrib["surveyBuilderV3CMSObjGUID"]:
            case "8642F4F1-E3E3-480C-89C8-60EDC3DD65FC": #DataType.Text
                self["datatype"] = dataTypeConstants.mtText

                self["comment"] = question.find('comment').attrib
                self["syntax"] = self.syntax_comment()
                
                self["columns"] = self.get_columns()
            case "7AA1B118-B3CA-4112-A4BC-3AFEF497B034": #DataType.Date
                self["datatype"] = dataTypeConstants.mtDate

                self["comment"] = question.find('comment').attrib
                self["syntax"] = self.syntax_comment()
                
                self["columns"] = self.get_columns()
            case "FCE61FC3-99D3-455A-B635-517183475C26": #DataType.Media | DataType.Categorical
                if self["answers"]["attributes"]["answerSetID"] != "8" and self["answers"]["attributes"]["answerSetID"] != "-1":
                    self["datatype"] = dataTypeConstants.mtCategorical
                    self["syntax"] = self.syntax_categorical()

                    self["columns"] = self.get_columns()
                else:
                    self["datatype"] = dataTypeConstants.mtNone
            case "FA4B8A93-09EC-4E23-B45D-FB848C64B834": #DataType.Categorical
                self["datatype"] = dataTypeConstants.mtCategorical
                self["syntax"] = self.syntax_categorical()

                self["columns"] = self.get_columns()
            case "101622D0-8B7C-4DE5-B97B-67D33C2E51D7": #Display.png
                self["datatype"] = dataTypeConstants.mtNone
            case "F620C65C-1072-4CF0-B293-A9C9012F5BE8": #DataType.Define
                self["datatype"] = dataTypeConstants.mtNone
                self["is_defined_list"] = True
                self["syntax"] = self.syntax_define()
            case "2E46C5F3-AF64-4EB9-99D3-E920455F33B6": #DataType.Long | DataType.Double
                self["datatype"] = dataTypeConstants.mtDouble

                self["comment"] = question.find('comment').attrib
                self["syntax"] = self.syntax_comment()
                
                self["columns"] = self.get_columns()
            case "A7C7BA09-0741-4F80-A99F-24C8F045E0B0": #DataType.Loop
                self["datatype"] = dataTypeConstants.mtLevel
                self["objecttype"] = "Loop"

                if self["attributes"]["iterationType"].lower() == 'f':
                    self["syntax"] = self.syntax_loop_numeric(int(self["attributes"]["fixedIterationsCount"]))
                else:
                    self["syntax"] = self.syntax_loop()
            case "59BD961F-E403-4D86-95ED-6A740EEEB16B": #DataType.Loop
                self["datatype"] = dataTypeConstants.mtLevel
                self["objecttype"] = "Loop"
                self["syntax"] = self.syntax_loop()
            case "809CF49C-529D-4336-872A-24BE1C3DC37C": #DataType.BlockFields
                self["datatype"] = dataTypeConstants.mtLevel 
                self["objecttype"] = "BlockFields"
                self["syntax"] = self.syntax_block_fields()
            case "0AB35540-8549-42F2-A4C4-EA793334170F": #Section in iField
                self["datatype"] = dataTypeConstants.mtLevel  
                self["objecttype"] = "BlockFields"
                self["syntax"] = self.syntax_block_fields()
            case "90922453-5C1F-4A6A-BEF2-D4F5A805AD6B": #DataType.Object
                self["datatype"] = dataTypeConstants.mtObject

                self["comment"] = question.find('comment').attrib
                self["comment_syntax"] = self.syntax_general()
                
                if self["answers"]["attributes"]["answerSetID"] != "8" and self["answers"]["attributes"]["answerSetID"] != "-1":
                    if len(self["answers"]["answerref"]["options"]) > 0:
                        self["syntax"] = self.syntax_categorical()

                self["columns"] = self.get_columns()
                
    def syntax_block_fields(self):
        s = "%s \"%s\" %s block fields();" % (
            self["attributes"]["objectName"], 
            self["text"],
            '[ py_setColumnName = "{}" ]'.format(re.sub(pattern="^_", repl="", string=self["attributes"]["objectName"]))
        )

        return s

    def syntax_loop_numeric(self, fixed_iterations_count):
        s = "%s \"%s\" %s loop [1 .. %s] fields () expand grid;" % (
                    self["attributes"]["objectName"], 
                    self["text"],
                    '[ py_setColumnName = "{}" ]'.format(re.sub(pattern="^_", repl="", string=self["attributes"]["objectName"])),
                    fixed_iterations_count
            )
        return s
    
    def syntax_loop(self):
        s = "%s \"%s\" %s loop {%s}fields () expand grid;" % (
                    self["attributes"]["objectName"], 
                    self["text"], 
                    '[ py_setColumnName = "{}" ]'.format(re.sub(pattern="^_", repl="", string=self["attributes"]["objectName"])), 
                    self["answers"]["syntax"]
            )
        return s

    def syntax_define(self):
        s = '%s "" define{%s};' % (
            self["attributes"]["objectName"], 
            self["answers"]["syntax"]
        )
        return s

    def get_properties(self):
        properties = list()

        if "parents" in self.keys() and self["parents"][len(self["parents"]) - 1]["is_grid"]:
            properties.append('py_setColumnName = "{}"'.format(re.sub(pattern="^_", repl="", string=self["parents"][len(self["parents"]) - 1]["attributes"]["objectName"])))
        else:
            properties.append('py_setColumnName = "{}"'.format(re.sub(pattern="^_", repl="", string=self["attributes"]["objectName"])))

        if self["datatype"].value == dataTypeConstants.mtCategorical.value: 
            properties.append('py_showPunchingData = {}'.format("True" if bool(int(self["answers"]["answerref"]["attributes"]["isMultipleSelection"])) else "False"))

            if bool(int(self["answers"]["answerref"]["attributes"]["isMultipleSelection"])):
                properties.append('py_showPunchingData = True')
            else:
                properties.append('py_showPunchingData = False')
                properties.append('py_showVariableValues = "Names"')  

        return properties

    def syntax_categorical(self):
        properties = self.get_properties()

        s = '%s "%s" %s categorical%s{%s}%s;' % (
                self["attributes"]["objectName"], 
                self["text"],
                '[{}]'.format(",".join(properties)), 
                "[1..]" if bool(int(self["answers"]["answerref"]["attributes"]["isMultipleSelection"])) else "[1..1]",
                self["answers"]["syntax"],
                "" if "axis_syntax" not in self["answers"].keys() else self["answers"]["axis_syntax"]
            )
        return s

    def syntax_comment(self):
        properties = self.get_properties()

        datatype = "text"

        match int(self["comment"]["datatype"]):
            case 2:
                datatype = "long" if self["comment"]["scale"] == 0 else "double"
            case 3:
                datatype = "text"
            case 4:
                datatype = "date"
        
        s = '%s "%s" %s %s;' % (
            self["attributes"]["objectName"], 
            self["text"],
            '[{}]'.format(",".join(properties)), 
            datatype
        )

        return s

    def syntax_general(self):
        datatype = "text"

        match int(self["comment"]["datatype"]):
            case 2:
                datatype = "long" if self["comment"]["scale"] == 0 else "double"
            case 3:
                datatype = "text"
            case 4:
                datatype = "date"

        s = '%s "%s" %s %s;' % (
            self["attributes"]["objectName"] + self["comment"]["objectName"],
            self["text"],
            '[ py_setColumnName = "{}" ]'.format(re.sub(pattern="^_", repl="", string=self["attributes"]["objectName"] + self["comment"]["objectName"])),
            datatype
        )

        return s

    def get_text(self, question, parent_nodes=None):
        if question.find('text') is None: 
            return ""

        if len(parent_nodes) == 0:
            text = np.nan if question.find('text') is None else question.find('text').text
        else:
            if parent_nodes[len(parent_nodes) - 1]["is_grid"]:
                text = parent_nodes[len(parent_nodes) - 1]["text"]
            else:
                text = np.nan if question.find('text') is None else question.find('text').text
        
        if question.attrib["objectName"] in ["_PBVC_DEMO"]:
            a = ""
        
        if re.search(pattern='^<([a-zA-Z0-9]+)[^>]*>(.*?)<\/\1>', string=text, flags=re.IGNORECASE):
            text = re.sub(pattern='^<([a-zA-Z0-9]+)[^>]*>(.*?)<\/\1>', repl="", string=text)
        
        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()
        # Remove HTML tags
        text = re.sub(pattern='\<(?:\"[^\"]*\"[\'\"]*|\'[^\']*\'[\'\"]*|[^\'\">])+\>', repl="", string=text) 
        # Remove quotes
        text = re.sub(pattern='[\"\']', repl="", string=text)
        # Remove new lines
        text = re.sub(pattern='\n', repl="", string=text)
        # Remove SHOWTABLE | SHOWPHOTO
        text = re.sub(pattern='SHOW\s*(TABLET|PHOTO)', repl="", string=text)

        object_name = re.sub(pattern='^_', repl="", string=question.attrib["objectName"].lower())

        arrPatterns = [
            r"%s(?:[\._]\d)*\.?\s+(.+?)\?" % (object_name),
            r"%s([\._]\d)*(\.\s).+(\?|(\[|\()(OE|SA|MA)(\]|\)))" % (object_name),
            r"\w+(?:[\._]\d)*\.?\s+(.+?)\?",
            r"\w+([\._]\d)*(\.\s).+(\?|(\[|\()(OE|SA|MA)(\]|\)))"
        ]

        for pattern in arrPatterns:
            if re.search(pattern=pattern, string=text.lower(), flags=re.IGNORECASE):
                m = re.search(pattern=pattern, string=text, flags=re.IGNORECASE)

                text = text[m.span()[0]:m.span()[1]]
                break
        
        # Remove resource tags
        m = re.match(pattern="{#resource:(.+)}", string=text)

        if m is not None:
            if len(re.sub(pattern="{#resource:(.+)}", repl="", string=text)) == 0:
                text = re.sub(pattern="{#resource:|#}", repl="", string=text)
            else:
                text = re.sub(pattern="{#resource:(.+)}", repl="", string=text)
        
        return text

    def generate_columns(self, parent_name=None):
        columns = {}
        
        mdd_col = self["attributes"]["objectName"] if parent_name is None else "%s.%s" % (parent_name, self["attributes"]["objectName"])
        
        if(self["datatype"].value == dataTypeConstants.mtCategorical.value or self["datatype"].value == dataTypeConstants.mtObject.value):
            columns[mdd_col] = dict({
                "csv" : list(),
                "others" : dict(),
                "datatype" : dataTypeConstants.mtCategorical
            })

            if self["answers"]["attributes"]["answerSetID"] != "8" and self["answers"]["attributes"]["answerSetID"] != "-1":
                if len(self["answers"]["answerref"]["options"]) > 0:
                    if bool(int(self["answers"]["answerref"]["attributes"]["isMultipleSelection"])):
                        for key, option in self["answers"]["options"].items():
                            csv_col = "%s.%s" % (re.sub(pattern="(}\])", repl="]", string=re.sub(pattern="(\[{)", repl="[", string=mdd_col)), option["objectname"])
                            
                            if not bool(int(option["attributes"]["isDisplayAsHeader"])):
                                columns[mdd_col]["csv"].append(csv_col)
                            if bool(int(option["attributes"]["isOtherSpecify"])):
                                mdd_other_col = "%s.%s" % (mdd_col, option["objectname"])
                                csv_other_col = "%s.%s.%s" % (
                                                    re.sub(pattern="(}\])", repl="]", string=re.sub(pattern="(\[{)", repl="[", string=mdd_col)), 
                                                    option["objectname"], 
                                                    option["otherfield"]["objectName"])

                                columns[mdd_col]["others"][mdd_other_col] = dict({
                                    "csv" : [csv_other_col],
                                    "datatype" : option["otherfield"]["datatype"]
                                })
                    else:
                        csv_col = re.sub(pattern="(}\])", repl="]", string=re.sub(pattern="(\[{)", repl="[", string=mdd_col))

                        columns[mdd_col]["csv"].append(csv_col)
                    
                        for key, option in self["answers"]["options"].items():
                            if bool(int(option["attributes"]["isOtherSpecify"])):
                                mdd_other_col = "%s.%s" % (mdd_col, option["objectname"])
                                csv_other_col = "%s.%s" % (
                                                    re.sub(pattern="(}\])", repl="]", string=re.sub(pattern="(\[{)", repl="[", string=mdd_col)), 
                                                    option["otherfield"]["objectName"])
                                
                                columns[mdd_col]["others"][mdd_other_col] = dict({
                                    "csv" : [csv_other_col],
                                    "datatype" : option["otherfield"]["datatype"]
                                })
            
            if (self["datatype"].value == dataTypeConstants.mtObject.value):
                columns[f'{mdd_col}{self["comment"]["objectName"]}'] = dict({
                    "csv" : ["%s.%s" % (
                                re.sub(pattern="(}\])", repl="]", string=re.sub(pattern="(\[{)", repl="[", string=mdd_col)),
                                self["comment"]["objectName"])],
                    "datatype" : dataTypeConstants.mtText if int(self["comment"]["datatype"]) == 3 else dataTypeConstants.mtDouble
                })
        else:
            columns[mdd_col] = dict({
                "csv" : [re.sub(pattern="(}\])", repl="]", string=re.sub(pattern="(\[{)", repl="[", string=mdd_col))]
            })
        
        return columns

    def get_columns(self):
        columns = list()

        if "parents" not in self.keys():
            columns.append(self.generate_columns()) 
        else:
            parents = self.get_parents(0)
            
            for parent_name in parents:
                columns.append(self.generate_columns(parent_name=parent_name)) 

        return columns
            
    def get_parents(self, index):
        if index == len(self["parents"]):
            return 
        else:
            p1s = self.get_parent_columns(self["parents"][index])
            p2s = self.get_parents(index + 1)

            if p2s is None:
                return p1s
            else:
                return ["%s.%s" % (p1, p2) for p1 in p1s for p2 in p2s]

    def get_parent_columns(self, parent):
        parent_columns = list()

        if parent["objecttype"] == "BlockFields":
            parent_columns.append(parent["attributes"]["objectName"]) 
        if parent["objecttype"] == "Loop":
            if parent["attributes"]["iterationType"].lower() == 'f':
                parent_columns.extend(['%s[%s]' % (parent["attributes"]["objectName"], i) for i in range(1, 51)])
            else:
                for key, option in parent["answers"]["options"].items():
                    if not bool(int(option["attributes"]["isDisplayAsHeader"])):
                        parent_columns.append('%s[{%s}]' % (parent["attributes"]["objectName"], option['objectname']))

        return parent_columns

class iAnswersRef(dict):
    def __init__(self, answersref):
        self.__dict__ = dict()
        self.generate(answersref)

    def generate(self, answersref):
        for answer in answersref.findall('answer'):
            if answer.attrib["id"] not in self:
                self[answer.attrib["id"]] = dict()
            
            if answer.attrib["id"] == '538411':
                a = ""

            #print(answer.attrib["id"])
            self[answer.attrib["id"]]["attributes"] = answer.attrib
            self[answer.attrib["id"]]["options"] = iOptions(answer.findall('option'))

class iAnswers(dict):
    def __init__(self, answers, answersref, definesref, iterationType=None, fixedIterationsCount=None):
        self.__dict__ = dict()

        if iterationType == 'f' and fixedIterationsCount > 0:
            self.generate_iterations(answers, fixedIterationsCount)
        else:
            self.generate(answers, answersref, definesref)

    def generate(self, answers, answersref, definesref):
        self["attributes"] = answers.attrib
        self["answerref"] = answersref[self["attributes"]["answerSetID"]]
    
        if self["attributes"]["answerSetID"] != '8' and self["attributes"]["answerSetID"] != '-1':
            if answers.find('options') is not None:
                self["options"] = iOptions(answers.find('options').findall('option'), self["answerref"], definesref) 
                self["syntax"] = self.syntax()

                if all(['measure' in opt['attributes'].keys() for opt in self["options"].values()]):
                    self["axis_syntax"] = self.axis_syntax() 

    def axis_syntax(self, options=None):
        s = 'axis("{base(),%s, mean() [Decimals=2], stddev() [Decimals=2]}")' % (
            ",".join([opt['objectname'] for opt in self["options"].values()])
        )
        return s
        

    def generate_iterations(self, answers, fixed_iterations_count):
        self["attributes"] = answers.attrib
        self["syntax"] = self.iteration_syntax(fixed_iterations_count)

    def iteration_syntax(self, fixed_iterations_count):
        return ",".join(["_{} \"{}\"".format(i, i) for i in range(1, 51)])

    def syntax(self):
        s = ""
        pos = 1
        sublist = []

        while pos <= len(self["options"].items()):
            option = self["options"][str(pos)]

            if len(option["answersetreference"]) == 0:
                if option["is_sublist"]:
                    s += ("" if len(s) == 0 else ",") + option["syntax"]
                    g_pos = pos + 1

                    s_child = ""

                    while g_pos <= len(self["options"].items()) and str(self["options"][str(g_pos)]["attributes"]["groupID"]) == str(pos):
                        option_child = self["options"][str(g_pos)]
                        
                        if option_child["is_sublist"]:
                            sub_child_list =[]

                            # s_child += ("" if len(s_child) == 0 else ",") + option_child["syntax"]
                            s_child = option_child["syntax"]
                            g_child_pos = g_pos + 1
                            
                            s_child_child = ""

                            while str(self["options"][str(g_child_pos)]["attributes"]["groupID"]) == str(g_pos):
                                option_child_child = self["options"][str(g_child_pos)]

                                if option_child["is_sublist"]:
                                    sub_child_child_list = []

                                    s_child_child += ("" if len(s_child_child) == 0 else ",") + option_child_child["syntax"]
                                    g_child_child_pos = g_child_pos + 1

                                    while str(self["options"][str(g_child_child_pos)]["attributes"]["groupID"]) == str(g_child_pos):
                                        sub_child_child_list.append(self["options"][str(g_child_child_pos)]["syntax"])
                                        g_child_child_pos += 1

                                    s_child_child = re.sub(pattern="###sublist{}###".format(str(g_child_pos)), repl=",".join(sub_child_child_list), string=s_child_child)

                                    sub_child_child_list = []
                                    g_child_pos = g_child_child_pos
                                else:    
                                    sub_child_list.append(self["options"][str(g_child_pos)]["syntax"])
                                    g_child_pos += 1

                            if len(sub_child_list) > 0:
                                s_child = re.sub(pattern="###sublist{}###".format(str(g_pos)), repl=",".join(sub_child_list), string=s_child)
                            else:
                                s_child = re.sub(pattern="###sublist{}###".format(str(g_pos)), repl=s_child_child, string=s_child)

                                sublist.append(s_child)
                            
                            sub_child_list =[]
                            g_pos = g_child_pos
                        else:
                            sublist.append(self["options"][str(g_pos)]["syntax"])
                            g_pos += 1
                        
                    if len(sublist) > 0:
                        s = re.sub(pattern="###sublist{}###".format(str(pos)), repl=",".join(sublist), string=s)
                    else:
                        s = re.sub(pattern="###sublist{}###".format(str(pos)), repl=s_child, string=s)

                    sublist = []
                    pos = g_pos - 1
                else:
                    s += ("" if len(s) == 0 else ",") + option["syntax"]
                
            pos += 1
        
        return s

class iOptions(dict):
    def __init__(self, *args):
        self.__dict__ = dict()

        if len(args) == 1 and args[0] is not None:
            self.generate(options=args[0])
        if len(args) == 2 and args[0] is not None and args[1] is not None:
            self.generate(options=args[0], answerref=args[1])
        if len(args) == 3 and args[0] is not None and args[1] is not None and args[2] is not None:
            self.generate(options=args[0], answerref=args[1], definesref=args[2])
        
    def generate(self, options=dict(), answerref=dict(), definesref=dict()):
        for option in options:
            if option.attrib["pos"] not in self:
                self[option.attrib["pos"]] = iOption(option, answerref, definesref)

class iOption(dict):
    def __init__(self, *args):
        self.__dict__ = dict()
        self.generate(option=args[0], answerref=args[1], definesref=args[2])
        
    def generate(self, option=dict(), answerref=dict(), definesref=dict()):
        if len(answerref) == 0:
            self["text"] = "" if option.find('text') is None else option.find('text').text
            self["attributes"] = option.attrib
            
            if option.find('otherField') is not None:
                self["otherfield"] = option.find('otherField')
        else:
            self["text"] = self.format_text(answerref["options"][option.attrib["pos"]]["text"])
            self["objectname"] = option.attrib["objectName"]
            self["answersetreference"] = option.attrib["answerSetReference"]
            self["attributes"] = answerref["options"][option.attrib["pos"]]["attributes"]
            self["is_sublist"] = len(option.attrib["answerSetReference"]) == 0 and bool(int(self["attributes"]["isDisplayAsHeader"])) and option.attrib["objectName"] not in definesref
            self["groupname_reference"] = "" if len(self["attributes"]["groupID"]) == 0 else "sublist{}".format(answerref["options"][self["attributes"]["groupID"]]["attributes"]["pos"])

            if option.find('otherField') is not None:
                self["otherfield"] = answerref["options"][option.attrib["pos"]]["otherfield"].attrib
                self["otherfield"]["objectName"] = option.find("otherField").attrib["objectName"]

            self["syntax"] = self.syntax()
            
    def syntax(self):
        if self["is_sublist"]:
            s = '%s "%s" {###sublist%s###}' % (self["objectname"], self["text"], self["attributes"]["pos"])
        elif bool(int(self["attributes"]["isDisplayAsHeader"])):
            s = "use %s" % (self["objectname"])
        else:
            s = '%s "%s" [ pos=%s, value="%s" ]%s' % (
                                self["objectname"], 
                                self["text"], 
                                self["attributes"]['pos'],
                                self["objectname"] if not re.match(pattern='^_(.*)$', string=self["objectname"]) else self["objectname"][1:len(self["objectname"])],
                                "" if "measure" not in self["attributes"].keys() else "factor({})".format(self["attributes"]["measure"]))

            if int(self["attributes"]['isOtherSpecify']) == 1:
                match int(self["otherfield"]['datatype']):
                    case 2:
                        otherdisplaytype = "double"
                    case 3:
                        otherdisplaytype = "text"
                    case 4:
                        otherdisplaytype = "date"

                s = f'{s} other({self["objectname"]} "" {otherdisplaytype})'
                
            if int(self["attributes"]['isExclusive']) == 1:
                s = f'{s} dk'
            if int(self["attributes"]['isExclusive']) == 1:
                s = f'{s} fix'

        return s
    
    def format_text(self, text):
        text = re.sub(pattern='\<(?:\"[^\"]*\"[\'\"]*|\'[^\']*\'[\'\"]*|[^\'\">])+\>', repl="", string=text)
        text = re.sub(pattern='[\"\']', repl="", string=text)
        text = re.sub(pattern='\n', repl="", string=text)

        m = re.search(pattern="{#resource:(.+)}", string=text)

        if m:
            if len(re.sub(pattern="{#resource:(.+)}", repl="", string=text)) == 0:
                text = re.sub(pattern="{#resource:|#}", repl="", string=text)
            else:
                text = re.sub(pattern="{#resource:(.+)}", repl="", string=text)

        return text

        