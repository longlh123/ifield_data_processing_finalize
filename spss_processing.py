import os
from object.spss import SPSSObject

import collections.abc
#hyper needs the four following aliases to be done manually.
collections.Iterable = collections.abc.Iterable
collections.Mapping = collections.abc.Mapping
collections.MutableSet = collections.abc.MutableSet
collections.MutableMapping = collections.abc.MutableMapping

project_name = "VN2024001_F2F_TEST"

mdd_file = "data\VN2025024_ADAM_CE_v1_SPSS.mdd"
ddf_file = "data\VN2025024_ADAM_CE_v1.ddf"

os.chdir("projects\\{}".format(project_name))

sql_query = "SELECT * FROM VDATA" # WHERE InstanceID = 1920291 WHERE InstanceID = 1921762

questions = ["InstanceID","_Quota_City","_AgeGroup_SPSS","_HouseHoldIncome","_S9","S9a_1_SPSS","S9a_2_SPSS","S9a_3_SPSS","S9a_4_SPSS","S9a_5_SPSS","S9a_6_SPSS","S9a_7_SPSS"]

#S10
s10_ids = [1, 2, 4, 5, 6, 7]
questions += [f"_S10[{{_{i}}}]._S10_Codes" for i in s10_ids]

# _A_1 loop
a1_ids = [1, 2, 3, 4, "5b", 6, 7, 8, 9, 11, 12, 15, 17, 18, 19, 22, 23, 24, 25, 30, 31, 32, 33, 34, 35, 36, 27, 37, 29, 38]
questions += [f"_A_1[{{_{i}}}]._A_1_Codes" for i in a1_ids]

#_A_3
questions.append("_A_3")

# _A_4_SPSS
a4_ids = [1, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 24, 25, 26, 27, 28, 29, 30, 22, 31, 32, 23]
questions += [f"_A_4_SPSS[{{_{i}}}]._A_4_Codes" for i in a4_ids]

# _S11 and _S12
questions += [f"_S11_{i}_SPSS" for i in range(1, 8)]
questions += [f"_S12_{i}_SPSS" for i in range(1, 8)]

# _Loop_S11_S12
questions += [f"_Loop_S11_S12[{{_{i}}}]._S12_Segment" for i in range(1, 8)]

# _S12_Segment_COMBINE
questions.append("_S12_Segment_COMBINE")

# _SESSION_3
questions += [f"_SESSION_3[{{_{i}}}]._C1" for i in range(1, 8)]

# _SESSION_4 A10a, D3, A11
for qtype in ["_A10a", "_D3", "_A11"]:
    questions += [f"_SESSION_4[{{_{i}}}].{qtype}" for i in range(1, 8)]

# _SESSION_5._A12_Recode
session_5_ids = list(range(1, 24)) + [9711, 9712, 9713] + list(range(24, 44)) + [9721, 9722, 9723]
questions += [f"_SESSION_5[{{_1}}]._A12_Recode[{{_{i}}}]._A12_Segment" for i in session_5_ids]
questions += [f"_SESSION_5[{{_2}}]._A12_Recode[{{_{i}}}]._A12_Segment" for i in session_5_ids]
questions += [f"_SESSION_5[{{_3}}]._A12_Recode[{{_{i}}}]._A12_Segment" for i in session_5_ids]
questions += [f"_SESSION_5[{{_4}}]._A12_Recode[{{_{i}}}]._A12_Segment" for i in session_5_ids]
questions += [f"_SESSION_5[{{_5}}]._A12_Recode[{{_{i}}}]._A12_Segment" for i in session_5_ids]
questions += [f"_SESSION_5[{{_6}}]._A12_Recode[{{_{i}}}]._A12_Segment" for i in session_5_ids]
questions += [f"_SESSION_5[{{_7}}]._A12_Recode[{{_{i}}}]._A12_Segment" for i in session_5_ids]

#_A9_Recode_Cat
questions += [f"_A9_Recode_Cat{i}" for i in range(1, 8)]

#"_B2_1, A5
for qtype in ["_B2_1", "A5"]:
    questions += [f"{qtype}_{i}_SPSS" for i in range(1, 8)]

#_A5_Recode_Cat
questions += [f"_A5_Recode_Cat{i}" for i in range(1, 8)]

#_A6_Product
questions += [f"_SESSION_5[{{_{i}}}]._A6_Product" for i in range(1, 8)]

#_A8_Product
for i in range(1, 7):
    questions += [f"_SESSION_5[{{_{i}}}]._A8_Product{j}" for j in range(1, 4)]

#_ACT1
questions += ["_ACT1", "_T2"]

#T3
T3_ids = list(range(1, 25)) + [97]
questions += [f"_T3[{{_{i}}}]._T3_Codes" for i in T3_ids]

questions += ["_S40","_S31a","_H2","_H8","_S20","_S22","_S19","_Education","_Occupation","_Married"] 

spssObject = SPSSObject(mdd_file, ddf_file, sql_query, questions)
spssObject.to_spss()

