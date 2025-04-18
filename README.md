<h3>iField data processing</h3>

<blockquote>
<p><b>Step 1</b>: Enter the project name to process the data</p>

<pre>
    <code>
        #Enter the project name to process the data
        project_name = "VN2024001_F2F_TEST"
    </code>
</pre>

<p><b>Step 2</b>: To see local branch</p>

<pre>
    <code>
        git branch
    </code>
</pre>

<p><b>Step 3</b>: To create and move to a new local branch</p>

<pre>
    <code>
        git branch -b my-branch-name
    </code>
</pre>

<u>Note</u>: replace <b>my-branch-name</b> with whatever name you want

<p><b>Step 4</b>: Setup config.json</p>

<ol>
    <li><b>project_name</b>: Enter a project name</li>
    <li><b>run_mdd_source</b>: Select TRUE to create an original mdd/ddf file or FALSE to skip this step.</li>
    <li>
        <b>source_initialization</b>: Thiết lập giá trị mặc định khi tạo một original mdd/ddf file
        <ol>
            <li><b>default_language</b>: Nhập ngôn ngữ mặc định cho file khi khởi tạo, mặc định là VIT</li>
            <li><b>delete_all</b>: Xóa tất cả mã ID cũ trước khi thêm mã ID mới. Nếu bạn không chắc, cứ để mặc định là TRUE (có nghĩa là nó sẽ xóa sạch trước khi thêm mới, tránh trùng lặp)</li>
            <li><b>remove_all_ids</b>: Xóa tất cả mã ID bị hủy (cancelled) hoặc thêm ngoài (extra), mặc định là TRUE (có nghĩa là chỉ để dữ liệu Main, Boosters, Non</li>
            <li><b>dummy_data_required</b>: Nếu bạn cần tạo file chỉ chứa các mã ID mẫu (dummy), thì chọn TRUE. Mặc định là FALSE</li>
        </ol>
    </li>
    <li>
        <b>processing_data</b>: Các bước xử lý dữ liệu đã được tạo và thiết lập ban đầu
        <ol>
            <li><b>run_ce_source</b>: Chạy dữ liệu cho phần CE (Closed-ended)</li>
            <li><b>run_oe_source</b>: Chạy dữ liệu cho phần OE (Open-ended)</li>
        </ol> 
    </li>
    <li><b>respondent_information_columns</b>: Liệt kê tất cả các câu liên quan đến thông tin đáp viên để hệ thống tách ra khỏi dữ liệu gốc</li>
    <li><b>respondent_information_columns_removed</b>: Liệt kê tất cả các câu liên quan đến thông tin đáp viên để hệ thống xóa ra khỏi dữ liệu gốc</li>
    <li>
        <b>axis_expression</b>: định nghĩa AXIS syntax cho từng câu để xử dụng phần tích dữ liệu.
        <pre>
            <code>
                "_AgeGroup" : "{base(),_2,_3,_4,_5, sub_25_40 '25-40 tuổi' combine({_2,_3}), sub_31_50 '31-50 tuổi' combine({_3,_4}), sub_41_60 '41-60 tuổi' combine({_4,_5})}",
                "LOOP_NAME.BLOCK_NAME.QUESTION_NAME" : "{base(),_1,_2,_3,_4,_5,_6,_7,_8,_9,_10, T2B 'T2B' combine({_9,_10}), T3B 'T3B' combine({_8,_9,_10}), mean() [Decimals=2], stddev() [Decimals=2]}" 
            </code>
        </pre>
    </li>
</ol>

<p><b>Step 5</b>: Setup for the F2F/CLT/CATI</p>

<ol>
    <li>
        <b>main</b>:
        <ul>
            <li>
                <b>xmls</b>: Fill in the information for the project's XML files (from the old version to the new version) according to the syntax:
                <pre>
                    <code>
                        "protodid": "file_name.xml"
                    </code>
                </pre>
            </li>
            <li><b>protoid_final</b>: Protoid is used fo create the file original mdd/ddf file</li>
        <ul>
    </li>
</ol>
</blockquote>

<pre>
    <code>
        {
            "project_name" : "--Enter a project name--",
            "run_mdd_source" : true, 
            "source_initialization" : {
                "default_language" : "VIT",
                "delete_all" : true,
                "remove_all_ids" : true,
                "dummy_data_required": true
            },
            "processing_data" : {
                "run_ce_source" : false,
                "run_oe_source" : false
            },
            "main" : {
                "xmls" : {
                    "--Enter a protodid 1--" : "--Enter a xml file--",
                    "--Enter a protodid 2--" : "--Enter a xml file--"
                },
                "protoid_final" : "--Enter a protoid final 2--"
            },
            "stages" : {
                "stage-1" : {
                    "xmls" : {
                        "--Enter a protodid 1--" : "--Enter a xml file--",
                        "--Enter a protodid 2--" : "--Enter a xml file--"
                    },
                    "protoid_final" : "--Enter a protoid final 2--"
                },
                "stage-2" : {
                    "xmls" : {
                        "--Enter a protodid 1--" : "--Enter a xml file--",
                        "--Enter a protodid 2--" : "--Enter a xml file--"
                    },
                    "protoid_final" : "--Enter a protoid final 2--"
                },
                "stage-3" : {
                    "xmls" : {
                        "--Enter a protodid 1--" : "--Enter a xml file--",
                        "--Enter a protodid 2--" : "--Enter a xml file--"
                    },
                    "protoid_final" : "--Enter a protoid final 2--"
                },
                "respondent_information_columns" : {
                    "ResName" : "_ResName",
                    "ResHouseNo" : "_ResHouseNo",
                    "ResStreet" : "_ResStreet",
                    "ResWard" : "_ResWardSelected",
                    "ResDistrict" : "_ResDistrictSelected",
                    "ResProvinces" : "_ResProvinces",
                    "ResPhone" : "_ResPhone._C_1",
                    "ResCellPhone" : "_ResCellPhone._C_1",
                    "Email" : "_Email._C_1"
                },
                "respondent_information_columns_removed" : [
                    "_ResName","_ResAddress","_ResHouseNo","_ResStreet","_ResPhone","_ResPhone._C_1","_ResCellPhone","_ResCellPhone._C_1","_Email","_Email._C_1","_Quota_City ","_Quota_Gender","_Quota_AgeGroup ","_Quota_Class ","_Quota_LSM","SHELL_NAME","SHELL_AGE","SHELL_AGE._A1","SHELL_BLOCK_TEL.SHELL_HOMETEL","SHELL_BLOCK_TEL.SHELL_MOBTEL","SHELL_BLOCK_TEL.SHELL_BC_EMAIL","SHELL_TEL","SHELL_TEXTTEL","SHELL_BLOCK_ADDRESS.SHELL_HOUSENO","SHELL_BLOCK_ADDRESS.SHELL_STREET","SHELL_BLOCK_ADDRESS.SHELL_DISTRICT","SHELL_BLOCK_ADDRESS.SHELL_TOWN","SHELL_BLOCK_ADDRESS.SHELL_ZIP","SHELL_ADDRESS","SHELL_BLOCK_EMAIL.SHELL_IIS_PANEL","SHELL_BLOCK_EMAIL.SHELL_EMAIL","SHELL_BLOCK_EMAIL.SHELL_EMAIL_DUMMY"
                ],
                "axis_expression" : { 
                }
            }
        }
    </code>
</pre>