from datetime import datetime, timedelta
import config

try:
    now = datetime.now()
    print("start_time ...", now)

    mongoConnect = config.get_mongo_client()
    formResponse_collection = mongoConnect.get_database(config.mongo_db).get_collection("formResponse")
    formBuilder_collection = mongoConnect.get_database(config.mongo_db).get_collection("formBuilder")


    config.mysql_cursor.execute("CREATE TABLE IF NOT EXISTS makePostOpCall (id INT AUTO_INCREMENT PRIMARY KEY, mongo_id TEXT, service_request_id TEXT, created_by_user TEXT, patient_name TEXT, patient_DOB TEXT, surgery_date TEXT ,surgery_name TEXT, surgeon TEXT, phoneNo TEXT, alternate_phoneNo TEXT, anesthesia_type TEXT, clinical_pathways TEXT, rn_name_1 TEXT, rn_date_1 TEXT, rn_time_1 TEXT, follow_up_status_1 TEXT, spoke_with1 TEXT, other_relation1 TEXT, rn_name_2 TEXT, rn_date_2 TEXT, rn_time_2 TEXT, follow_up_status_2 TEXT, spoke_with2 TEXT, other_relation2 TEXT, rn_name_3 TEXT, rn_date_3 TEXT, rn_time_3 TEXT, follow_up_status_3 TEXT, spoke_with3 TEXT, other_relation3 TEXT, doNotCall TEXT, doNotCall_reason TEXT, patient_resumed_activity_daily_living_radio TEXT, patient_resumed_activity_daily_living_text TEXT, patient_resumed_normal_diet_radio TEXT, patient_resumed_normal_diet_text TEXT, patient_experienced_nausea_radio TEXT, patient_experienced_nausea_text TEXT, vomiting_radio TEXT, patient_had_question_anesthesia_radio TEXT, patient_had_question_anesthesia_text TEXT, patient_had_pain_from_surgery_radio TEXT, patient_had_pain_from_surgery_text TEXT, patient_relieved_by_medication_radio TEXT, patient_relieved_by_medication_text TEXT, patient_consuming_prescribed_medication_date TEXT, patient_consuming_prescribed_medication_time TEXT, patient_following_medication_by_surgeon_radio TEXT, patient_following_medication_by_surgeon_text TEXT, patient_has_question_about_medication_radio TEXT, patient_has_question_about_medication_text TEXT, dressing_cast_radio TEXT, intact_radio TEXT, intact_text TEXT, presence_drainage_radio TEXT, presence_drainage_text TEXT, mascular_discomfort TEXT, elevated_temperature TEXT, pain_at_IV_site TEXT, numbness TEXT, sore_throat TEXT, difficulty_voiding TEXT, comment_on_yes_response TEXT, postoperative_instructions_reinforced TEXT, postoperative_instructions_reinforced_comments TEXT, patient_questions_about_followup_radio TEXT, patient_questions_about_followup_text TEXT, patient_impression_positive TEXT, ifPosetiveSpecify TEXT, recognizeThem TEXT, ifNegativeSpecify TEXT, provideBetterExperience TEXT, end_comments TEXT,rnName TEXT, form_date TEXT, form_time TEXT,form_response_id TEXT, status TEXT, patientId TEXT, lastUpdatedBy TEXT, markToBeDeleted BOOLEAN, mongo_date_created DATETIME, mongo_last_updated DATETIME, created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP, updated_datetime DATETIME ON UPDATE CURRENT_TIMESTAMP)")

    try:
        makePostOpCallIds = [doc["_id"] for doc in formBuilder_collection.find({"module": "makePostOpCall"}, {"_id": 1})]

        print(f"Going to fetch latest mongo_last_updated in makePostOpCall Table")

        config.mysql_cursor.execute("SELECT mongo_last_updated FROM makePostOpCall ORDER BY mongo_last_updated DESC LIMIT 1")
        latest_record = config.mysql_cursor.fetchone()

        if latest_record:
            latest_mongo_lastUpdated = latest_record[0]
            print(f"Found latest mongo_last_updated in makePostOpCall as {latest_mongo_lastUpdated}")
            print(f"Going to fetch data from formResponse collection for makePostOpCall after  {latest_mongo_lastUpdated}")

            # Your code here
            # Modify mongo_query to include a condition on lastUpdated
            mongo_query = {"form.reference": {"$in": makePostOpCallIds},"lastUpdated": { "$gt": latest_mongo_lastUpdated }}
            print("Updated MongoDB query with lastUpdated condition:", mongo_query)
        else:
            print(f"No records found, Please run legacy script first.---->")

        mongo_results = formResponse_collection.find(mongo_query)
    except Exception as e:
        print("A error occurred while performing mongo operations :", str(e))



    for document in mongo_results:
    # Extract data from MongoDB document
        mongo_id = str(document["_id"])
        print("Document with mongo id",mongo_id)
        service_request_id = str(document.get("basedOn", [{}])[0].get("reference", "")) if document.get("basedOn", [{}])[0] and document.get("basedOn", [{}])[0].get("reference", "")  else ""
        created_by_user = str(document.get("createdByUser", "")) if document.get("createdByUser", "") else ""
        extension_value = document.get("extension", [{}])[0].get("value", [{}])[0].get("formData", {}) if document.get("extension", [{}])[0] and document.get("extension", [{}])[0].get("value", [{}])[0] and document.get("extension", [{}])[0].get("value", [{}])[0].get("formData", {}) else None
        patient_name = str(extension_value.get("patient_name", "")) if extension_value and extension_value.get("patient_name", "") else ""
        # ... Remaining fields ...
        patient_DOB = str(extension_value.get("birth_date", "")) if extension_value and extension_value.get("birth_date", "") else ""
        surgery_date = str(extension_value.get("surgery_date", "")) if extension_value and extension_value.get("surgery_date", "") else ""
        surgery_name = str(extension_value.get("surgery_name", "")) if extension_value and extension_value.get("surgery_name", "") else ""
        surgeon = str(extension_value.get("surgeon", "")) if extension_value and extension_value.get("surgeon", "") else ""
        phoneNo = str(extension_value.get("phoneNo", "")) if extension_value and extension_value.get("phoneNo", "") else ""
        alternate_phoneNo = str(extension_value.get("alternate_phoneNo", "")) if extension_value and extension_value.get("alternate_phoneNo", "") else ""
        anesthesia_type = str(extension_value.get("anesthesia_type", "")) if extension_value and extension_value.get("anesthesia_type", "") else ""
        clinical_pathways = str(extension_value.get("clinical_pathways", "")) if extension_value and extension_value.get("clinical_pathways", "") else ""

        rn_name_1 = str(extension_value.get("rn_name_1", "")) if extension_value and extension_value.get("rn_name_1", "") else ""
        rn_date_1 = str(extension_value.get("rn_date_1", "")) if extension_value and extension_value.get("rn_date_1", "") else ""
        rn_time_1 = str(extension_value.get("rn_time_1", "")) if extension_value and extension_value.get("rn_time_1", "") else ""
        follow_up_status_1 = str(extension_value.get("follow_up_status_1", "")) if extension_value and extension_value.get("follow_up_status_1", "") else ""

        spoke_with1 = str(extension_value.get("spoke_with1", {}).get("value", "")) if extension_value.get("spoke_with1", {}) and extension_value.get("spoke_with1", {}).get("value", "") else ""
        other_relation1 = str(extension_value.get("other_relation1", "")) if extension_value and extension_value.get("other_relation1", "") else ""

        rn_name_2 = str(extension_value.get("rn_name_2", "")) if extension_value and extension_value.get("rn_name_2", "") else ""
        rn_date_2 = str(extension_value.get("rn_date_2", "")) if extension_value and extension_value.get("rn_date_2", "") else ""
        rn_time_2 = str(extension_value.get("rn_time_2", "")) if extension_value and extension_value.get("rn_time_2", "") else ""
        follow_up_status_2 = str(extension_value.get("follow_up_status_2", "")) if extension_value and extension_value.get("follow_up_status_2", "") else ""

        spoke_with2 = str(extension_value.get("spoke_with2", {}).get("value", "")) if extension_value.get("spoke_with2", {}) and extension_value.get("spoke_with2", {}).get("value", "") else ""
        other_relation2 = str(extension_value.get("other_relation2", "")) if extension_value and extension_value.get("other_relation2", "") else ""

        rn_name_3 = str(extension_value.get("rn_name_3", "")) if extension_value and extension_value.get("rn_name_3", "") else ""
        rn_date_3 = str(extension_value.get("rn_date_3", "")) if extension_value and extension_value.get("rn_date_3", "") else ""
        rn_time_3 = str(extension_value.get("rn_time_3", "")) if extension_value and extension_value.get("rn_time_3", "") else ""
        follow_up_status_3 = str(extension_value.get("follow_up_status_3", "")) if extension_value and extension_value.get("follow_up_status_3", "") else ""
        spoke_with3 = str(extension_value.get("spoke_with3", {}).get("value", "")) if extension_value.get("spoke_with3", {}) and extension_value.get("spoke_with3", {}).get("value", "") else ""
        other_relation3 = str(extension_value.get("other_relation3", "")) if extension_value and extension_value.get("other_relation3", "") else ""

        doNotCall = str(extension_value.get("doNotCall", "")) if extension_value and extension_value.get("doNotCall", "") else ""
        doNotCall_reason = str(extension_value.get("reason", "")) if extension_value and extension_value.get("reason", "") else ""
        patient_resumed_activity_daily_living_radio = str(extension_value.get("patient_resumed_activity_daily_living_radio", "")) if extension_value and extension_value.get("patient_resumed_activity_daily_living_radio", "") else ""
        patient_resumed_activity_daily_living_text = str(extension_value.get("patient_resumed_activity_daily_living_text", "")) if extension_value and extension_value.get("patient_resumed_activity_daily_living_text", "") else ""

        patient_resumed_activity_daily_living_radio = str(extension_value.get("patient_resumed_activity_daily_living_radio", "")) if extension_value and extension_value.get("patient_resumed_activity_daily_living_radio", "") else ""
        patient_resumed_activity_daily_living_text = str(extension_value.get("patient_resumed_activity_daily_living_text", "")) if extension_value and extension_value.get("patient_resumed_activity_daily_living_text", "") else ""
        patient_resumed_normal_diet_radio = str(extension_value.get("patient_resumed_normal_diet_radio", "")) if extension_value and extension_value.get("patient_resumed_normal_diet_radio", "") else ""
        patient_resumed_normal_diet_text = str(extension_value.get("patient_resumed_normal_diet_text", "")) if extension_value and extension_value.get("patient_resumed_normal_diet_text", "") else ""
        patient_experienced_nausea_radio = str(extension_value.get("patient_experienced_nausea_radio", "")) if extension_value and extension_value.get("patient_experienced_nausea_radio", "") else ""
        patient_experienced_nausea_text = str(extension_value.get("patient_experienced_nausea_text", "")) if extension_value and extension_value.get("patient_experienced_nausea_text", "") else ""

        vomiting_radio = str(extension_value.get("vomiting_radio", "")) if extension_value and extension_value.get("vomiting_radio", "") else ""
        patient_had_question_anesthesia_radio = str(extension_value.get("patient_had_question_anesthesia_radio", "")) if extension_value and extension_value.get("patient_had_question_anesthesia_radio", "") else ""
        patient_had_question_anesthesia_text = str(extension_value.get("patient_had_question_anesthesia_text", "")) if extension_value and extension_value.get("patient_had_question_anesthesia_text", "") else ""

        patient_had_pain_from_surgery_radio = str(extension_value.get("patient_had_pain_from_surgery_radio", "")) if extension_value and extension_value.get("patient_had_pain_from_surgery_radio", "") else ""
        patient_had_pain_from_surgery_text = str(extension_value.get("patient_had_pain_from_surgery_text", "")) if extension_value and extension_value.get("patient_had_pain_from_surgery_text", "") else ""

        patient_relieved_by_medication_radio = str(extension_value.get("patient_relieved_by_medication_radio", "")) if extension_value and extension_value.get("patient_relieved_by_medication_radio", "") else ""
        patient_relieved_by_medication_text = str(extension_value.get("patient_relieved_by_medication_text", "")) if extension_value and extension_value.get("patient_relieved_by_medication_text", "") else ""

        patient_consuming_prescribed_medication_date = str(extension_value.get("patient_consuming_prescribed_medication_date", "")) if extension_value and extension_value.get("patient_consuming_prescribed_medication_date", "") else ""
        patient_consuming_prescribed_medication_time = str(extension_value.get("patient_consuming_prescribed_medication_time", "")) if extension_value and extension_value.get("patient_consuming_prescribed_medication_time", "") else ""

        patient_following_medication_by_surgeon_radio = str(extension_value.get("patient_following_medication_by_surgeon_radio", "")) if extension_value and extension_value.get("patient_following_medication_by_surgeon_radio", "") else ""
        patient_following_medication_by_surgeon_text = str(extension_value.get("patient_following_medication_by_surgeon_text", "")) if extension_value and extension_value.get("patient_following_medication_by_surgeon_text", "") else ""

        patient_has_question_about_medication_radio = str(extension_value.get("patient_has_question_about_medication_radio", "")) if extension_value and extension_value.get("patient_has_question_about_medication_radio", "") else ""
        patient_has_question_about_medication_text = str(extension_value.get("patient_has_question_about_medication_text", "")) if extension_value and extension_value.get("patient_has_question_about_medication_text", "") else ""

        dressing_cast_radio = str(extension_value.get("dressing_cast_radio", "")) if extension_value and extension_value.get("dressing_cast_radio", "") else ""
        intact_radio = str(extension_value.get("intact_radio", "")) if extension_value and extension_value.get("intact_radio", "") else ""
        intact_text = str(extension_value.get("intact_text", "")) if extension_value and extension_value.get("intact_text", "") else ""


        presence_drainage_radio = str(extension_value.get("presence_drainage_radio", "")) if extension_value and extension_value.get("presence_drainage_radio", "") else ""
        presence_drainage_text = str(extension_value.get("presence_drainage_text", "")) if extension_value and extension_value.get("presence_drainage_text", "") else ""

        mascular_discomfort = str(extension_value.get("mascular_discomfort", "")) if extension_value and extension_value.get("mascular_discomfort", "") else ""
        elevated_temperature = str(extension_value.get("elevated_temperature", "")) if extension_value and extension_value.get("elevated_temperature", "") else ""
        pain_at_IV_site = str(extension_value.get("pain_at_IV_site", "")) if extension_value and extension_value.get("pain_at_IV_site", "") else ""
        numbness = str(extension_value.get("numbness", "")) if extension_value and extension_value.get("numbness", "") else ""
        sore_throat = str(extension_value.get("sore_throat", "")) if extension_value and extension_value.get("sore_throat", "") else ""
        difficulty_voiding = str(extension_value.get("difficulty_voiding", "")) if extension_value and extension_value.get("difficulty_voiding", "") else ""
        comment_on_yes_response = str(extension_value.get("comment_on_yes_response", "")) if extension_value and extension_value.get("comment_on_yes_response", "") else ""

        postoperative_instructions_reinforced = str(extension_value.get("postoperative_instructions_reinforced", "")) if extension_value and extension_value.get("postoperative_instructions_reinforced", "") else ""
        postoperative_instructions_reinforced_comments = str(extension_value.get("postoperative_instructions_reinforced_comments", "")) if extension_value and extension_value.get("postoperative_instructions_reinforced_comments", "") else ""
        patient_questions_about_followup_radio = str(extension_value.get("patient_questions_about_followup_radio", "")) if extension_value and extension_value.get("patient_questions_about_followup_radio", "") else ""
        patient_questions_about_followup_text = str(extension_value.get("patient_questions_about_followup_text", "")) if extension_value and extension_value.get("patient_questions_about_followup_text", "") else ""
        patient_impression_positive = str(extension_value.get("patient_impression_positive", "")) if extension_value and extension_value.get("patient_impression_positive", "") else ""
        ifPosetiveSpecify = str(extension_value.get("ifPosetiveSpecify", "")) if extension_value and extension_value.get("ifPosetiveSpecify", "") else ""
        recognizeThem = str(extension_value.get("recognizeThem", "")) if extension_value and extension_value.get("recognizeThem", "") else ""
        ifNegativeSpecify = str(extension_value.get("ifNegativeSpecify", "")) if extension_value and extension_value.get("ifNegativeSpecify", "") else ""
        provideBetterExperience = str(extension_value.get("provideBetterExperience", "")) if extension_value and extension_value.get("provideBetterExperience", "") else ""
        end_comments = str(extension_value.get("comments", "")) if extension_value and extension_value.get("comments", "") else ""


        rnName = str(extension_value.get("RNname", "")) if extension_value and extension_value.get("RNname", "") else ""
        form_date = str(extension_value.get("date", "")) if extension_value and extension_value.get("date", "") else ""
        form_time = str(extension_value.get("time", "")) if extension_value and extension_value.get("time", "") else ""

        form_response_id = str(document.get("identifier", [{}])[0].get("value", {}).get("value", "")) if document.get("identifier", [{}])[0] and document.get("identifier", [{}])[0].get("value", {}) and document.get("identifier", [{}])[0].get("value", {}).get("value", "") else ""
        status = str(document.get("status", {}).get("value", "")) if document.get("status", {}) and document.get("status", {}).get("value", "") else ""  # Update the default value to an empty string
        patientId = str(document.get("subject", "")) if document.get("subject", "") else ""
        markToBeDeleted = document.get("markToBeDeleted", "") if document.get("markToBeDeleted", "") else ""
        lastUpdatedBy = str(document.get("lastUpdatedBy", {}).get("reference", "")) if document.get("lastUpdatedBy", {}) and document.get("lastUpdatedBy", {}).get("reference", "") else ""
        mongo_date_created = document.get("dateCreated", "") if document.get("dateCreated", "") else ""
        mongo_last_updated = document.get("lastUpdated", "") if document.get("lastUpdated", "") else ""

    # ... (your code before the loop)

    # Fetch existing record
        config.mysql_cursor.execute("SELECT id FROM makePostOpCall WHERE mongo_id = %s", (mongo_id,))
        existing_record = config.mysql_cursor.fetchone()

        if existing_record:
        # If the record exists, perform an UPDATE operation
            print(f"Found an existing record which has been updated after {latest_mongo_lastUpdated} ......So updating")
            try:
                config.mysql_cursor.execute("UPDATE makePostOpCall SET service_request_id = %s, created_by_user = %s, patient_name = %s, patient_DOB = %s, surgery_date = %s, surgery_name = %s, surgeon = %s, phoneNo = %s, alternate_phoneNo = %s, anesthesia_type = %s, clinical_pathways = %s, rn_name_1 = %s, rn_date_1 = %s, rn_time_1 = %s, follow_up_status_1 = %s, spoke_with1 = %s, other_relation1 = %s, rn_name_2 = %s, rn_date_2 = %s, rn_time_2 = %s, follow_up_status_2 = %s, spoke_with2 = %s, other_relation2 = %s, rn_name_3 = %s, rn_date_3 = %s, rn_time_3 = %s, follow_up_status_3 = %s, spoke_with3 = %s, other_relation3 = %s, doNotCall = %s, doNotCall_reason = %s, patient_resumed_activity_daily_living_radio = %s, patient_resumed_activity_daily_living_text = %s, patient_resumed_normal_diet_radio = %s, patient_resumed_normal_diet_text = %s, patient_experienced_nausea_radio = %s, patient_experienced_nausea_text = %s, vomiting_radio = %s, patient_had_question_anesthesia_radio = %s, patient_had_question_anesthesia_text = %s, patient_had_pain_from_surgery_radio = %s, patient_had_pain_from_surgery_text = %s, patient_relieved_by_medication_radio = %s, patient_relieved_by_medication_text = %s, patient_consuming_prescribed_medication_date = %s, patient_consuming_prescribed_medication_time = %s, patient_following_medication_by_surgeon_radio = %s, patient_following_medication_by_surgeon_text = %s, patient_has_question_about_medication_radio = %s, patient_has_question_about_medication_text = %s, dressing_cast_radio = %s, intact_radio = %s, intact_text = %s, presence_drainage_radio = %s, presence_drainage_text = %s, mascular_discomfort = %s, elevated_temperature = %s, pain_at_IV_site = %s, numbness = %s, sore_throat = %s, difficulty_voiding = %s, comment_on_yes_response = %s, postoperative_instructions_reinforced = %s, postoperative_instructions_reinforced_comments = %s, patient_questions_about_followup_radio = %s, patient_questions_about_followup_text = %s, patient_impression_positive = %s, ifPosetiveSpecify = %s, recognizeThem = %s, ifNegativeSpecify = %s, provideBetterExperience = %s, end_comments = %s, rnName = %s, form_date = %s, form_time = %s, form_response_id = %s, status = %s, patientId = %s, lastUpdatedBy = %s, markToBeDeleted = %s, mongo_date_created = %s, mongo_last_updated = %s WHERE mongo_id = %s ",(service_request_id, created_by_user, patient_name, patient_DOB, surgery_date, surgery_name, surgeon, phoneNo, alternate_phoneNo, anesthesia_type, clinical_pathways, rn_name_1, rn_date_1, rn_time_1, follow_up_status_1, spoke_with1, other_relation1, rn_name_2, rn_date_2, rn_time_2, follow_up_status_2, spoke_with2, other_relation2, rn_name_3, rn_date_3, rn_time_3, follow_up_status_3, spoke_with3, other_relation3, doNotCall, doNotCall_reason, patient_resumed_activity_daily_living_radio, patient_resumed_activity_daily_living_text, patient_resumed_normal_diet_radio, patient_resumed_normal_diet_text, patient_experienced_nausea_radio, patient_experienced_nausea_text, vomiting_radio, patient_had_question_anesthesia_radio, patient_had_question_anesthesia_text, patient_had_pain_from_surgery_radio, patient_had_pain_from_surgery_text, patient_relieved_by_medication_radio, patient_relieved_by_medication_text, patient_consuming_prescribed_medication_date, patient_consuming_prescribed_medication_time, patient_following_medication_by_surgeon_radio, patient_following_medication_by_surgeon_text, patient_has_question_about_medication_radio, patient_has_question_about_medication_text, dressing_cast_radio, intact_radio, intact_text, presence_drainage_radio, presence_drainage_text, mascular_discomfort, elevated_temperature, pain_at_IV_site, numbness, sore_throat, difficulty_voiding, comment_on_yes_response, postoperative_instructions_reinforced, postoperative_instructions_reinforced_comments, patient_questions_about_followup_radio, patient_questions_about_followup_text, patient_impression_positive, ifPosetiveSpecify,recognizeThem, ifNegativeSpecify, provideBetterExperience, end_comments, rnName, form_date, form_time, form_response_id, status, patientId,lastUpdatedBy, markToBeDeleted,  mongo_date_created, mongo_last_updated,mongo_id))
                print("Executed UPDATE for document with mongo_id:", mongo_id)
            except Exception as e:
                print(f"Error found {str(e)} while updating for document with mongo Id {mongo_id}")
        else:
        # If the record doesn't exist, perform an INSERT operation
            print("Found new record so inserting with Mongo-Id ",mongo_id)
            try:
                config.mysql_cursor.execute(
                """
                INSERT INTO makePostOpCall (mongo_id, service_request_id, created_by_user, patient_name, patient_DOB, surgery_date, surgery_name, surgeon, phoneNo, alternate_phoneNo, anesthesia_type, clinical_pathways, rn_name_1, rn_date_1, rn_time_1, follow_up_status_1,spoke_with1, other_relation1, rn_name_2, rn_date_2, rn_time_2, follow_up_status_2, spoke_with2, other_relation2, rn_name_3, rn_date_3, rn_time_3, follow_up_status_3, spoke_with3, other_relation3, doNotCall, doNotCall_reason, patient_resumed_activity_daily_living_radio, patient_resumed_activity_daily_living_text, patient_resumed_normal_diet_radio, patient_resumed_normal_diet_text, patient_experienced_nausea_radio, patient_experienced_nausea_text, vomiting_radio, patient_had_question_anesthesia_radio, patient_had_question_anesthesia_text, patient_had_pain_from_surgery_radio, patient_had_pain_from_surgery_text, patient_relieved_by_medication_radio, patient_relieved_by_medication_text, patient_consuming_prescribed_medication_date, patient_consuming_prescribed_medication_time, patient_following_medication_by_surgeon_radio, patient_following_medication_by_surgeon_text, patient_has_question_about_medication_radio, patient_has_question_about_medication_text, dressing_cast_radio, intact_radio, intact_text, presence_drainage_radio, presence_drainage_text, mascular_discomfort, elevated_temperature, pain_at_IV_site, numbness, sore_throat, difficulty_voiding, comment_on_yes_response, postoperative_instructions_reinforced, postoperative_instructions_reinforced_comments, patient_questions_about_followup_radio, patient_questions_about_followup_text, patient_impression_positive, ifPosetiveSpecify, recognizeThem, ifNegativeSpecify, provideBetterExperience, end_comments, rnName, form_date, form_time, form_response_id, status, patientId, lastUpdatedBy, markToBeDeleted, mongo_date_created, mongo_last_updated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    mongo_id, service_request_id, created_by_user, patient_name, patient_DOB, surgery_date, surgery_name, surgeon, phoneNo, alternate_phoneNo, anesthesia_type, clinical_pathways, rn_name_1, rn_date_1, rn_time_1, follow_up_status_1, spoke_with1, other_relation1, rn_name_2, rn_date_2, rn_time_2, follow_up_status_2, spoke_with2, other_relation2, rn_name_3, rn_date_3, rn_time_3, follow_up_status_3, spoke_with3, other_relation3, doNotCall, doNotCall_reason, patient_resumed_activity_daily_living_radio, patient_resumed_activity_daily_living_text, patient_resumed_normal_diet_radio, patient_resumed_normal_diet_text, patient_experienced_nausea_radio, patient_experienced_nausea_text, vomiting_radio, patient_had_question_anesthesia_radio, patient_had_question_anesthesia_text, patient_had_pain_from_surgery_radio, patient_had_pain_from_surgery_text, patient_relieved_by_medication_radio, patient_relieved_by_medication_text, patient_consuming_prescribed_medication_date, patient_consuming_prescribed_medication_time, patient_following_medication_by_surgeon_radio, patient_following_medication_by_surgeon_text, patient_has_question_about_medication_radio, patient_has_question_about_medication_text, dressing_cast_radio, intact_radio, intact_text, presence_drainage_radio, presence_drainage_text, mascular_discomfort, elevated_temperature, pain_at_IV_site, numbness, sore_throat, difficulty_voiding, comment_on_yes_response, postoperative_instructions_reinforced, postoperative_instructions_reinforced_comments, patient_questions_about_followup_radio, patient_questions_about_followup_text, patient_impression_positive, ifPosetiveSpecify,recognizeThem, ifNegativeSpecify, provideBetterExperience, end_comments, rnName, form_date, form_time, form_response_id, status, patientId,lastUpdatedBy, markToBeDeleted,  mongo_date_created, mongo_last_updated,
                )
                )
                print("Executed INSERT document with mongo_id:", mongo_id)

            except Exception as e:
                print(f"Error found {str(e)} while inserting for document with mongo Id {mongo_id}")

    config.mysql_connection.commit()

    now = datetime.now()
    print("end_time ...", now)
    print("processed_count ...", mongo_results.count())


except Exception as e:
    print("A final error occurred as :", str(e))

finally:
    config.mysql_cursor.close()
    config.mysql_connection.close()
    mongoConnect.close()
