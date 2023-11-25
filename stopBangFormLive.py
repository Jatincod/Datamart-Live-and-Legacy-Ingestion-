from datetime import datetime, timedelta
import config

try:
    now = datetime.now()
    print("start_time ...", now)

    mongoConnect = config.get_mongo_client()
    formResponse_collection = mongoConnect.get_database(config.mongo_db).get_collection("formResponse")
    formBuilder_collection = mongoConnect.get_database(config.mongo_db).get_collection("formBuilder")
    
    config.mysql_cursor.execute("CREATE TABLE IF NOT EXISTS stopBangForm (id INT AUTO_INCREMENT PRIMARY KEY, mongo_id TEXT, service_request_id TEXT, created_by_user TEXT, patient_name TEXT, patient_DOB TEXT, age TEXT, gender TEXT, height TEXT, weight TEXT, bmi TEXT, surgeon_name TEXT, date_of_surgery TEXT, stop_snore_yes TEXT, stop_snore_no TEXT, stop_daytime_yes TEXT, stop_daytime_no TEXT, stop_sleep_yes TEXT, stop_sleep_no TEXT, stop_bp_yes TEXT, stop_bp_no TEXT, bang_bmi_yes TEXT, bang_bmi_no TEXT, bang_age_yes TEXT, bang_age_no TEXT, bang_ncm_yes TEXT, bang_ncm_no TEXT, bang_gender_yes TEXT, bang_gender_no TEXT, bang_enter TEXT, bang_inch_code TEXT, bang_inch_value TEXT, os_score TEXT, form_date TEXT, form_response_id TEXT, status TEXT, patientId TEXT, lastUpdatedBy TEXT, markToBeDeleted BOOLEAN, mongo_date_created DATETIME, mongo_last_updated DATETIME, created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP, updated_datetime DATETIME ON UPDATE CURRENT_TIMESTAMP)")

    try:
        stopBangFormIds = [doc["_id"] for doc in formBuilder_collection.find({"module": "stopBangForm"}, {"_id": 1})]

        print(f"Going to fetch latest mongo_last_updated in stopBangForm Table")

        config.mysql_cursor.execute("SELECT mongo_last_updated FROM stopBangForm ORDER BY mongo_last_updated DESC LIMIT 1")
        latest_record = config.mysql_cursor.fetchone()

        if latest_record:
            latest_mongo_lastUpdated = latest_record[0]
            print(f"Found latest mongo_last_updated in stopBangForm as {latest_mongo_lastUpdated}")
            print(f"Going to fetch data from formResponse collection for stopBangForm after  {latest_mongo_lastUpdated}")

            # Your code here
            # Modify mongo_query to include a condition on lastUpdated
            mongo_query = {"form.reference": {"$in": stopBangFormIds},"lastUpdated": { "$gt": latest_mongo_lastUpdated }}
            print("Updated MongoDB query with lastUpdated condition:", mongo_query)
        else:
            print(f"No records found, Please run legacy script first.---->")

        mongo_results = formResponse_collection.find(mongo_query)
    except Exception as e:
        print("A error occurred while performing mongo operations :", str(e))
        
    

    for document in mongo_results:
    # Extract data from MongoDB document
        print("Document",document)
        mongo_id = str(document["_id"])
        print("mongo id for this Document",mongo_id)
        service_request_id = str(document.get("basedOn", [{}])[0].get("reference", "")) if document.get("basedOn", [{}])[0] and document.get("basedOn", [{}])[0].get("reference", "")  else ""
        created_by_user = str(document.get("createdByUser", "")) if document.get("createdByUser", "") else ""
        extension_value = document.get("extension", [{}])[0].get("value", [{}])[0].get("formData", {}) if document.get("extension", [{}])[0] and document.get("extension", [{}])[0].get("value", [{}])[0] and document.get("extension", [{}])[0].get("value", [{}])[0].get("formData", {}) else None
        patient_name = str(extension_value.get("patient_name", "")) if extension_value and extension_value.get("patient_name", "") else ""
        patientDob = str(extension_value.get("patientDob", "")) if extension_value and extension_value.get("patientDob", "") else ""
        age = str(extension_value.get("age", "")) if extension_value and extension_value.get("age", "") else ""
        gender = str(extension_value.get("gender", "")) if extension_value and extension_value.get("gender", "") else ""
        height = str(extension_value.get("hight", "")) if extension_value and extension_value.get("hight", "") else ""
        weight = str(extension_value.get("weight", "")) if extension_value and extension_value.get("weight", "") else ""
        bmi = str(extension_value.get("bmi", "")) if extension_value and extension_value.get("bmi", "") else ""
        surgeon_name = str(extension_value.get("surgenName", "")) if extension_value and extension_value.get("surgenName", "") else ""
        dateOfSurgery = str(extension_value.get("dateOfSurgery", "")) if extension_value and extension_value.get("dateOfSurgery", "") else ""
        stop_snore_no = str(extension_value.get("stop_snore", {}).get("no", "")) if extension_value and extension_value.get("stop_snore", {}) and extension_value.get("stop_snore", {}).get("no", "") else ""
        stop_snore_yes = str(extension_value.get("stop_snore", {}).get("yes", "")) if extension_value and extension_value.get("stop_snore", {}) and extension_value.get("stop_snore", {}).get("yes", "") else ""
        stop_daytime_no = str(extension_value.get("stop_daytime", {}).get("no", "")) if extension_value and extension_value.get("stop_daytime", {}) and extension_value.get("stop_daytime", {}).get("no", "") else ""
        stop_daytime_yes = str(extension_value.get("stop_daytime", {}).get("yes", "")) if extension_value and extension_value.get("stop_daytime", {}) and extension_value.get("stop_daytime", {}).get("yes", "") else ""
        stop_sleep_no = str(extension_value.get("stop_sleep", {}).get("no", "")) if extension_value and extension_value.get("stop_sleep", {}) and extension_value.get("stop_sleep", {}).get("no", "") else ""
        stop_sleep_yes = str(extension_value.get("stop_sleep", {}).get("yes", "")) if extension_value and extension_value.get("stop_sleep", {}) and extension_value.get("stop_sleep", {}).get("yes", "") else ""
        stop_bp_no = str(extension_value.get("stop_bp", {}).get("no", "")) if extension_value and extension_value.get("stop_bp", {}) and extension_value.get("stop_bp", {}).get("no", "") else ""
        stop_bp_yes = str(extension_value.get("stop_bp", {}).get("yes", "")) if extension_value and extension_value.get("stop_bp", {}) and extension_value.get("stop_bp", {}).get("yes", "") else ""
        bang_bmi_no = str(extension_value.get("bang_bmi", {}).get("no", "")) if extension_value and extension_value.get("bang_bmi", {}) and extension_value.get("bang_bmi", {}).get("no", "") else ""
        bang_bmi_yes = str(extension_value.get("bang_bmi", {}).get("yes", "")) if extension_value and extension_value.get("bang_bmi", {}) and extension_value.get("bang_bmi", {}).get("yes", "") else ""
        bang_age_no = str(extension_value.get("bang_age", {}).get("no", "")) if extension_value and extension_value.get("bang_age", {}) and extension_value.get("bang_age", {}).get("no", "") else ""
        bang_age_yes = str(extension_value.get("bang_age", {}).get("yes", "")) if extension_value and extension_value.get("bang_age", {}) and extension_value.get("bang_age", {}).get("yes", "") else ""
        bang_ncm_no = str(extension_value.get("bang_ncm", {}).get("no", "")) if extension_value and extension_value.get("bang_ncm", {}) and extension_value.get("bang_ncm", {}).get("no", "") else ""
        bang_ncm_yes = str(extension_value.get("bang_ncm", {}).get("yes", "")) if extension_value and extension_value.get("bang_ncm", {}) and extension_value.get("bang_ncm", {}).get("yes", "") else ""
        bang_gender_no = str(extension_value.get("bang_gender", {}).get("no", "")) if extension_value and extension_value.get("bang_gender", {}) and extension_value.get("bang_gender", {}).get("no", "") else ""
        bang_gender_yes = str(extension_value.get("bang_gender", {}).get("yes", "")) if extension_value and extension_value.get("bang_gender", {}) and extension_value.get("bang_gender", {}).get("yes", "") else ""
        bang_enter = str(extension_value.get("bang_enter", "")) if extension_value and extension_value.get("bang_enter", "") else ""
        bang_inch_code = str(extension_value.get("bang_inch", {}).get("code", "")) if extension_value and extension_value.get("bang_inch", {}) and extension_value.get("bang_inch", {}).get("code", "") else ""
        bang_inch_value = str(extension_value.get("bang_inch", {}).get("value", "")) if extension_value and extension_value.get("bang_inch", {}) and extension_value.get("bang_inch", {}).get("value", "") else ""
        osScore = str(extension_value.get("osScore", "")) if extension_value and extension_value.get("osScore", "") else ""
        form_date = str(extension_value.get("date", "")) if extension_value and extension_value.get("date", "") else ""
        form_response_id = str(document.get("identifier", [{}])[0].get("value", {}).get("value", "")) if document.get("identifier", [{}])[0] and document.get("identifier", [{}])[0].get("value", {}) and document.get("identifier", [{}])[0].get("value", {}).get("value", "") else ""
        status = str(document.get("status", {}).get("value", "")) if document.get("status", {}) and document.get("status", {}).get("value", "") else "" # Update the default value to an empty string
        patientId = str(document.get("subject", "")) if document.get("subject", "") else ""
        markToBeDeleted = document.get("markToBeDeleted", "") if document.get("markToBeDeleted", "") else ""
        lastUpdatedBy = str(document.get("lastUpdatedBy", {}).get("reference", "")) if document.get("lastUpdatedBy", {}) and document.get("lastUpdatedBy", {}).get("reference", "") else ""
        mongo_date_created = document.get("dateCreated", "") 
        mongo_last_updated = document.get("lastUpdated", "")

    # ... (your code before the loop)

    # Fetch existing record
        config.mysql_cursor.execute("SELECT id FROM stopBangForm WHERE mongo_id = %s", (mongo_id,))
        existing_record = config.mysql_cursor.fetchone()

        if existing_record:
        # If the record exists, perform an UPDATE operation
            print(f"Found an existing record which has been updated after {latest_mongo_lastUpdated} ......So updating")
            try:
                config.mysql_cursor.execute("UPDATE stopBangForm SET service_request_id = %s, created_by_user = %s, patient_name = %s, patient_DOB = %s,age = %s, gender = %s, height = %s, weight = %s, bmi = %s, surgeon_name = %s,  date_of_surgery = %s, stop_snore_yes = %s, stop_snore_no = %s, stop_daytime_yes = %s, stop_daytime_no = %s, stop_sleep_yes = %s, stop_sleep_no = %s, stop_bp_yes = %s,stop_bp_no = %s, bang_bmi_yes = %s, bang_bmi_no = %s, bang_age_yes = %s, bang_age_no = %s, bang_ncm_yes = %s, bang_ncm_no = %s, bang_gender_yes = %s, bang_gender_no = %s, bang_enter = %s, bang_inch_code = %s, bang_inch_value = %s, os_score = %s, form_date = %s, form_response_id = %s, status = %s, patientId = %s, lastUpdatedBy = %s, markToBeDeleted = %s, mongo_date_created = %s, mongo_last_updated = %s WHERE mongo_id = %s ",(service_request_id, created_by_user, patient_name, patientDob, age, gender, height, weight, bmi, surgeon_name, dateOfSurgery, stop_snore_yes, stop_snore_no, stop_daytime_yes, stop_daytime_no,stop_sleep_yes, stop_sleep_no, stop_bp_yes, stop_bp_no, bang_bmi_yes, bang_bmi_no, bang_age_yes, bang_age_no, bang_ncm_yes, bang_ncm_no, bang_gender_yes, bang_gender_no, bang_enter, bang_inch_code, bang_inch_value, osScore, form_date, form_response_id, status, patientId,lastUpdatedBy,markToBeDeleted, mongo_date_created, mongo_last_updated,mongo_id))
                print("Executed UPDATE for document with mongo_id:", mongo_id)
            except Exception as e:
                print(f"Error found {str(e)} while updating for document with mongo Id {mongo_id}")
        else:
        # If the record doesn't exist, perform an INSERT operation
            print("Found new record so inserting with Mongo-Id ",mongo_id)
            try:
                config.mysql_cursor.execute(
                """
                INSERT INTO stopBangForm (mongo_id, service_request_id, created_by_user, patient_name, patient_DOB,
                    age, gender, height, weight, bmi, surgeon_name, date_of_surgery, stop_snore_yes, stop_snore_no,
                    stop_daytime_yes, stop_daytime_no, stop_sleep_yes, stop_sleep_no, stop_bp_yes, stop_bp_no,
                    bang_bmi_yes, bang_bmi_no, bang_age_yes, bang_age_no, bang_ncm_yes, bang_ncm_no, bang_gender_yes,
                    bang_gender_no, bang_enter, bang_inch_code, bang_inch_value, os_score, form_date, form_response_id,
                    status, patientId, lastUpdatedBy,markToBeDeleted,mongo_date_created, mongo_last_updated
                ) VALUES (%s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    mongo_id,service_request_id, created_by_user, patient_name, patientDob, age, gender, height, weight, bmi,
                    surgeon_name, dateOfSurgery, stop_snore_yes, stop_snore_no, stop_daytime_yes, stop_daytime_no,
                    stop_sleep_yes, stop_sleep_no, stop_bp_yes, stop_bp_no, bang_bmi_yes, bang_bmi_no, bang_age_yes,
                    bang_age_no, bang_ncm_yes, bang_ncm_no, bang_gender_yes, bang_gender_no, bang_enter, bang_inch_code,
                    bang_inch_value, osScore, form_date, form_response_id, status, patientId, lastUpdatedBy,markToBeDeleted,mongo_date_created,
                    mongo_last_updated,
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