from datetime import datetime
import config

try:
    now = datetime.now()
    print("start_time ...", now)

    mongoConnect = config.get_mongo_client()
    formResponse_collection = mongoConnect.get_database(config.mongo_db).get_collection("formResponse")
    formBuilder_collection = mongoConnect.get_database(config.mongo_db).get_collection("formBuilder")


    config.mysql_cursor.execute("CREATE TABLE IF NOT EXISTS makePreOpCall (id INT AUTO_INCREMENT PRIMARY KEY, mongo_id TEXT, service_request_id TEXT, created_by_user TEXT, patient_name TEXT, patient_DOB TEXT,date_of_surgery TEXT,surgery_name TEXT, surgeon_name TEXT,primary_phone TEXT,alternate_phone TEXT,patient_anesthesia_type TEXT,spokeWith TEXT,ifOtherSpoke TEXT,leftMessage TEXT,spelling_yes TEXT,spelling_no TEXT,verificationSpelling TEXT,verificationSpellingDescribe TEXT,patientDOBConfirmedYes TEXT,patientDOBConfirmedNo TEXT,verificationDOB TEXT,verificationDOBDescribe TEXT,patientInstructedToArriveAtTheAsc TEXT,floorNumber TEXT,ifOtherFloor TEXT,procedureTime TEXT,NPO_instructions_anesthesia TEXT,NPO_instructions_anesthesia_comments TEXT,escort_patient_home TEXT,planned_escort TEXT,homeMedication_yes TEXT,homeMedication_na TEXT,follow_prescribing_physician_surgeon_instructions TEXT,follow_prescribing_physician_surgeon_instructions_describe TEXT,NPO_instructions_anesthesia_2 TEXT,NPO_instructions_anesthesia_2_describe TEXT,photoID_medicaldevice_specialequipment_clothes TEXT,photoID_medicaldevice_specialequipment_clothes_comments TEXT,leave_valuables TEXT,remove_jewellery TEXT,makeup TEXT,makeup_comments TEXT,rn_name TEXT ,form_date TEXT,form_time TEXT,form_response_id TEXT, status TEXT, patientId TEXT, lastUpdatedBy TEXT, markToBeDeleted BOOLEAN, mongo_date_created DATETIME, mongo_last_updated DATETIME, created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP, updated_datetime DATETIME ON UPDATE CURRENT_TIMESTAMP)")

    try:
        makePreOpCallIds = [doc["_id"] for doc in formBuilder_collection.find({"module": "makePreOpCall"}, {"_id": 1})]
        mongo_query = {"form.reference": {"$in": makePreOpCallIds}}
        mongo_results = formResponse_collection.find(mongo_query)
    except Exception as e:
        print("A error while performing mongo operations :", str(e))

    processed_count = 0  # Counter to track the number of processed records
    batch_size = 1000  # A  djust the batch size as per your needs
    batch = []

    # ...
    for document in mongo_results:
        print("Document",document)
        mongo_id = str(document["_id"])
        print("Document with mongo id",mongo_id)
        service_request_id = str(document.get("basedOn", [{}])[0].get("reference", "")) if document.get("basedOn", [{}])[0] and document.get("basedOn", [{}])[0].get("reference", "")  else ""
        created_by_user = str(document.get("createdByUser", "")) if document.get("createdByUser", "") else ""
        extension_value = document.get("extension", [{}])[0].get("value", [{}])[0].get("formData", {}) if document.get("extension", [{}])[0] and document.get("extension", [{}])[0].get("value", [{}])[0] and document.get("extension", [{}])[0].get("value", [{}])[0].get("formData", {}) else None
        patient_name = str(extension_value.get("patient_name", "")) if extension_value and extension_value.get("patient_name", "") else ""
        # ... Remaining fields ...
        patient_DOB = str(extension_value.get("patient_DOB", "")) if extension_value and extension_value.get("patient_DOB", "") else ""
        patient_dateOfSurgery = str(extension_value.get("patient_dateOfSurgery", "")) if extension_value and extension_value.get("patient_dateOfSurgery", "") else ""
        patient_surgeryName = str(extension_value.get("patient_surgeryName", "")) if extension_value and extension_value.get("patient_surgeryName", "") else ""
        patient_surgeonName = str(extension_value.get("patient_surgeonName", "")) if extension_value and extension_value.get("patient_surgeonName", "") else ""
        patient_primaryPhoneNo = str(extension_value.get("patient_primaryPhoneNo", "")) if extension_value and extension_value.get("patient_primaryPhoneNo", "") else ""
        patient_altPhoneNo = str(extension_value.get("patient_altPhoneNo", "")) if extension_value and extension_value.get("patient_altPhoneNo", "") else ""

        patient_typeAnesthesia = str(extension_value.get("patient_typeAnesthesia", "")) if extension_value and extension_value.get("patient_typeAnesthesia", "") else ""

        spokeWith = str(extension_value.get("spokeWith", {}).get("value", "")) if extension_value and extension_value.get("spokeWith", {}) and extension_value.get("spokeWith", {}).get("value", "") else ""
        ifOtherSpoke = str(extension_value.get("ifOtherSpoke", "")) if extension_value and extension_value.get("ifOtherSpoke", "") else ""

        leftMessage = str(extension_value.get("leftMessage", "")) if extension_value and extension_value.get("leftMessage", "") else ""

        spelling_yes = str(extension_value.get("spelling", {}).get("yes", "")) if extension_value and extension_value.get("spelling", {}) and extension_value.get("spelling", {}).get("yes", "") else ""
        spelling_no = str(extension_value.get("spelling", {}).get("no", "")) if extension_value and extension_value.get("spelling", {}) and extension_value.get("spelling", {}).get("no", "") else ""

        verificationSpelling = str(extension_value.get("verificationSpelling", "")) if extension_value and extension_value.get("verificationSpelling", "") else ""

        verificationSpellingDescribe = str(extension_value.get("describeSpelling", "")) if extension_value and extension_value.get("describeSpelling", "") else ""


        patientDOBConfirmedYes = str(extension_value.get("patientDOB", {}).get("yes", "")) if extension_value and extension_value.get("patientDOB", {}) and extension_value.get("patientDOB", {}).get("yes", "") else ""
        patientDOBConfirmedNo = str(extension_value.get("patientDOB", {}).get("no", "")) if extension_value and extension_value.get("patientDOB", {}) and extension_value.get("patientDOB", {}).get("no", "") else ""

        verificationDOB = str(extension_value.get("verificationDOB", "")) if extension_value and extension_value.get("verificationDOB", "") else ""
        verificationDOBDescribe = str(extension_value.get("describeDOB", "")) if extension_value and extension_value.get("describeDOB", "") else ""

        patientInstructedToArriveAtTheAsc = str(extension_value.get("patientInstruction", {}).get("value", "")) if extension_value and extension_value.get("patientInstruction", {}) and extension_value.get("patientInstruction", {}).get("value", "") else ""

        floor = str(extension_value.get("floor", {}).get("value", "")) if extension_value and extension_value.get("floor", {}) and extension_value.get("floor", {}).get("value", "") else ""
        ifOtherFloor = str(extension_value.get("ifOtherFloor", "")) if extension_value and extension_value.get("ifOtherFloor", "") else ""
        procedureTime = str(extension_value.get("procedure", "")) if extension_value and extension_value.get("procedure", "") else ""

        npo_instructions_anesthesia = str(extension_value.get("guidelines", "")) if extension_value and extension_value.get("guidelines", "") else ""
        npo_instructions_anesthesia_comments = str(extension_value.get("commentGuideline", "")) if extension_value and extension_value.get("commentGuideline", "") else ""

        escort_patient_home = str(extension_value.get("instructed", "")) if extension_value and extension_value.get("instructed", "") else ""
        planned_escort = str(extension_value.get("planned", "")) if extension_value and extension_value.get("planned", "") else ""
        homeMedication_yes = str(extension_value.get("homeMedication", {}).get("yes", "")) if extension_value and extension_value.get("homeMedication", {}) and extension_value.get("homeMedication", {}).get("yes", "") else ""
        homeMedication_na = str(extension_value.get("homeMedication", {}).get("n/a", "")) if extension_value and extension_value.get("homeMedication", {}) and extension_value.get("homeMedication", {}).get("n/a", "") else ""
        follow_prescribing_physician_surgeon_instructions = str(extension_value.get("advised", "")) if extension_value and extension_value.get("advised", "") else ""
        follow_prescribing_physician_surgeon_instructions_describe = str(extension_value.get("describeAdvise", "")) if extension_value and extension_value.get("describeAdvise", "") else ""

        npo_instructions_anesthesia_2 = str(extension_value.get("instructions2", "")) if extension_value and extension_value.get("instructions2", "") else ""
        npo_instructions_anesthesia_2_describe = str(extension_value.get("describeInstruction", "")) if extension_value and extension_value.get("describeInstruction", "") else ""

        photoID_medicaldevice_specialequipment_clothes = str(extension_value.get("instructedID", "")) if extension_value and extension_value.get("instructedID", "") else ""

        photoID_medicaldevice_specialequipment_clothes_comments = str(extension_value.get("commentSpecify", "")) if extension_value and extension_value.get("commentSpecify", "") else ""

        leave_valuables = str(extension_value.get("instruction3", "")) if extension_value and extension_value.get("instruction3", "") else ""

        remove_jewellery = str(extension_value.get("remove", "")) if extension_value and extension_value.get("remove", "") else ""
        makeup = str(extension_value.get("makeup", "")) if extension_value and extension_value.get("makeup", "") else ""
        makeup_comments = str(extension_value.get("commentInstruction", "")) if extension_value and extension_value.get("commentInstruction", "") else ""

        rn_name = str(extension_value.get("RNname", "")) if extension_value and extension_value.get("RNname", "") else ""
        form_date = str(extension_value.get("date", "")) if extension_value and extension_value.get("date", "") else ""
        form_time = str(extension_value.get("time", "")) if extension_value and extension_value.get("time", "") else ""

        form_response_id = str(document.get("identifier", [{}])[0].get("value", {}).get("value", "")) if document.get("identifier", [{}])[0] and document.get("identifier", [{}])[0].get("value", {}) and document.get("identifier", [{}])[0].get("value", {}).get("value", "") else ""
        status = str(document.get("status", {}).get("value", "")) if document.get("status", {}) and document.get("status", {}).get("value", "") else ""  # Update the default value to an empty string
        patientId = str(document.get("subject", "")) if document.get("subject", "") else ""
        markToBeDeleted = document.get("markToBeDeleted", "") if document.get("markToBeDeleted", "") else ""
        lastUpdatedBy = str(document.get("lastUpdatedBy", {}).get("reference", "")) if document.get("lastUpdatedBy", {}) and document.get("lastUpdatedBy", {}).get("reference", "") else ""
        mongo_date_created = document.get("dateCreated", "")
        mongo_last_updated = document.get("lastUpdated", "")

        mysql_values = (
                mongo_id,service_request_id, created_by_user, patient_name, patient_DOB, patient_dateOfSurgery, patient_surgeryName, patient_surgeonName, patient_primaryPhoneNo, patient_altPhoneNo, patient_typeAnesthesia, spokeWith, ifOtherSpoke, leftMessage, spelling_yes, spelling_no, verificationSpelling, verificationSpellingDescribe, patientDOBConfirmedYes, patientDOBConfirmedNo, verificationDOB, verificationDOBDescribe, patientInstructedToArriveAtTheAsc, floor, ifOtherFloor, procedureTime, npo_instructions_anesthesia, npo_instructions_anesthesia_comments, escort_patient_home, planned_escort, homeMedication_yes, homeMedication_na, follow_prescribing_physician_surgeon_instructions, follow_prescribing_physician_surgeon_instructions_describe, npo_instructions_anesthesia_2, npo_instructions_anesthesia_2_describe, photoID_medicaldevice_specialequipment_clothes, photoID_medicaldevice_specialequipment_clothes_comments, leave_valuables, remove_jewellery, makeup, makeup_comments, rn_name, form_date, form_time, form_response_id, status, patientId,lastUpdatedBy, markToBeDeleted, mongo_date_created, mongo_last_updated
          )

        batch.append(mysql_values)

        # Insert batch into MySQL
        try:
            if len(batch) >= batch_size:
                config.mysql_cursor.executemany(
                    "INSERT INTO makePreOpCall (mongo_id, service_request_id, created_by_user, patient_name, patient_DOB, date_of_surgery, surgery_name, surgeon_name, primary_phone, alternate_phone, patient_anesthesia_type, spokeWith, ifOtherSpoke, leftMessage, spelling_yes, spelling_no, verificationSpelling, verificationSpellingDescribe, patientDOBConfirmedYes, patientDOBConfirmedNo, verificationDOB, verificationDOBDescribe, patientInstructedToArriveAtTheAsc, floorNumber, ifOtherFloor, procedureTime, NPO_instructions_anesthesia, NPO_instructions_anesthesia_comments, escort_patient_home, planned_escort, homeMedication_yes, homeMedication_na, follow_prescribing_physician_surgeon_instructions, follow_prescribing_physician_surgeon_instructions_describe, NPO_instructions_anesthesia_2, NPO_instructions_anesthesia_2_describe, photoID_medicaldevice_specialequipment_clothes, photoID_medicaldevice_specialequipment_clothes_comments, leave_valuables, remove_jewellery, makeup, makeup_comments, rn_name, form_date, form_time, form_response_id, status, patientId, lastUpdatedBy, markToBeDeleted, mongo_date_created, mongo_last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    batch
                )
                print("Documents inserted",batch)
                config.mysql_connection.commit()
                processed_count += len(batch)
                batch = []

        except Exception as e:
            print("An error occurred while inserting documents:", str(e))
            # Insert the remaining records in the last batch
    if batch:
        try:
            config.mysql_cursor.executemany(
                "INSERT INTO makePreOpCall (mongo_id, service_request_id, created_by_user, patient_name, patient_DOB, date_of_surgery, surgery_name, surgeon_name, primary_phone, alternate_phone, patient_anesthesia_type, spokeWith, ifOtherSpoke, leftMessage, spelling_yes, spelling_no, verificationSpelling, verificationSpellingDescribe, patientDOBConfirmedYes, patientDOBConfirmedNo, verificationDOB, verificationDOBDescribe, patientInstructedToArriveAtTheAsc, floorNumber, ifOtherFloor, procedureTime, NPO_instructions_anesthesia, NPO_instructions_anesthesia_comments, escort_patient_home, planned_escort, homeMedication_yes, homeMedication_na, follow_prescribing_physician_surgeon_instructions, follow_prescribing_physician_surgeon_instructions_describe, NPO_instructions_anesthesia_2, NPO_instructions_anesthesia_2_describe, photoID_medicaldevice_specialequipment_clothes, photoID_medicaldevice_specialequipment_clothes_comments, leave_valuables, remove_jewellery, makeup, makeup_comments, rn_name, form_date, form_time, form_response_id, status, patientId, lastUpdatedBy, markToBeDeleted, mongo_date_created, mongo_last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                batch
            )
            print("Documents inserted - ", batch)
            config.mysql_connection.commit()
            processed_count += len(batch)

        except Exception as e:
            print(f"An error {str(e)} occurred while inserting remaining documents which are less than {batch_size}")


    now = datetime.now()
    print("end_time ...", now)
    print("processed_count ...", processed_count)

except Exception as e:
    print("A final error occurred:", str(e))

finally:
    config.mysql_cursor.close()
    config.mysql_connection.close()
    mongoConnect.close()

