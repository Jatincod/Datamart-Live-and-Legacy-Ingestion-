from datetime import datetime
import config

try:
    now = datetime.now()
    print("start_time ...", now)

    mongoConnect = config.get_mongo_client()
    formResponse_collection = mongoConnect.get_database(config.mongo_db).get_collection("formResponse")
    formBuilder_collection = mongoConnect.get_database(config.mongo_db).get_collection("formBuilder")


    config.mysql_cursor.execute("CREATE TABLE IF NOT EXISTS makePostOpCallLhh (id INT AUTO_INCREMENT PRIMARY KEY, mongo_id TEXT, service_request_id TEXT, created_by_user TEXT, patient_name TEXT, patient_DOB TEXT,surgery_date TEXT, surgery_name TEXT, surgeon TEXT, phoneNo TEXT, alternate_phoneNo TEXT, anesthesia_type TEXT, clinical_pathways TEXT, rn_name_1 TEXT, rn_date_1 TEXT, rn_time_1 TEXT, follow_up_status_1 TEXT, spoke_with1 TEXT, other_relation1 TEXT, rn_name_2 TEXT, rn_date_2 TEXT, rn_time_2 TEXT, follow_up_status_2 TEXT, spoke_with2 TEXT, other_relation2 TEXT, rn_name_3 TEXT, rn_date_3 TEXT, rn_time_3 TEXT, follow_up_status_3 TEXT, spoke_with3 TEXT, other_relation3 TEXT, ask_about_recent_visit TEXT, good_time_callback_date TEXT, good_time_callback_time TEXT, do_not_call TEXT, do_not_call_reason TEXT, tolerateFluid TEXT, tolerateFluid_Specify TEXT, ambulate TEXT, ambulate_Specify TEXT, soreThroat TEXT, soreThroat_Specify TEXT, shortnessOfBreath TEXT, shortnessOfBreath_duration TEXT, difficultyBreathing TEXT, difficultyBreathing_SpecifyDuration1 TEXT, nausea TEXT, nausea_SpecifyDuration3 TEXT, vomiting TEXT, vomiting_SpecifyNumberOfEpisodes TEXT, bleeding TEXT, bleeding_Specify TEXT, difficultyUrinating TEXT, difficultyUrinating_Specify TEXT, chills TEXT, chills_SpecifyDuration TEXT, chills_SpecifyTemperature TEXT, pain_IV_site TEXT, painControll TEXT, painControll_Specify TEXT, prescribedMedication TEXT, medicationForPain TEXT, medicationForPain_Specify TEXT, questionRegardingPain TEXT, questionRegardingPain_Specify TEXT, currentyHavingPain TEXT, currentyHavingPain_specifyPainDetails TEXT, currentyHavingPain_specifySeverity TEXT, postoperativeInstruction TEXT, dischargeInstruction TEXT, satisfiedWithCare TEXT, satisfiedWithCare_Specify TEXT, Comments TEXT, occurredSinceDischarge TEXT, occurredSinceDischarge_Specify TEXT, end_comments TEXT,form_response_id TEXT, status TEXT, patientId TEXT, lastUpdatedBy TEXT, markToBeDeleted BOOLEAN, mongo_date_created DATETIME, mongo_last_updated DATETIME, created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP, updated_datetime DATETIME ON UPDATE CURRENT_TIMESTAMP)")

    try:
        makePreOpCallLhhIds = [doc["_id"] for doc in formBuilder_collection.find({"module": "makePostOpCallLhh"}, {"_id": 1})]
        mongo_query = {"form.reference": {"$in": makePreOpCallLhhIds}}
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
        # ... Remaining fields ..
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
        spoke_with1 = str(extension_value.get("spoke_with1", {}).get("value", "")) if extension_value.get("spoke_with1", {}) and extension_value.get("spoke_with1", {}).get("value", "") else ""  # Update the default value to an empty string
        other_relation1 = str(extension_value.get("other_relation1", "")) if extension_value and extension_value.get("other_relation1", "") else ""

        rn_name_2 = str(extension_value.get("rn_name_2", "")) if extension_value and extension_value.get("rn_name_2", "") else ""
        rn_date_2 = str(extension_value.get("rn_date_2", "")) if extension_value and extension_value.get("rn_date_2", "") else ""
        rn_time_2 = str(extension_value.get("rn_time_2", "")) if extension_value and extension_value.get("rn_time_2", "") else ""
        follow_up_status_2 = str(extension_value.get("follow_up_status_2", "")) if extension_value and extension_value.get("follow_up_status_2", "") else ""
        spoke_with2 = str(extension_value.get("spoke_with2", {}).get("value", "")) if extension_value.get("spoke_with2", {}) and extension_value.get("spoke_with2", {}).get("value", "") else ""  # Update the default value to an empty string
        other_relation2 = str(extension_value.get("other_relation2", "")) if extension_value and extension_value.get("other_relation2", "") else ""

        rn_name_3 = str(extension_value.get("rn_name_3", "")) if extension_value and extension_value.get("rn_name_3", "") else ""
        rn_date_3 = str(extension_value.get("rn_date_3", "")) if extension_value and extension_value.get("rn_date_3", "") else ""
        rn_time_3 = str(extension_value.get("rn_time_3", "")) if extension_value and extension_value.get("rn_time_3", "") else ""
        follow_up_status_3 = str(extension_value.get("follow_up_status_3", "")) if extension_value and extension_value.get("follow_up_status_3", "") else ""
        spoke_with3 = str(extension_value.get("spoke_with3", {}).get("value", "")) if extension_value.get("spoke_with3", {}) and extension_value.get("spoke_with3", {}).get("value", "") else ""  # Update the default value to an empty string
        other_relation3 = str(extension_value.get("other_relation3", "")) if extension_value and extension_value.get("other_relation3", "") else ""

        ask_about_recent_visit = str(extension_value.get("desc", "")) if extension_value and extension_value.get("desc", "") else ""
        good_time_callback_date = str(extension_value.get("cllBack_date", "")) if extension_value and extension_value.get("cllBack_date", "") else ""
        good_time_callback_time = str(extension_value.get("cllBack_time", "")) if extension_value and extension_value.get("cllBack_time", "") else ""
        do_not_call = str(extension_value.get("doNotCall", "")) if extension_value and extension_value.get("doNotCall", "") else ""
        do_not_call_reason = str(extension_value.get("reason", "")) if extension_value and extension_value.get("reason", "") else ""

        tolerateFluid = str(extension_value.get("tolerateFluid", "")) if extension_value and extension_value.get("tolerateFluid", "") else ""
        tolerateFluid_Specify = str(extension_value.get("tolerateFluid_Specify", "")) if extension_value and extension_value.get("tolerateFluid_Specify", "") else ""

        ambulate = str(extension_value.get("ambulate", "")) if extension_value and extension_value.get("ambulate", "") else ""
        ambulate_Specify = str(extension_value.get("ambulate_Specify", "")) if extension_value and extension_value.get("ambulate_Specify", "") else ""

        soreThroat = str(extension_value.get("soreThroat", "")) if extension_value and extension_value.get("soreThroat", "") else ""
        soreThroat_Specify = str(extension_value.get("soreThroat_Specify", "")) if extension_value and extension_value.get("soreThroat_Specify", "") else ""

        shortnessOfBreath = str(extension_value.get("shortnessOfBreath", "")) if extension_value and extension_value.get("shortnessOfBreath", "") else ""
        shortnessOfBreath_duration = str(extension_value.get("shortnessOfBreath_duration", "")) if extension_value and extension_value.get("shortnessOfBreath_duration", "") else ""

        difficultyBreathing = str(extension_value.get("difficultyBreathing", "")) if extension_value and extension_value.get("difficultyBreathing", "") else ""
        difficultyBreathing_SpecifyDuration1 = str(extension_value.get("difficultyBreathing_SpecifyDuration1", "")) if extension_value and extension_value.get("difficultyBreathing_SpecifyDuration1", "") else ""

        nausea = str(extension_value.get("nausea", "")) if extension_value and extension_value.get("nausea", "") else ""
        nausea_SpecifyDuration3 = str(extension_value.get("nausea_SpecifyDuration3", "")) if extension_value and extension_value.get("nausea_SpecifyDuration3", "") else ""

        vomiting = str(extension_value.get("vomiting", "")) if extension_value and extension_value.get("vomiting", "") else ""
        vomiting_SpecifyNumberOfEpisodes = str(extension_value.get("vomiting_SpecifyNumberOfEpisodes", "")) if extension_value and extension_value.get("vomiting_SpecifyNumberOfEpisodes", "") else ""

        bleeding = str(extension_value.get("bleeding", "")) if extension_value and extension_value.get("bleeding", "") else ""
        bleeding_Specify = str(extension_value.get("bleeding_Specify", "")) if extension_value and extension_value.get("bleeding_Specify", "") else ""

        difficultyUrinating = str(extension_value.get("difficultyUrinating", "")) if extension_value and extension_value.get("difficultyUrinating", "") else ""
        difficultyUrinating_Specify = str(extension_value.get("difficultyUrinating_Specify", "")) if extension_value and extension_value.get("difficultyUrinating_Specify", "") else ""

        chills = str(extension_value.get("chills", "")) if extension_value and extension_value.get("chills", "") else ""
        chills_SpecifyDuration = str(extension_value.get("chills_SpecifyDuration", "")) if extension_value and extension_value.get("chills_SpecifyDuration", "") else ""
        chills_SpecifyTemperature = str(extension_value.get("chills_SpecifyTemperature", "")) if extension_value and extension_value.get("chills_SpecifyTemperature", "") else ""

        pain_IV_site = str(extension_value.get("pain", "")) if extension_value and extension_value.get("pain", "") else ""

        painControll = str(extension_value.get("painControll", "")) if extension_value and extension_value.get("painControll", "") else ""
        painControll_Specify = str(extension_value.get("painControll_Specify", "")) if extension_value and extension_value.get("painControll_Specify", "") else ""

        prescribedMedication = str(extension_value.get("prescribedMedication", "")) if extension_value and extension_value.get("prescribedMedication", "") else ""

        medicationForPain = str(extension_value.get("medicationForPain", "")) if extension_value and extension_value.get("medicationForPain", "") else ""
        medicationForPain_Specify = str(extension_value.get("medicationForPain_Specify", "")) if extension_value and extension_value.get("medicationForPain_Specify", "") else ""

        questionRegardingPain = str(extension_value.get("questionRegardingPain", "")) if extension_value and extension_value.get("questionRegardingPain", "") else ""
        questionRegardingPain_Specify = str(extension_value.get("questionRegardingPain_Specify", "")) if extension_value and extension_value.get("questionRegardingPain_Specify", "") else ""

        currentyHavingPain = str(extension_value.get("currentyHavingPain", "")) if extension_value and extension_value.get("currentyHavingPain", "") else ""
        currentyHavingPain_specifyPainDetails = str(extension_value.get("currentyHavingPain_specifyPainDetails", "")) if extension_value and extension_value.get("currentyHavingPain_specifyPainDetails", "") else ""
        currentyHavingPain_specifySeverity = str(extension_value.get("currentyHavingPain_specifySeverity", "")) if extension_value and extension_value.get("currentyHavingPain_specifySeverity", "") else ""

        postoperativeInstruction = str(extension_value.get("postoperativeInstruction", "")) if extension_value and extension_value.get("postoperativeInstruction", "") else ""
        dischargeInstruction = str(extension_value.get("dischargeInstruction", "")) if extension_value and extension_value.get("dischargeInstruction", "") else ""

        satisfiedWithCare = str(extension_value.get("satisfiedWithCare", "")) if extension_value and extension_value.get("satisfiedWithCare", "") else ""
        satisfiedWithCare_Specify = str(extension_value.get("satisfiedWithCare_Specify", "")) if extension_value and extension_value.get("satisfiedWithCare_Specify", "") else ""

        comments = str(extension_value.get("comments", "")) if extension_value and extension_value.get("comments", "") else ""

        occurredSinceDischarge = str(extension_value.get("occurredSinceDischarge", "")) if extension_value and extension_value.get("occurredSinceDischarge", "") else ""
        occurredSinceDischarge_Specify = str(extension_value.get("occurredSinceDischarge_Specify", "")) if extension_value and extension_value.get("occurredSinceDischarge_Specify", "") else ""

        end_comments = str(extension_value.get("attempt_comments", "")) if extension_value and extension_value.get("attempt_comments", "") else ""

        form_response_id = str(document.get("identifier", [{}])[0].get("value", {}).get("value", "")) if document.get("identifier", [{}])[0] and document.get("identifier", [{}])[0].get("value", {}) and document.get("identifier", [{}])[0].get("value", {}).get("value", "") else ""
        status = str(document.get("status", {}).get("value", "")) if document.get("status", {}) and document.get("status", {}).get("value", "") else ""  # Update the default value to an empty string
        patientId = str(document.get("subject", "")) if document.get("subject", "") else ""
        markToBeDeleted = document.get("markToBeDeleted", "") if document.get("markToBeDeleted", "") else ""
        lastUpdatedBy = str(document.get("lastUpdatedBy", {}).get("reference", "")) if document.get("lastUpdatedBy", {}) and document.get("lastUpdatedBy", {}).get("reference", "") else ""
        mongo_date_created = document.get("dateCreated", "")
        mongo_last_updated = document.get("lastUpdated", "")

        mysql_values = (
            mongo_id,service_request_id, created_by_user, patient_name, patient_DOB, surgery_date,surgery_name, surgeon, phoneNo, alternate_phoneNo, anesthesia_type, clinical_pathways, rn_name_1, rn_date_1, rn_time_1, follow_up_status_1, spoke_with1, other_relation1, rn_name_2, rn_date_2, rn_time_2, follow_up_status_2, spoke_with2, other_relation2, rn_name_3, rn_date_3, rn_time_3, follow_up_status_3, spoke_with3, other_relation3, ask_about_recent_visit, good_time_callback_date, good_time_callback_time, do_not_call, do_not_call_reason, tolerateFluid, tolerateFluid_Specify, ambulate, ambulate_Specify, soreThroat, soreThroat_Specify, shortnessOfBreath, shortnessOfBreath_duration, difficultyBreathing, difficultyBreathing_SpecifyDuration1, nausea, nausea_SpecifyDuration3, vomiting, vomiting_SpecifyNumberOfEpisodes, bleeding, bleeding_Specify, difficultyUrinating, difficultyUrinating_Specify, chills, chills_SpecifyDuration, chills_SpecifyTemperature, pain_IV_site, painControll, painControll_Specify, prescribedMedication, medicationForPain, medicationForPain_Specify, questionRegardingPain, questionRegardingPain_Specify, currentyHavingPain, currentyHavingPain_specifyPainDetails, currentyHavingPain_specifySeverity, postoperativeInstruction, dischargeInstruction, satisfiedWithCare, satisfiedWithCare_Specify, comments, occurredSinceDischarge, occurredSinceDischarge_Specify, end_comments, form_response_id, status, patientId, lastUpdatedBy, markToBeDeleted, mongo_date_created, mongo_last_updated
        )

        batch.append(mysql_values)

        # Insert batch into MySQL
        try:
            if len(batch) >= batch_size:
                config.mysql_cursor.executemany(
                    "INSERT INTO makePostOpCallLhh (mongo_id, service_request_id, created_by_user, patient_name, patient_DOB, surgery_date, surgery_name, surgeon, phoneNo, alternate_phoneNo, anesthesia_type, clinical_pathways, rn_name_1, rn_date_1, rn_time_1, follow_up_status_1, spoke_with1, other_relation1, rn_name_2, rn_date_2, rn_time_2, follow_up_status_2, spoke_with2, other_relation2, rn_name_3, rn_date_3, rn_time_3, follow_up_status_3, spoke_with3, other_relation3, ask_about_recent_visit, good_time_callback_date, good_time_callback_time, do_not_call, do_not_call_reason, tolerateFluid, tolerateFluid_Specify, ambulate, ambulate_Specify, soreThroat, soreThroat_Specify, shortnessOfBreath, shortnessOfBreath_duration, difficultyBreathing, difficultyBreathing_SpecifyDuration1, nausea, nausea_SpecifyDuration3, vomiting, vomiting_SpecifyNumberOfEpisodes, bleeding, bleeding_Specify, difficultyUrinating, difficultyUrinating_Specify, chills, chills_SpecifyDuration, chills_SpecifyTemperature, pain_IV_site, painControll, painControll_Specify, prescribedMedication, medicationForPain, medicationForPain_Specify, questionRegardingPain, questionRegardingPain_Specify, currentyHavingPain, currentyHavingPain_specifyPainDetails, currentyHavingPain_specifySeverity, postoperativeInstruction, dischargeInstruction, satisfiedWithCare, satisfiedWithCare_Specify, Comments, occurredSinceDischarge, occurredSinceDischarge_Specify, end_comments, form_response_id, status, patientId, lastUpdatedBy, markToBeDeleted, mongo_date_created, mongo_last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
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
                "INSERT INTO makePostOpCallLhh (mongo_id, service_request_id, created_by_user, patient_name, patient_DOB, surgery_date, surgery_name, surgeon, phoneNo, alternate_phoneNo, anesthesia_type, clinical_pathways, rn_name_1, rn_date_1, rn_time_1, follow_up_status_1, spoke_with1, other_relation1, rn_name_2, rn_date_2, rn_time_2, follow_up_status_2, spoke_with2, other_relation2, rn_name_3, rn_date_3, rn_time_3, follow_up_status_3, spoke_with3, other_relation3, ask_about_recent_visit, good_time_callback_date, good_time_callback_time, do_not_call, do_not_call_reason, tolerateFluid, tolerateFluid_Specify, ambulate, ambulate_Specify, soreThroat, soreThroat_Specify, shortnessOfBreath, shortnessOfBreath_duration, difficultyBreathing, difficultyBreathing_SpecifyDuration1, nausea, nausea_SpecifyDuration3, vomiting, vomiting_SpecifyNumberOfEpisodes, bleeding, bleeding_Specify, difficultyUrinating, difficultyUrinating_Specify, chills, chills_SpecifyDuration, chills_SpecifyTemperature, pain_IV_site, painControll, painControll_Specify, prescribedMedication, medicationForPain, medicationForPain_Specify, questionRegardingPain, questionRegardingPain_Specify, currentyHavingPain, currentyHavingPain_specifyPainDetails, currentyHavingPain_specifySeverity, postoperativeInstruction, dischargeInstruction, satisfiedWithCare, satisfiedWithCare_Specify, Comments, occurredSinceDischarge, occurredSinceDischarge_Specify, end_comments, form_response_id, status, patientId, lastUpdatedBy, markToBeDeleted, mongo_date_created, mongo_last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
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

