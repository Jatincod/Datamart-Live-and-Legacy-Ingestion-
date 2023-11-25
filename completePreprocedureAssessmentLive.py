from datetime import datetime, timedelta
import config

try:
    now = datetime.now()
    print("start_time ...", now)

    mongoConnect = config.get_mongo_client()
    formResponse_collection = mongoConnect.get_database(config.mongo_db).get_collection("formResponse")
    formBuilder_collection = mongoConnect.get_database(config.mongo_db).get_collection("formBuilder")

    config.mysql_cursor.execute("CREATE TABLE IF NOT EXISTS completePreprocedureAssessment (id INT AUTO_INCREMENT PRIMARY KEY, mongo_id TEXT, service_request_id TEXT, created_by_user TEXT,patient_name TEXT, patientDob TEXT, dateOfSurgery TEXT, surgeryName TEXT, surgeon_name TEXT, primaryPhone TEXT, alternatePhone TEXT, date1 TEXT, admitTime TEXT, patientIdentification_yes TEXT, patientIdentification_no TEXT, procedureVerified_yes TEXT, procedureVerified_no TEXT, solidsFormDate TEXT, solidsFormTime TEXT, clearFluidFormDateTime TEXT, clearFluidFormTime TEXT, lastHomeMedicationDate TEXT, lastHomeMedicationTime TEXT, allergyReviewed_yes TEXT, allergyReviewed_no TEXT, allergyBand_na TEXT, allergyBand_yes TEXT, bloodPressure TEXT, pulse TEXT, regular TEXT, irregular TEXT, respiratory TEXT, temperature TEXT,temperature_unit TEXT, oxygen TEXT, painScale TEXT, scaleType_face TEXT, scaleType_flacc TEXT, scaleType_numerical TEXT, scaleType_pain_ad TEXT, historyPhysical_yes TEXT, historyPhysical_no TEXT, glucoseResult_na TEXT, glucoseResult_yes TEXT, mg TEXT, date2 TEXT, mgTime TEXT, pregancyTest_control_good TEXT, pregancyTest_na TEXT, pregancyTest_yes_attach_strip TEXT, skinPrep_na TEXT, skinPrep_yes_attach_strip TEXT, advanceDirective_na TEXT, advanceDirective_yes TEXT, verificationpostOperative TEXT, name TEXT, phone TEXT, dispositiorOfBelongings_family TEXT, dispositiorOfBelongings_locker TEXT, valuablesGivenSecurity_na TEXT, valuablesGivenSecurity_yes TEXT, dentures_family TEXT, dentures_locker TEXT, dentures_na TEXT, hearingAid_family TEXT, hearingAid_locker TEXT, hearingAid_na TEXT, glasses_family TEXT, glasses_locker TEXT, glasses_na TEXT, prosthesisBrace_family TEXT, prosthesisBrace_locker TEXT, prosthesisBrace_na TEXT, contactLenses_family TEXT, contactLenses_locker TEXT, contactLenses_na TEXT, jewelry_family TEXT, jewelry_locker TEXT, jewelry_na TEXT, safetyAlertGiven_na TEXT, safetyAlertGiven_yes TEXT, specify TEXT, ready TEXT, transferVia_ambulatory TEXT, transferVia_recliner TEXT, transferVia_stretcher TEXT, transferVia_transport TEXT,rnName TEXT,date TEXT,time TEXT,additional_comments TEXT,weightObservationUnit TEXT, weightObservationValue TEXT, heightObservationUnit TEXT, heightObservationValue TEXT, bmi TEXT, form_response_id TEXT, status TEXT, patientId TEXT, lastUpdatedBy TEXT, markToBeDeleted BOOLEAN, mongo_date_created DATETIME, mongo_last_updated DATETIME, created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP, updated_datetime DATETIME ON UPDATE CURRENT_TIMESTAMP)")

    try:
        completePreprocedureAssessmentIds = [doc["_id"] for doc in formBuilder_collection.find({"module": "completePreprocedureAssessment"}, {"_id": 1})]

        print(f"Going to fetch latest mongo_last_updated in completePreprocedureAssessment Table")

        config.mysql_cursor.execute("SELECT mongo_last_updated FROM completePreprocedureAssessment ORDER BY mongo_last_updated DESC LIMIT 1")
        latest_record = config.mysql_cursor.fetchone()

        if latest_record:
            latest_mongo_lastUpdated = latest_record[0]
            print(f"Found latest mongo_last_updated in completePreprocedureAssessment as {latest_mongo_lastUpdated}")
            print(f"Going to fetch data from formResponse collection for completePreprocedureAssessment after  {latest_mongo_lastUpdated}")

            # Your code here
            # Modify mongo_query to include a condition on lastUpdated
            mongo_query = {"form.reference": {"$in": completePreprocedureAssessmentIds},"lastUpdated": { "$gt": latest_mongo_lastUpdated }}
            print("Updated MongoDB query with lastUpdated condition:", mongo_query)
        else:
            print(f"No records found, Please run legacy script first.---->")

        mongo_results = formResponse_collection.find(mongo_query)
    except Exception as e:
        print("A error occurred while performing mongo operations :", str(e))



    for document in mongo_results:
    # Extract data from MongoDB document
        mongo_id = str(document["_id"])
        print(f"MongoId {mongo_id}")
        service_request_id = str(document.get("basedOn", [{}])[0].get("reference", "")) if document.get("basedOn", [{}])[0] and document.get("basedOn", [{}])[0].get("reference", "")  else ""
        created_by_user = str(document.get("createdByUser", "")) if document.get("createdByUser", "") else ""
        extension_value = document.get("extension", [{}])[0].get("value", [{}])[0].get("formData", {}) if document.get("extension", [{}])[0] and document.get("extension", [{}])[0].get("value", [{}])[0] and document.get("extension", [{}])[0].get("value", [{}])[0].get("formData", {}) else None
        patient_name = str(extension_value.get("patientName", "")) if extension_value and extension_value.get("patientName", "") else ""
        patientDob = str(extension_value.get("birthDate", "")) if extension_value and extension_value.get("birthDate", "") else ""
        datOfSurgery = str(extension_value.get("datOfSurgery", "")) if extension_value and extension_value.get("datOfSurgery", "") else ""
        surgeryName = str(extension_value.get("surgeryName", "")) if extension_value and extension_value.get("surgeryName", "") else ""
        surgeon_name = str(extension_value.get("surgeonName", "")) if extension_value and extension_value.get("surgeonName", "") else ""
        primaryPhone = str(extension_value.get("primaryPhon", "")) if extension_value and extension_value.get("primaryPhon", "") else ""
        alternatePhone = str(extension_value.get("alternatePhon", "")) if extension_value and extension_value.get("alternatePhon", "") else ""
        date1 = str(extension_value.get("date1", "")) if extension_value and extension_value.get("date1", "") else ""
        admitTime = str(extension_value.get("admitTime", "")) if extension_value and extension_value.get("admitTime", "") else ""

        patientIdentification_yes = str(extension_value.get("patientIdentification", {}).get("yes", "")) if extension_value and extension_value.get("patientIdentification", {}) and extension_value.get("patientIdentification", {}).get("yes", "") else ""
        patientIdentification_no = str(extension_value.get("patientIdentification", {}).get("no", "")) if extension_value and extension_value.get("patientIdentification", {}) and extension_value.get("patientIdentification", {}).get("no", "") else ""
        procedureVerified_yes = str(extension_value.get("procedureVerified", {}).get("yes", "")) if extension_value and extension_value.get("procedureVerified", {}) and extension_value.get("procedureVerified", {}).get("yes", "") else ""
        procedureVerified_no = str(extension_value.get("procedureVerified", {}).get("no", "")) if extension_value and extension_value.get("procedureVerified", {}) and extension_value.get("procedureVerified", {}).get("no", "") else ""

        solidsFormDate = str(extension_value.get("solidsFormDate", "")) if extension_value and extension_value.get("solidsFormDate", "") else ""
        solidsFormTime = str(extension_value.get("solidsFormTime", "")) if extension_value and extension_value.get("solidsFormTime", "") else ""
        clearFluidFormDateTime = str(extension_value.get("clearFluidForm", "")) if extension_value and extension_value.get("clearFluidForm", "") else ""
        clearFluidFormTime = str(extension_value.get("clearFluidFormTime", "")) if extension_value and extension_value.get("clearFluidFormTime", "") else ""
        lastHomeMedicationDate = str(extension_value.get("lastHomeMedicationDate", "")) if extension_value and extension_value.get("lastHomeMedicationDate", "") else ""
        lastHomeMedicationTime = str(extension_value.get("lastHomeMedicationTime", "")) if extension_value and extension_value.get("lastHomeMedicationTime", "") else ""

        allergyReviewed_yes = str(extension_value.get("allergyReviewed", {}).get("yes", "")) if extension_value and extension_value.get("allergyReviewed", {}) and extension_value.get("allergyReviewed", {}).get("yes", "") else ""
        allergyReviewed_no = str(extension_value.get("allergyReviewed", {}).get("no", "")) if extension_value and extension_value.get("allergyReviewed", {}) and extension_value.get("allergyReviewed", {}).get("no", "") else ""
        allergyBand_na = str(extension_value.get("allergyBand", {}).get("n/a", "")) if extension_value and extension_value.get("allergyBand", {}) and extension_value.get("allergyBand", {}).get("n/a", "") else ""
        allergyBand_yes = str(extension_value.get("allergyBand", {}).get("yes", "")) if extension_value and extension_value.get("allergyBand", {}) and extension_value.get("allergyBand", {}).get("yes", "") else ""

        bloodPressure = str(extension_value.get("bloodPressure", "")) if extension_value and extension_value.get("bloodPressure", "") else ""
        pulse = str(extension_value.get("pulse", "")) if extension_value and extension_value.get("pulse", "") else ""
        regular = str(extension_value.get("regular", "")) if extension_value and extension_value.get("regular", "") else ""
        irregular = str(extension_value.get("irregular", "")) if extension_value and extension_value.get("irregular", "") else ""
        respiratory = str(extension_value.get("respiratory", "")) if extension_value and extension_value.get("respiratory", "") else ""
        temperature = str(extension_value.get("temperature", "")) if extension_value and extension_value.get("temperature", "") else ""
        temperature_unit = str(extension_value.get("fahrenheit", {}).get("value", "")) if extension_value and extension_value.get("fahrenheit", {}) and extension_value.get("fahrenheit", {}).get("value", "") else ""
        oxygen = str(extension_value.get("oxygen", "")) if extension_value and extension_value.get("oxygen", "") else ""
        painScale = str(extension_value.get("painScale", "")) if extension_value and extension_value.get("painScale", "") else ""

        scaleType_face = str(extension_value.get("scaleType", {}).get("face", "")) if extension_value and extension_value.get("scaleType", {}) and extension_value.get("scaleType", {}).get("face", "") else ""
        scaleType_flacc = str(extension_value.get("scaleType", {}).get("flacc", "")) if extension_value and extension_value.get("scaleType", {}) and extension_value.get("scaleType", {}).get("flacc", "") else ""
        scaleType_numerical = str(extension_value.get("scaleType", {}).get("numerical", "")) if extension_value and extension_value.get("scaleType", {}) and extension_value.get("scaleType", {}).get("numerical", "") else ""
        scaleType_pain_ad = str(extension_value.get("scaleType", {}).get("pain_ad", "")) if extension_value and extension_value.get("scaleType", {}) and extension_value.get("scaleType", {}).get("pain_ad", "") else ""

        historyPhysical_yes = str(extension_value.get("historyPhysical", {}).get("yes", "")) if extension_value and extension_value.get("historyPhysical", {}) and extension_value.get("historyPhysical", {}).get("yes", "") else ""
        historyPhysical_no = str(extension_value.get("historyPhysical", {}).get("no", "")) if extension_value and extension_value.get("historyPhysical", {}) and extension_value.get("historyPhysical", {}).get("no", "") else ""

        glucoseResult_na = str(extension_value.get("glucoseResult", {}).get("n/a", "")) if extension_value and extension_value.get("glucoseResult", {}) and extension_value.get("glucoseResult", {}).get("n/a", "") else ""
        glucoseResult_yes = str(extension_value.get("glucoseResult", {}).get("yes<sup>1</sup>", "")) if extension_value and extension_value.get("glucoseResult", {}) and extension_value.get("glucoseResult", {}).get("yes<sup>1</sup>", "") else ""

        mg = str(extension_value.get("mg", "")) if extension_value and extension_value.get("mg", "") else ""
        date2 = str(extension_value.get("date2", "")) if extension_value and extension_value.get("date2", "") else ""
        mgTime = str(extension_value.get("mgTime", "")) if extension_value and extension_value.get("mgTime", "") else ""

        pregancyTest_control_good = str(extension_value.get("pregancyTest", {}).get("control_good", "")) if extension_value and extension_value.get("pregancyTest", {}) and extension_value.get("pregancyTest", {}).get("control_good", "") else ""
        pregancyTest_na = str(extension_value.get("pregancyTest", {}).get("n/a", "")) if extension_value and extension_value.get("pregancyTest", {}) and extension_value.get("pregancyTest", {}).get("n/a", "") else ""
        pregancyTest_yes_attach_strip = str(extension_value.get("pregancyTest", {}).get("yes_(attach_strip)", "")) if extension_value and extension_value.get("pregancyTest", {}) and extension_value.get("pregancyTest", {}).get("yes_(attach_strip)", "") else ""

        skinPrep_na = str(extension_value.get("skinPrep", {}).get("n/a", "")) if extension_value and extension_value.get("skinPrep", {}) and extension_value.get("skinPrep", {}).get("n/a", "") else ""
        skinPrep_yes_attach_strip = str(extension_value.get("skinPrep", {}).get("yes_(attach_strip)", "")) if extension_value and extension_value.get("skinPrep", {}) and extension_value.get("skinPrep", {}).get("yes_(attach_strip)", "") else ""

        advanceDirective_na = str(extension_value.get("advanceDirective", {}).get("n/a", "")) if extension_value and extension_value.get("advanceDirective", {}) and extension_value.get("advanceDirective", {}).get("n/a", "") else ""
        advanceDirective_yes = str(extension_value.get("advanceDirective", {}).get("yes", "")) if extension_value and extension_value.get("advanceDirective", {}) and extension_value.get("advanceDirective", {}).get("yes", "") else ""

        verificationpostOperative = str(extension_value.get("verificationpostOperative", "")) if extension_value and extension_value.get("verificationpostOperative", "") else ""
        name = str(extension_value.get("name", "")) if extension_value and extension_value.get("name", "") else ""
        phone = str(extension_value.get("phone", "")) if extension_value and extension_value.get("phone", "") else ""

        dispositiorOfBelongings_family = str(extension_value.get("dispositiorOfBelongings", {}).get("family", "")) if extension_value and extension_value.get("dispositiorOfBelongings", {}) and extension_value.get("dispositiorOfBelongings", {}).get("family", "") else ""
        dispositiorOfBelongings_locker = str(extension_value.get("dispositiorOfBelongings", {}).get("locker", "")) if extension_value and extension_value.get("dispositiorOfBelongings", {}) and extension_value.get("dispositiorOfBelongings", {}).get("locker", "") else ""

        valuablesGivenSecurity_na = str(extension_value.get("valuablesGivenSecurity", {}).get("n/a", "")) if extension_value and extension_value.get("valuablesGivenSecurity", {}) and extension_value.get("valuablesGivenSecurity", {}).get("n/a", "") else ""
        valuablesGivenSecurity_yes = str(extension_value.get("valuablesGivenSecurity", {}).get("yes", "")) if extension_value and extension_value.get("valuablesGivenSecurity", {}) and extension_value.get("valuablesGivenSecurity", {}).get("yes", "") else ""

        dentures_family = str(extension_value.get("dentures", {}).get("family", "")) if extension_value and extension_value.get("dentures", {}) and extension_value.get("dentures", {}).get("family", "") else ""
        dentures_locker = str(extension_value.get("dentures", {}).get("locker", "")) if extension_value and extension_value.get("dentures", {}) and extension_value.get("dentures", {}).get("locker", "") else ""
        dentures_na = str(extension_value.get("dentures", {}).get("n/a", "")) if extension_value and extension_value.get("dentures", {}) and extension_value.get("dentures", {}).get("n/a", "") else ""

        hearingAid_family = str(extension_value.get("hearingAid", {}).get("family", "")) if extension_value and extension_value.get("hearingAid", {}) and extension_value.get("hearingAid", {}).get("family", "") else ""
        hearingAid_locker = str(extension_value.get("hearingAid", {}).get("locker", "")) if extension_value and extension_value.get("hearingAid", {}) and extension_value.get("hearingAid", {}).get("locker", "") else ""
        hearingAid_na = str(extension_value.get("hearingAid", {}).get("n/a", "")) if extension_value and extension_value.get("hearingAid", {}) and extension_value.get("hearingAid", {}).get("n/a", "") else ""


        glasses_family = str(extension_value.get("glasses", {}).get("family", "")) if extension_value and extension_value.get("glasses", {}) and extension_value.get("glasses", {}).get("family", "") else ""
        glasses_locker = str(extension_value.get("glasses", {}).get("locker", "")) if extension_value and extension_value.get("glasses", {}) and extension_value.get("glasses", {}).get("locker", "") else ""
        glasses_na = str(extension_value.get("glasses", {}).get("n/a", "")) if extension_value and extension_value.get("glasses", {}) and extension_value.get("glasses", {}).get("n/a", "") else ""

        prosthesisBrace_family = str(extension_value.get("prosthesisBrace", {}).get("family", "")) if extension_value and extension_value.get("prosthesisBrace", {}) and extension_value.get("prosthesisBrace", {}).get("family", "") else ""
        prosthesisBrace_locker = str(extension_value.get("prosthesisBrace", {}).get("locker", "")) if extension_value and extension_value.get("prosthesisBrace", {}) and extension_value.get("prosthesisBrace", {}).get("locker", "") else ""
        prosthesisBrace_na = str(extension_value.get("prosthesisBrace", {}).get("n/a", "")) if extension_value and extension_value.get("prosthesisBrace", {}) and extension_value.get("prosthesisBrace", {}).get("n/a", "") else ""

        contactLenses_family = str(extension_value.get("contactLenses", {}).get("family", "")) if extension_value and extension_value.get("contactLenses", {}) and extension_value.get("contactLenses", {}).get("family", "") else ""
        contactLenses_locker = str(extension_value.get("contactLenses", {}).get("locker", "")) if extension_value and extension_value.get("contactLenses", {}) and extension_value.get("contactLenses", {}).get("locker", "") else ""
        contactLenses_na = str(extension_value.get("contactLenses", {}).get("n/a", "")) if extension_value and extension_value.get("contactLenses", {}) and extension_value.get("contactLenses", {}).get("n/a", "") else ""

        jewelry_family = str(extension_value.get("jewelry", {}).get("family", "")) if extension_value and extension_value.get("jewelry", {}) and extension_value.get("jewelry", {}).get("family", "") else ""
        jewelry_locker = str(extension_value.get("jewelry", {}).get("locker", "")) if extension_value and extension_value.get("jewelry", {}) and extension_value.get("jewelry", {}).get("locker", "") else ""
        jewelry_na = str(extension_value.get("jewelry", {}).get("n/a", "")) if extension_value and extension_value.get("jewelry", {}) and extension_value.get("jewelry", {}).get("n/a", "") else ""


        safetyAlertGiven_na = str(extension_value.get("safetyAlertGiven", {}).get("n/a", "")) if extension_value and extension_value.get("safetyAlertGiven", {}) and extension_value.get("safetyAlertGiven", {}).get("n/a", "") else ""
        safetyAlertGiven_yes = str(extension_value.get("safetyAlertGiven", {}).get("yes", "")) if extension_value and extension_value.get("safetyAlertGiven", {}) and extension_value.get("safetyAlertGiven", {}).get("yes", "") else ""

        specify = str(extension_value.get("specify", "")) if extension_value and extension_value.get("specify", "") else ""
        ready = str(extension_value.get("ready", "")) if extension_value and extension_value.get("ready", "") else ""

        transferVia_ambulatory = str(extension_value.get("transferVia", {}).get("ambulatory", "")) if extension_value and extension_value.get("transferVia", {}) and extension_value.get("transferVia", {}).get("ambulatory", "") else ""
        transferVia_recliner = str(extension_value.get("transferVia", {}).get("recliner", "")) if extension_value and extension_value.get("transferVia", {}) and extension_value.get("transferVia", {}).get("recliner", "") else ""
        transferVia_stretcher = str(extension_value.get("transferVia", {}).get("stretcher", "")) if extension_value and extension_value.get("transferVia", {}) and extension_value.get("transferVia", {}).get("stretcher", "") else ""
        transferVia_transport = str(extension_value.get("transferVia", {}).get("transport", "")) if extension_value and extension_value.get("transferVia", {}) and extension_value.get("transferVia", {}).get("transport", "") else ""

        rnName = str(extension_value.get("rnName", "")) if extension_value and extension_value.get("rnName", "") else ""
        date = str(extension_value.get("date", "")) if extension_value and extension_value.get("date", "") else ""
        time = str(extension_value.get("time", "")) if extension_value and extension_value.get("time", "") else ""
        additional_comments = str(extension_value.get("comment", "")) if extension_value and extension_value.get("comment", "") else ""

        weightObservationUnit = str(extension_value.get("weightObservationList", [{}])[0].get("value", {}).get("unit", "")) if extension_value and extension_value.get("weightObservationList", [{}])[0] and extension_value.get("weightObservationList", [{}])[0].get("value", {}) and extension_value.get("weightObservationList", [{}])[0].get("value", {}).get("unit", "") else ""
        weightObservationValue = str(extension_value.get("weightObservationList", [{}])[0].get("value", {}).get("value", "")) if extension_value and extension_value.get("weightObservationList", [{}])[0] and extension_value.get("weightObservationList", [{}])[0].get("value", {}) and extension_value.get("weightObservationList", [{}])[0].get("value", {}).get("value", "") else ""
        heightObservationUnit = str(extension_value.get("heightObservationList", [{}])[0].get("value", {}).get("unit", "")) if extension_value and extension_value.get("heightObservationList", [{}])[0] and extension_value.get("heightObservationList", [{}])[0].get("value", {}) and extension_value.get("weightObservationList", [{}])[0].get("value", {}).get("unit", "") else ""
        heightObservationValue = str(extension_value.get("heightObservationList", [{}])[0].get("value", {}).get("value", "")) if extension_value and extension_value.get("heightObservationList", [{}])[0] and extension_value.get("heightObservationList", [{}])[0].get("value", {}) and extension_value.get("weightObservationList", [{}])[0].get("value", {}).get("value", "") else ""

        bmi = str(extension_value.get("bmi", "")) if extension_value and extension_value.get("bmi", "") else ""

        form_response_id = str(document.get("identifier", [{}])[0].get("value", {}).get("value", "")) if document.get("identifier", [{}])[0] and document.get("identifier", [{}])[0].get("value", {}) and document.get("identifier", [{}])[0].get("value", {}).get("value", "") else ""
        status = str(document.get("status", {}).get("value", "")) if document.get("status", {}) and document.get("status", {}).get("value", "") else "" # Update the default value to an empty string
        patientId = str(document.get("subject", "")) if document.get("subject", "") else ""

        markToBeDeleted = document.get("markToBeDeleted", "") if document.get("markToBeDeleted", "") else ""
        lastUpdatedBy = str(document.get("lastUpdatedBy", {}).get("reference", "")) if document.get("lastUpdatedBy", {}) and document.get("lastUpdatedBy", {}).get("reference", "") else ""
        mongo_date_created = document.get("dateCreated", "")
        mongo_last_updated = document.get("lastUpdated", "")

    # ... (your code before the loop)

    # Fetch existing record
        config.mysql_cursor.execute("SELECT id FROM completePreprocedureAssessment WHERE mongo_id = %s", (mongo_id,))
        existing_record = config.mysql_cursor.fetchone()

        if existing_record:
        # If the record exists, perform an UPDATE operation
            print(f"Found an existing record which has been updated after {latest_mongo_lastUpdated} ......So updating ")
            try:
                config.mysql_cursor.execute("UPDATE completePreprocedureAssessment SET service_request_id = %s, created_by_user = %s , patient_name = %s, patientDob = %s, dateOfSurgery = %s, surgeryName = %s, surgeon_name = %s, primaryPhone = %s, alternatePhone = %s, date1 = %s, admitTime = %s, patientIdentification_yes = %s, patientIdentification_no = %s, procedureVerified_yes = %s, procedureVerified_no = %s, solidsFormDate = %s, solidsFormTime = %s, clearFluidFormDateTime = %s, clearFluidFormTime = %s, lastHomeMedicationDate = %s , lastHomeMedicationTime = %s, allergyReviewed_yes = %s, allergyReviewed_no = %s, allergyBand_na = %s, allergyBand_yes = %s, bloodPressure = %s, pulse = %s, regular = %s, irregular = %s, respiratory = %s, temperature = %s,temperature_unit = %s, oxygen = %s, painScale = %s, scaleType_face = %s, scaleType_flacc = %s, scaleType_numerical= %s, scaleType_pain_ad = %s, historyPhysical_yes = %s, historyPhysical_no = %s, glucoseResult_na = %s, glucoseResult_yes = %s, mg = %s, date2 = %s, mgTime = %s, pregancyTest_control_good = %s, pregancyTest_na = %s, pregancyTest_yes_attach_strip = %s, skinPrep_na = %s, skinPrep_yes_attach_strip = %s, advanceDirective_na = %s, advanceDirective_yes = %s, verificationpostOperative = %s, name = %s, phone = %s, dispositiorOfBelongings_family = %s, dispositiorOfBelongings_locker = %s, valuablesGivenSecurity_na = %s, valuablesGivenSecurity_yes = %s, dentures_family = %s, dentures_locker = %s, dentures_na = %s, hearingAid_family = %s, hearingAid_locker = %s, hearingAid_na = %s, glasses_family = %s, glasses_locker = %s, glasses_na = %s, prosthesisBrace_family = %s, prosthesisBrace_locker = %s, prosthesisBrace_na = %s, contactLenses_family = %s, contactLenses_locker = %s, contactLenses_na = %s, jewelry_family = %s, jewelry_locker = %s, jewelry_na = %s, safetyAlertGiven_na = %s, safetyAlertGiven_yes = %s, specify = %s, ready = %s, transferVia_ambulatory = %s, transferVia_recliner = %s, transferVia_stretcher = %s, transferVia_transport = %s ,rnName = %s,date = %s,time = %s,additional_comments = %s, weightObservationUnit = %s, weightObservationValue = %s, heightObservationUnit = %s, heightObservationValue = %s, bmi = %s, form_response_id = %s, status = %s, patientId = %s,lastUpdatedBy = %s,markToBeDeleted = %s, mongo_date_created = %s, mongo_last_updated = %s WHERE mongo_id = %s ",(service_request_id, created_by_user, patient_name, patientDob, datOfSurgery, surgeryName, surgeon_name, primaryPhone, alternatePhone, date1, admitTime, patientIdentification_yes, patientIdentification_no, procedureVerified_yes, procedureVerified_no, solidsFormDate, solidsFormTime, clearFluidFormDateTime, clearFluidFormTime, lastHomeMedicationDate, lastHomeMedicationTime, allergyReviewed_yes, allergyReviewed_no, allergyBand_na, allergyBand_yes, bloodPressure, pulse, regular, irregular, respiratory, temperature,temperature_unit, oxygen, painScale, scaleType_face, scaleType_flacc, scaleType_numerical, scaleType_pain_ad, historyPhysical_yes, historyPhysical_no, glucoseResult_na, glucoseResult_yes, mg, date2, mgTime, pregancyTest_control_good, pregancyTest_na, pregancyTest_yes_attach_strip, skinPrep_na, skinPrep_yes_attach_strip, advanceDirective_na, advanceDirective_yes, verificationpostOperative, name, phone, dispositiorOfBelongings_family, dispositiorOfBelongings_locker, valuablesGivenSecurity_na, valuablesGivenSecurity_yes, dentures_family, dentures_locker, dentures_na, hearingAid_family, hearingAid_locker, hearingAid_na, glasses_family, glasses_locker, glasses_na, prosthesisBrace_family, prosthesisBrace_locker, prosthesisBrace_na, contactLenses_family, contactLenses_locker, contactLenses_na, jewelry_family, jewelry_locker, jewelry_na, safetyAlertGiven_na, safetyAlertGiven_yes, specify, ready, transferVia_ambulatory, transferVia_recliner, transferVia_stretcher, transferVia_transport,rnName,date,time,additional_comments, weightObservationUnit, weightObservationValue, heightObservationUnit, heightObservationValue, bmi, form_response_id, status, patientId,lastUpdatedBy,markToBeDeleted, mongo_date_created , mongo_last_updated,mongo_id))
                print("Executed UPDATE for document with mongo_id:", mongo_id)
            except Exception as e:
                print(f"Error found {str(e)} while updating for document with mongo Id {mongo_id}")
        else:
        # If the record doesn't exist, perform an INSERT operation
            print("Found new record so inserting with Mongo-Id ",mongo_id)
            try:
                config.mysql_cursor.execute(
                """
                INSERT INTO completePreprocedureAssessment (mongo_id, service_request_id, created_by_user, patient_name, patientDob, dateOfSurgery, surgeryName, surgeon_name, primaryPhone, alternatePhone, date1, admitTime, patientIdentification_yes, patientIdentification_no, procedureVerified_yes, procedureVerified_no, solidsFormDate, solidsFormTime, clearFluidFormDateTime, clearFluidFormTime, lastHomeMedicationDate, lastHomeMedicationTime, allergyReviewed_yes, allergyReviewed_no, allergyBand_na, allergyBand_yes, bloodPressure, pulse, regular, irregular, respiratory, temperature,temperature_unit, oxygen, painScale, scaleType_face, scaleType_flacc, scaleType_numerical, scaleType_pain_ad, historyPhysical_yes, historyPhysical_no, glucoseResult_na, glucoseResult_yes, mg, date2, mgTime, pregancyTest_control_good, pregancyTest_na, pregancyTest_yes_attach_strip, skinPrep_na, skinPrep_yes_attach_strip, advanceDirective_na, advanceDirective_yes, verificationpostOperative, name, phone, dispositiorOfBelongings_family, dispositiorOfBelongings_locker, valuablesGivenSecurity_na, valuablesGivenSecurity_yes, dentures_family, dentures_locker, dentures_na, hearingAid_family, hearingAid_locker, hearingAid_na, glasses_family, glasses_locker, glasses_na, prosthesisBrace_family, prosthesisBrace_locker, prosthesisBrace_na, contactLenses_family, contactLenses_locker, contactLenses_na, jewelry_family, jewelry_locker, jewelry_na, safetyAlertGiven_na, safetyAlertGiven_yes, specify, ready, transferVia_ambulatory, transferVia_recliner, transferVia_stretcher, transferVia_transport,rnName,date,time,additional_comments, weightObservationUnit, weightObservationValue, heightObservationUnit, heightObservationValue, bmi, form_response_id, status, patientId,lastUpdatedBy,markToBeDeleted, mongo_date_created , mongo_last_updated
                ) VALUES (%s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s)
                """,
                (
                    mongo_id, service_request_id, created_by_user, patient_name, patientDob, datOfSurgery, surgeryName, surgeon_name, primaryPhone, alternatePhone, date1, admitTime, patientIdentification_yes, patientIdentification_no, procedureVerified_yes, procedureVerified_no, solidsFormDate, solidsFormTime, clearFluidFormDateTime, clearFluidFormTime, lastHomeMedicationDate, lastHomeMedicationTime, allergyReviewed_yes, allergyReviewed_no, allergyBand_na, allergyBand_yes, bloodPressure, pulse, regular, irregular, respiratory, temperature,temperature_unit, oxygen, painScale, scaleType_face, scaleType_flacc, scaleType_numerical, scaleType_pain_ad, historyPhysical_yes, historyPhysical_no, glucoseResult_na, glucoseResult_yes, mg, date2, mgTime, pregancyTest_control_good, pregancyTest_na, pregancyTest_yes_attach_strip, skinPrep_na, skinPrep_yes_attach_strip, advanceDirective_na, advanceDirective_yes, verificationpostOperative, name, phone, dispositiorOfBelongings_family, dispositiorOfBelongings_locker, valuablesGivenSecurity_na, valuablesGivenSecurity_yes, dentures_family, dentures_locker, dentures_na, hearingAid_family, hearingAid_locker, hearingAid_na, glasses_family, glasses_locker, glasses_na, prosthesisBrace_family, prosthesisBrace_locker, prosthesisBrace_na, contactLenses_family, contactLenses_locker, contactLenses_na, jewelry_family, jewelry_locker, jewelry_na, safetyAlertGiven_na, safetyAlertGiven_yes, specify, ready, transferVia_ambulatory, transferVia_recliner, transferVia_stretcher, transferVia_transport,rnName,date,time,additional_comments, weightObservationUnit, weightObservationValue, heightObservationUnit, heightObservationValue, bmi, form_response_id, status, patientId,lastUpdatedBy,markToBeDeleted, mongo_date_created , mongo_last_updated
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