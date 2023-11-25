from datetime import datetime, timedelta
import config

try:
    now = datetime.now()
    print("start_time ...", now)

    mongoConnect = config.get_mongo_client()
    serviceRequest_collection = mongoConnect.get_database(config.mongo_db).get_collection("serviceRequest")
    appointment_collection = mongoConnect.get_database(config.mongo_db).get_collection("appointment")

    config.mysql_cursor.execute("CREATE TABLE IF NOT EXISTS QuestionnaireAudit (id INT AUTO_INCREMENT PRIMARY KEY, appointment_mongo_id TEXT, service_request_mongo_id TEXT,questionnaireResponseId TEXT, Name TEXT,Gender TEXT,DOB TEXT,Phone_number TEXT,Email_address TEXT,Email_sent TEXT ,SMS_sent TEXT, mongo_lastUpdated DATETIME,created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP, updated_datetime DATETIME ON UPDATE CURRENT_TIMESTAMP)")

    def getPatientDetails(service_request_mongo_id):
        patientDetails = {}
        mongo_query = {"_id":int(service_request_mongo_id)}
        mongo_results = serviceRequest_collection.find(mongo_query)


        for document in mongo_results:
            for extension_values in document.get("extension", [{}]):
                if extension_values.get("url","") == "patientDetails":
                    lastName = str(extension_values.get("value",[{}])[0].get("lastName","")) if extension_values.get("value",[{}]) and extension_values.get("value",[{}])[0] and extension_values.get("value",[{}])[0].get("lastName","") else ""
                    firstName = str(extension_values.get("value",[{}])[0].get("firstName","")) if extension_values.get("value",[{}]) and extension_values.get("value",[{}])[0] and extension_values.get("value",[{}])[0].get("firstName","") else ""

                    patientDetails["Name"] = firstName + " " + lastName
                    patientDetails["Gender"] = str(extension_values.get("value",[{}])[0].get("gender",{}).get("text","")) if extension_values.get("value",[{}]) and extension_values.get("value",[{}])[0] and extension_values.get("value",[{}])[0].get("gender",{}) and extension_values.get("value",[{}])[0].get("gender",{}).get("text","") else ""
                    patientDetails["DOB"] = str(extension_values.get("value",[{}])[0].get("birthDate","")) if extension_values.get("value",[{}]) and extension_values.get("value",[{}])[0] and extension_values.get("value",[{}])[0].get("birthDate","") else ""
                    patientDetails["Phone_number"] = str(extension_values.get("value",[{}])[0].get("phoneNo","")) if extension_values.get("value",[{}]) and extension_values.get("value",[{}])[0] and extension_values.get("value",[{}])[0].get("phoneNo","") else ""
                    patientDetails["Email_address"] = str(extension_values.get("value",[{}])[0].get("email","")) if extension_values.get("value",[{}]) and extension_values.get("value",[{}])[0] and extension_values.get("value",[{}])[0].get("email","") else ""


        return patientDetails


    try:
        print(f"Going to fetch latest mongo_lastUpdated in QuestionnaireAudit Table")

        config.mysql_cursor.execute("SELECT mongo_lastUpdated FROM QuestionnaireAudit ORDER BY mongo_lastUpdated DESC LIMIT 1")
        latest_record = config.mysql_cursor.fetchone()

        if latest_record:
            latest_mongo_lastUpdated = latest_record[0]
            print(f"Found latest mongo_lastUpdated in QuestionnaireAudit as {latest_mongo_lastUpdated}")
            # Your code here
            # Modify mongo_query to include a condition on lastUpdated
            mongo_query = {
                "extension.url": "questionnaire",
                "lastUpdated": {"$gt": latest_mongo_lastUpdated}
            }
            print("Updated MongoDB query with lastUpdated condition:", mongo_query)
        else:
            mongo_query = {"extension.url": "questionnaire"}
            print("No records found, using default MongoDB query:", mongo_query)

        mongo_results = appointment_collection.find(mongo_query)
    except Exception as e:
        print("A error occurred while performing mongo operations :", str(e))

    for document in mongo_results:
    # Extract data from MongoDB document
        appointment_mongo_id = str(document["_id"])
        service_request_mongo_id = str(document.get("basedOn", [{}])[0]) if document.get("basedOn", [{}]) and document.get("basedOn", [{}])[0] else ""
        mongo_lastUpdated = document.get("lastUpdated", "")
        extensionArray = document.get("extension", [{}]) if document.get("extension", [{}]) else []
        for extension in extensionArray:
            if extension.get("url") == "questionnaire":
               extensionValue = extension.get("value") if extension.get("value") else []
               Sms_sent = "SENT"
               Email_sent = "SENT"
               for questionnaireResponse in extensionValue:
                    questionnaireResponseId = questionnaireResponse.get("questionnaireResponseId") if questionnaireResponse.get("questionnaireResponseId") else ""
                    config.mysql_cursor.execute("SELECT id FROM QuestionnaireAudit WHERE questionnaireResponseId = %s", (questionnaireResponseId,))

                    existing_record = config.mysql_cursor.fetchone()

                    if not existing_record:
                        print(f"Found new questionnaireResponseId with id - {questionnaireResponseId} in appointment Collection with Mongo-id - {appointment_mongo_id} so inserting this entry in QuestionnaireAudit table")
                        patientDetails = getPatientDetails(service_request_mongo_id)
                        config.mysql_cursor.execute(
                                    """
                                    INSERT INTO QuestionnaireAudit (appointment_mongo_id, service_request_mongo_id, questionnaireResponseId, Name, Gender,
                                        DOB, Phone_number, Email_address, Email_sent, SMS_sent,mongo_lastUpdated
                                    ) VALUES (%s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s)
                                    """,
                                    (
                                        appointment_mongo_id,service_request_mongo_id, questionnaireResponseId, patientDetails["Name"], patientDetails["Gender"], patientDetails["DOB"], patientDetails["Phone_number"], patientDetails["Email_address"], Email_sent, Sms_sent,mongo_lastUpdated

                                    )
                                    )
                        config.mysql_connection.commit()
                        print(f"Inserted ROW with questionnaireResponseId - {questionnaireResponseId} in QuestionnaireAudit table and appointment_mongo_id - {appointment_mongo_id}")
                    else:
                        print(f"Record with questionnaireResponseId - {questionnaireResponseId} already exists in QuestionnaireAudit table")

    now = datetime.now()
    print("end_time ...", now)


except Exception as e:
    print("A final error occurred as :", str(e))

finally:
    config.mysql_cursor.close()
    config.mysql_connection.close()
    mongoConnect.close()