from datetime import datetime
import config

try:
    now = datetime.now()
    print("start_time ...", now)
    mongoConnect = config.get_mongo_client()
    formResponse_collection = mongoConnect.get_database(config.mongo_db).get_collection("formResponse")
    formBuilder_collection = mongoConnect.get_database(config.mongo_db).get_collection("formBuilder")

    config.mysql_cursor.execute("CREATE TABLE IF NOT EXISTS completeNursingAssessmentPartA (id INT AUTO_INCREMENT PRIMARY KEY, mongo_id TEXT, service_request_id TEXT, created_by_user TEXT,patientName TEXT, patientDob TEXT, datOfSurgery TEXT, surgeryName TEXT, surgeonName TEXT, primaryPhon TEXT, alternatePhon TEXT, patientPreferredLanguage TEXT, otherPreferredLanguage TEXT, interpreterRequired_family TEXT, interpreterRequired_language_line_id_number TEXT, patientDesignated TEXT, informationObtained TEXT, languageLineId TEXT, advancedDirectives_yes TEXT, advancedDirectives_no TEXT, copyOnChart_yes TEXT, copyOnChart_no TEXT, allergies_no_known_allergy TEXT, allergies_yes TEXT, allergiesTo TEXT, allergies_reactions TEXT, surgicalHistory_denied TEXT, surgicalHistory_yes TEXT, surgicalHistoryYes TEXT, anesthesiaHistory_yes TEXT, anesthesiaHistory_no TEXT, bloodRelatives_yes TEXT, bloodRelatives_no TEXT, adverseReaction_yes TEXT, adverseReaction_no TEXT, reactions1 TEXT, currentlyPain_yes TEXT, currentlyPain_no TEXT, painScale TEXT, painQuality TEXT, scaleType_Numerical TEXT, scaleType_face_ TEXT, scaleType_flacc__ TEXT, scaleType_pain_ad TEXT, painDuration_value TEXT, painLocation TEXT, visualImpairment_yes TEXT, visualImpairment_no TEXT, visualImpairmentText TEXT, auditoryImpairment_yes TEXT, auditoryImpairment_no TEXT, auditoryImpairmentText TEXT, congnitiveImpairment_yes TEXT, congnitiveImpairment_no TEXT, congnitiveImpairmentText TEXT, barriersToLearning_yes TEXT, barriersToLearning_no TEXT, learningBarrierType_value TEXT, ifOther TEXT, mobilityImpairment_yes TEXT, mobilityImpairment_no TEXT, mobilityImpairment_value TEXT, fallRiskAssessment_all_surgical_patients TEXT, fallRiskAssessment_fall_with_harm_risk TEXT, yellowWristband_needs_assistance TEXT, yellowWristband_current_falls TEXT, yellowWristband_85_years TEXT, yellowWristband_bone_condition TEXT, yellowWristband_has_bleeding_disorder TEXT, reportedNurologicProblem_yes TEXT, reportedNurologicProblem_no TEXT, seizures_yes TEXT, seizures_no TEXT, lastEpisode TEXT, onMeds_yes TEXT, onMeds_no TEXT, seizureHeadachesMigraines_yes TEXT, seizureHeadachesMigraines_no TEXT, frequency TEXT, lastEpisode1 TEXT, onMeds1_no TEXT, onMeds1_yes TEXT, strokeCVATIA_yes TEXT, strokeCVATIA_no TEXT, lastEpisode2 TEXT, weakness TEXT, vpShuntPlace_yes TEXT, vpShuntPlace_no TEXT, VpShunt TEXT, cronicDizziness_yes TEXT, cronicDizziness_no TEXT, lastEpisode3 TEXT, onMeds2_yes TEXT, onMeds2_no TEXT, comment TEXT, cardiovascularProblem_yes TEXT, cardiovascularProblem_no TEXT, hypertension_yes TEXT, hypertension_no TEXT, mi_yes TEXT, mi_no TEXT, dateOfEvent TEXT, angina_yes TEXT, angina_no TEXT, lastEpisode4 TEXT, chestPain_yes TEXT, chestPain_no TEXT, describe_ TEXT, arterioscleroticHeartDisease_yes TEXT, arterioscleroticHeartDisease_no TEXT, onset TEXT, congestiveHeartFailure_yes TEXT, congestiveHeartFailure_no TEXT, onset1 TEXT, swellingOfAnklesFeet_yes TEXT, swellingOfAnklesFeet_no TEXT, pitting_yes TEXT, pitting_no TEXT, arrhythmia_yes TEXT, arrhythmia_no TEXT, typeArrhythmia TEXT, murmurs_yes TEXT, murmurs_no TEXT, peripheralVascularDisease_yes TEXT, peripheralVascularDisease_no TEXT, otherPeripheral TEXT, pacemaker_yes TEXT, pacemaker_no TEXT, pacemakerDate TEXT, aicd_yes TEXT, aicd_no TEXT, aicdDate TEXT, cardiackStentBypass_yes TEXT, cardiackStentBypass_no TEXT, cardiack TEXT, pastBloodTransfusions_yes TEXT, pastBloodTransfusions_no TEXT, transfusionsReson TEXT, transfusionsDate TEXT, adverseReaction1_yes TEXT, adverseReaction1_no TEXT, describe3 TEXT, active_yes TEXT, active_no TEXT, active1 TEXT, withoutStoping_yes TEXT, withoutStoping_no TEXT, flights TEXT, atolerateLyingFlatPeriodOfTime_yes TEXT, atolerateLyingFlatPeriodOfTime_no TEXT, tolerate TEXT, onMeds3_yes TEXT, onMeds3_no TEXT, comment1 TEXT, form_response_id TEXT, status TEXT, patientId TEXT, lastUpdatedBy TEXT, markToBeDeleted BOOLEAN, mongo_date_created DATETIME, mongo_last_updated DATETIME, created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP, updated_datetime DATETIME ON UPDATE CURRENT_TIMESTAMP)")
    print("CREATED Table completeNursingAssessmentPartA")
    config.mysql_cursor.execute("CREATE TABLE IF NOT EXISTS completeNursingAssessmentPartB (id INT AUTO_INCREMENT PRIMARY KEY, mongo_id TEXT, service_request_id TEXT,respiratoryProblem_yes TEXT, respiratoryProblem_no TEXT, sleepApnea_yes TEXT, sleepApnea_no TEXT, daignosed TEXT, sleepStudyDone_yes TEXT, sleepStudyDone_no TEXT, sleepStudyDone_uses_cpap TEXT, asthma_yes TEXT, asthma_no TEXT, lastEpisode5 TEXT, lastHospitalization TEXT, intubation_yes TEXT, intubation_no TEXT, intubationYes TEXT, chronicCoughBronchitis_yes TEXT, chronicCoughBronchitis_no TEXT, lastEpisode6 TEXT, lastHospitalization1 TEXT, intubation1_yes TEXT, intubation1_no TEXT, intubationYes1 TEXT, emphysema_yes TEXT, emphysema_no TEXT, lastEpisode7 TEXT, lastHospitalization2 TEXT, intubation2_yes TEXT, intubation2_no TEXT, intubationYes2 TEXT, pneumonia_yes TEXT, pneumonia_no TEXT, lastEpisode8 TEXT, lastHospitalization3 TEXT, intubation3_yes TEXT, intubation3_no TEXT, intubationYes3 TEXT, tuberculosis_yes TEXT, tuberculosis_no TEXT, lastEpisode9 TEXT, lastHospitalization4 TEXT, intubation4_yes TEXT, intubation4_no TEXT, intubationYes4 TEXT, recentUpperRespiratoryInfection_yes TEXT, recentUpperRespiratoryInfection_no TEXT, lastEpisode01 TEXT, lastHospitalization05 TEXT, intubation5_yes TEXT, intubation5_no TEXT, intubationYes5 TEXT, lastEpisodeOther TEXT, lastEpisode02 TEXT, lastHospitalization7 TEXT, intubation7_yes TEXT, intubation7_no TEXT, intubationYes7 TEXT, onMeds4_yes TEXT, onMeds4_no TEXT, comment2 TEXT, reportedGastroinestinalProblem_yes TEXT, reportedGastroinestinalProblem_no TEXT, curentDiet TEXT, difficultySwallowing_yes TEXT, difficultySwallowing_no TEXT, pepticUlcerDisease_yes TEXT, pepticUlcerDisease_no TEXT, gerdHeartBurn_yes TEXT, gerdHeartBurn_no TEXT, hiatalHernia_yes TEXT, hiatalHernia_no TEXT, hepatitis_yes TEXT, hepatitis_no TEXT, hepatitisType TEXT, hepatitisDiagnosed TEXT, hepatitisTreated TEXT, irritableBowelSyndromeCrohn_yes TEXT, irritableBowelSyndromeCrohn_no TEXT, bowelActivity TEXT, unintentionalweightLoss_yes TEXT, unintentionalweightLoss_no TEXT, onMeds5_yes TEXT, onMeds5_no TEXT, comment3 TEXT, reportRenalProblem_yes TEXT, reportRenalProblem_no TEXT, kidneyDisease_yes TEXT, kidneyDisease_no TEXT, urinaryInfections_yes TEXT, urinaryInfections_no TEXT, enlargedProstate_yes TEXT, enlargedProstate_no TEXT, difficultyUrinating_yes TEXT, difficultyUrinating_no TEXT, bloodUrine_yes TEXT, bloodUrine_no TEXT, onDialysis_yes TEXT, onDialysis_no TEXT, dialysisDays TEXT, dialysisType TEXT, shuntFistualAccess_yes TEXT, shuntFistualAccess_no TEXT, selfCatherization_yes TEXT, selfCatherization_no TEXT, otherCatherization TEXT, onMeds6_yes TEXT, onMeds6_no TEXT, comments TEXT, reportedMusculoskeletalProblem_yes TEXT, reportedMusculoskeletalProblem_no TEXT, arthritis_yes TEXT, arthritis_no TEXT, gout_yes TEXT, gout_no TEXT, osteoperosis_yes TEXT, osteoperosis_no TEXT, muscleWeakness_yes TEXT, muscleWeakness_no TEXT, fractures TEXT, others TEXT, onMeds7_yes TEXT, onMeds7_no TEXT, musculoskeletalComment TEXT, reportedHematologyProblem_yes TEXT, reportedHematologyProblem_no TEXT, bloodTransfusions_yes TEXT, bloodTransfusions_no TEXT, bloodTransfusionsReason TEXT, bloodTransfusionsDate TEXT, adverseReactions_yes TEXT, adverseReactions_no TEXT, adverseReactionsDescribe TEXT, bleedingProblem_yes TEXT, bleedingProblem_no TEXT, bruiseEasily_yes TEXT, bruiseEasily_no TEXT, sickleCellDisease_yes TEXT, sickleCellDisease_no TEXT, hivAIDS_yes TEXT, hivAIDS_no TEXT, leukemia_yes TEXT, leukemia_no TEXT, leukemiaYes TEXT, treatmentType_chemotherapy TEXT, treatmentType_bone_marrow_transplant TEXT, cancer_yes TEXT, cancer_no TEXT, treatmentTypeCancer__chemotherapy TEXT, treatmentTypeCancer_radiation_therapy TEXT, onMedsHematology_yes TEXT, onMedsHematology_no TEXT, hematologyComment TEXT, endocrineProblem_yes TEXT, endocrineProblem_no TEXT, diabetes_yes TEXT, diabetes_no TEXT, type_type_i TEXT, type_type_ii TEXT, diabetesDate TEXT, BloodGlucose_yes TEXT, BloodGlucose_no TEXT, bloodGlucoseMonitoring TEXT, lowRnge TEXT, highRange TEXT, hospitalizationDiabetes TEXT, reason TEXT, thyroidDisease_yes TEXT, thyroidDisease_no TEXT, HistorySteroidUse_yes TEXT, HistorySteroidUse_no TEXT, steroidHistory TEXT, onMeds8_yes TEXT, onMeds8_no TEXT, EndocrineComment TEXT, created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP, updated_datetime DATETIME ON UPDATE CURRENT_TIMESTAMP)")
    print("CREATED Table completeNursingAssessmentPartB")
    config.mysql_cursor.execute("CREATE TABLE IF NOT EXISTS completeNursingAssessmentPartC (id INT AUTO_INCREMENT PRIMARY KEY, mongo_id TEXT, service_request_id TEXT, reportedSkinProblem_no TEXT, reportedSkinProblem_yes TEXT, intact_yes TEXT, intact_no TEXT, psoriasis_yes TEXT, psoriasis_no TEXT, eczdma_yes TEXT, eczdma_no TEXT, brusesCuts_yes TEXT, brusesCuts_no TEXT, rashesDescribe TEXT, rashesDescribeOther TEXT, skinComment TEXT, visualImpairmentGyn_yes TEXT, visualImpairmentGyn_no TEXT, visualImpairmentGyn_na TEXT, fynComment TEXT, menstrualPeriod TEXT, SapSmear TEXT, dateOfLastPelvicExam TEXT, pregnantNow_yes TEXT, pregnantNow_no TEXT, possibilityPregnantDescribe TEXT, lactating_yes TEXT, lactating_no TEXT, peds_na TEXT, deliveryType TEXT, preemie TEXT, birthWeight TEXT, immunizationDate_yes TEXT, immunizationDate_no TEXT, immunizationDetails TEXT, respiratoryInfection_no TEXT, respiratoryInfection_yes TEXT, respiratoryInfectionDetails TEXT, communicableDisease_measles TEXT, communicableDisease_chicken_pox TEXT, communicableDisease_tb TEXT, communicableDisease_meningitis TEXT, communicableDisease_na TEXT, communicableDiseaseOthers TEXT, pedsComment TEXT, culturalSurgery_yes TEXT, culturalSurgery_no TEXT, dayOfSurgery TEXT, livesAlone_yes TEXT, livesAlone_no TEXT, typeOfHouse TEXT, stairs TEXT, alcohol_yes TEXT, alcohol_no TEXT, freq TEXT, alcoholOther TEXT, alcoholAmount TEXT, drugs_yes TEXT, drugs_no TEXT, drugsFreq TEXT, drugsOther TEXT, drugslAmount TEXT, lastDoseAmount TEXT, caffeine_yes TEXT, caffeine_no TEXT, caffeineFreq TEXT, caffeineOther TEXT, caffeineAmount TEXT, cigarettesVaping_yes TEXT, cigarettesVaping_no TEXT, packsPerDay TEXT, lastCigarette TEXT, cigars_yes TEXT, cigars_no TEXT, cigarsPerDay TEXT, lastCigar TEXT, readinessToQuit TEXT, psychosocialComment TEXT, reportedPsychiatricProblem_yes TEXT, reportedPsychiatricProblem_no TEXT, anxiety_yes TEXT, anxiety_no TEXT, hallucinations_yes TEXT, hallucinations_no TEXT, depressions_yes TEXT, depressions_no TEXT, postTraumaticReaction_yes TEXT, postTraumaticReaction_no TEXT, suicidalIdeations_yes TEXT, suicidalIdeations_no TEXT, suicidalOther TEXT, onMeds9_yes TEXT, onMeds9_no TEXT, followedByMD_yes TEXT, followedByMD_no TEXT, psychiatricComment TEXT, afraidOfAnyyone_yes TEXT, afraidOfAnyyone_no TEXT, afraidOfAnyyone_unable_to_access TEXT, afraid TEXT, unableAfraid TEXT, isAnyoneHurtingYou_yes TEXT, isAnyoneHurtingYou_no TEXT, isAnyoneHurtingYou_unable_to_access TEXT, hurting TEXT, unableHurting TEXT, talkSomeone_yes TEXT, talkSomeone_no TEXT, talkSomeone_unable_to_access TEXT, unableTalk TEXT, abuseScreenComments TEXT, placeYouDoNotFeelSafe_yes TEXT, placeYouDoNotFeelSafe_no TEXT, placeYouDoNotFeelSafe_unable_to_access TEXT, feelSafeYes TEXT, unableFeelSafe TEXT, isAnyoneHurtingYou1_yes TEXT, isAnyoneHurtingYou1_no TEXT, isAnyoneHurtingYou1_unable_to_access TEXT, hurtingYes TEXT, unableToHurting TEXT, afraidOfAnyone_yes TEXT, afraidOfAnyone_no TEXT, afraidOfAnyone_unable_to_access TEXT, personAfraid TEXT, otherPersonAfraid TEXT, unableToAssess TEXT, abuseScreenComments1 TEXT, nameOfPhysician TEXT, nameOfPhysicianDate TEXT, nameOfPhysicianTime TEXT, nameOfSocialWorker TEXT, nameOfSocialWorkerDate TEXT, nameOfSocialWorkerTime TEXT , created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP, updated_datetime DATETIME ON UPDATE CURRENT_TIMESTAMP)")
    print("CREATED Table completeNursingAssessmentPartC")


    try:
        completeNursingAssessmentIds = [doc["_id"] for doc in formBuilder_collection.find({"module": "completeNursingAssessment"}, {"_id": 1})]
        mongo_query = {"form.reference": {"$in": completeNursingAssessmentIds}}
        mongo_results = formResponse_collection.find(mongo_query)
    except Exception as e:
        print("A error while performing mongo operations :", str(e))

    processed_count = 0  # Counter to track the number of processed records
    batch_size = 1000  # A  djust the batch size as per your needs
    batchA = []
    batchB = []
    batchC = []


    # ...
    for document in mongo_results:
        print("Document",document)
        mongo_id = str(document["_id"])
        print("Document with mongo id",mongo_id)
        service_request_id = str(document.get("basedOn", [{}])[0].get("reference", "")) if document.get("basedOn", [{}])[0] and document.get("basedOn", [{}])[0].get("reference", "")  else ""
        created_by_user = str(document.get("createdByUser", "")) if document.get("createdByUser", "") else ""
        extension_value = document.get("extension", [{}])[0].get("value", [{}])[0].get("formData", {}) if document.get("extension", [{}])[0] and document.get("extension", [{}])[0].get("value", [{}])[0] and document.get("extension", [{}])[0].get("value", [{}])[0].get("formData", {}) else None
        patientName = str(extension_value.get("patientName", "")) if extension_value and extension_value.get("patientName", "") else ""
        patientDob = str(extension_value.get("patientDob", "")) if extension_value and extension_value.get("patientDob", "") else ""
        datOfSurgery = str(extension_value.get("datOfSurgery", "")) if extension_value and extension_value.get("datOfSurgery", "") else ""
        surgeryName = str(extension_value.get("surgeryName", "")) if extension_value and extension_value.get("surgeryName", "") else ""
        surgeonName = str(extension_value.get("surgeonName", "")) if extension_value and extension_value.get("surgeonName", "") else ""
        primaryPhon = str(extension_value.get("primaryPhon", "")) if extension_value and extension_value.get("primaryPhon", "") else ""
        alternatePhon = str(extension_value.get("alternatePhon", "")) if extension_value and extension_value.get("alternatePhon", "") else ""
        patientPreferredLanguage = str(extension_value.get("patientPreferredLanguage", {}).get("value", "")) if extension_value.get("patientPreferredLanguage", {}) and extension_value.get("patientPreferredLanguage", {}).get("value", "") else ""
        otherPreferredLanguage = str(extension_value.get("otherPreferredLanguage", "")) if extension_value and extension_value.get("otherPreferredLanguage", "") else ""
        interpreterRequired_family = str(extension_value.get("interpreterRequired", {}).get("family_(as_indicated_by_patient)", "")) if extension_value.get("interpreterRequired", {}) and extension_value.get("interpreterRequired", {}).get("family_(as_indicated_by_patient)", "") else ""
        interpreterRequired_language_line_id_number = str(extension_value.get("interpreterRequired", {}).get("language_line_id_number", "")) if extension_value.get("interpreterRequired", {}) and extension_value.get("interpreterRequired", {}).get("language_line_id_number", "") else ""


        patientDesignated = str(extension_value.get("patientDesignated", "")) if extension_value and extension_value.get("patientDesignated", "") else ""
        informationObtained = str(extension_value.get("informationObtained", "")) if extension_value and extension_value.get("informationObtained", "") else ""
        languageLineId = str(extension_value.get("languageLineId", "")) if extension_value and extension_value.get("languageLineId", "") else ""
        interpreterRequired_language_line_id_number = str(extension_value.get("interpreterRequired", {}).get("language_line_id_number", "")) if extension_value.get("interpreterRequired", {}) and extension_value.get("interpreterRequired", {}).get("language_line_id_number", "") else ""

        advancedDirectives_yes = str(extension_value.get("advancedDirectives", {}).get("yes", "")) if extension_value.get("advancedDirectives", {}) and extension_value.get("advancedDirectives", {}).get("yes", "") else ""
        advancedDirectives_no = str(extension_value.get("advancedDirectives", {}).get("no", "")) if extension_value.get("advancedDirectives", {}) and extension_value.get("advancedDirectives", {}).get("no", "") else ""
        copyOnChart_yes = str(extension_value.get("copyOnChart", {}).get("yes", "")) if extension_value.get("copyOnChart", {}) and extension_value.get("copyOnChart", {}).get("yes", "") else ""
        copyOnChart_no = str(extension_value.get("copyOnChart", {}).get("no", "")) if extension_value.get("copyOnChart", {}) and extension_value.get("copyOnChart", {}).get("no", "") else ""

        allergies_no_known_allergy = str(extension_value.get("allergies", {}).get("no_known_allergy", "")) if extension_value.get("allergies", {}) and extension_value.get("allergies", {}).get("no_known_allergy", "") else ""
        allergies_yes = str(extension_value.get("allergies", {}).get("yes", "")) if extension_value.get("allergies", {}) and extension_value.get("allergies", {}).get("yes", "") else ""
        allergiesTo = str(extension_value.get("allergiesTo", "")) if extension_value and extension_value.get("allergiesTo", "") else ""
        allergies_reactions = str(extension_value.get("reactions", "")) if extension_value and extension_value.get("reactions", "") else ""

        surgicalHistory_denied = str(extension_value.get("surgicalHistory", {}).get("denied", "")) if extension_value.get("surgicalHistory", {}) and extension_value.get("surgicalHistory", {}).get("denied", "") else ""
        surgicalHistory_yes = str(extension_value.get("surgicalHistory", {}).get("yes", "")) if extension_value.get("surgicalHistory", {}) and extension_value.get("surgicalHistory", {}).get("yes", "") else ""
        surgicalHistoryYes = str(extension_value.get("surgicalHistoryYes", "")) if extension_value and extension_value.get("surgicalHistoryYes", "") else ""
        anesthesiaHistory_yes = str(extension_value.get("anesthesiaHistory", {}).get("yes", "")) if extension_value.get("anesthesiaHistory", {}) and extension_value.get("anesthesiaHistory", {}).get("yes", "") else ""
        anesthesiaHistory_no = str(extension_value.get("anesthesiaHistory", {}).get("no", "")) if extension_value.get("anesthesiaHistory", {}) and extension_value.get("anesthesiaHistory", {}).get("no", "") else ""
        bloodRelatives_yes = str(extension_value.get("bloodRelatives", {}).get("yes", "")) if extension_value.get("bloodRelatives", {}) and extension_value.get("bloodRelatives", {}).get("yes", "") else ""
        bloodRelatives_no = str(extension_value.get("bloodRelatives", {}).get("no", "")) if extension_value.get("bloodRelatives", {}) and extension_value.get("bloodRelatives", {}).get("no", "") else ""
        adverseReaction_yes = str(extension_value.get("adverseReaction", {}).get("yes", "")) if extension_value.get("adverseReaction", {}) and extension_value.get("adverseReaction", {}).get("yes", "") else ""
        adverseReaction_no = str(extension_value.get("adverseReaction", {}).get("no", "")) if extension_value.get("adverseReaction", {}) and extension_value.get("adverseReaction", {}).get("no", "") else ""

        reactions1 = str(extension_value.get("reactions1", "")) if extension_value and extension_value.get("reactions1", "") else ""

        currentlyPain_yes = str(extension_value.get("currentlyPain", {}).get("yes", "")) if extension_value.get("currentlyPain", {}) and extension_value.get("currentlyPain", {}).get("yes", "") else ""
        currentlyPain_no = str(extension_value.get("currentlyPain", {}).get("no", "")) if extension_value.get("currentlyPain", {}) and extension_value.get("currentlyPain", {}).get("no", "") else ""

        painScale = str(extension_value.get("painScal", {}).get("value", "")) if extension_value.get("painScal", {}) and extension_value.get("painScal", {}).get("value", "") else ""
        painQuality = str(extension_value.get("painQuality", {}).get("value", "")) if extension_value.get("painQuality", {}) and extension_value.get("painQuality", {}).get("value", "") else ""

        scaleType_Numerical = str(extension_value.get("scaleType", {}).get("numerical", "")) if extension_value.get("scaleType", {}) and extension_value.get("scaleType", {}).get("numerical", "") else ""
        scaleType_face_ = str(extension_value.get("scaleType", {}).get("face_", "")) if extension_value.get("scaleType", {}) and extension_value.get("scaleType", {}).get("face_", "") else ""
        scaleType_flacc__ = str(extension_value.get("scaleType", {}).get("flacc__", "")) if extension_value.get("scaleType", {}) and extension_value.get("scaleType", {}).get("flacc__", "") else ""
        scaleType_pain_ad = str(extension_value.get("scaleType", {}).get("pain_ad", "")) if extension_value.get("scaleType", {}) and extension_value.get("scaleType", {}).get("pain_ad", "") else ""

        painDuration_value = str(extension_value.get("painDuration", {}).get("value", "")) if extension_value.get("painDuration", {}) and extension_value.get("painDuration", {}).get("value", "") else ""
        painLocation = str(extension_value.get("painLocation", "")) if extension_value and extension_value.get("painLocation", "") else ""

        visualImpairment_yes = str(extension_value.get("visualImpairment", {}).get("yes", "")) if extension_value.get("visualImpairment", {}) and extension_value.get("visualImpairment", {}).get("yes", "") else ""
        visualImpairment_no = str(extension_value.get("visualImpairment", {}).get("no", "")) if extension_value.get("visualImpairment", {}) and extension_value.get("visualImpairment", {}).get("no", "") else ""
        visualImpairmentText = str(extension_value.get("visualImpairmentText", "")) if extension_value and extension_value.get("visualImpairmentText", "") else ""

        auditoryImpairment_yes = str(extension_value.get("auditoryImpairment", {}).get("yes", "")) if extension_value.get("auditoryImpairment", {}) and extension_value.get("auditoryImpairment", {}).get("yes", "") else ""
        auditoryImpairment_no = str(extension_value.get("auditoryImpairment", {}).get("no", "")) if extension_value.get("auditoryImpairment", {}) and extension_value.get("auditoryImpairment", {}).get("no", "") else ""
        auditoryImpairmentText = str(extension_value.get("auditoryImpairmentText", "")) if extension_value and extension_value.get("auditoryImpairmentText", "") else ""

        congnitiveImpairment_yes = str(extension_value.get("congnitiveImpairment", {}).get("yes", "")) if extension_value.get("congnitiveImpairment", {}) and extension_value.get("congnitiveImpairment", {}).get("yes", "") else ""
        congnitiveImpairment_no = str(extension_value.get("congnitiveImpairment", {}).get("no", "")) if extension_value.get("congnitiveImpairment", {}) and extension_value.get("congnitiveImpairment", {}).get("no", "") else ""
        congnitiveImpairmentText = str(extension_value.get("congnitiveImpairmentText", "")) if extension_value and extension_value.get("congnitiveImpairmentText", "") else ""

        barriersToLearning_yes = str(extension_value.get("barriersToLearning", {}).get("yes", "")) if extension_value.get("barriersToLearning", {}) and extension_value.get("barriersToLearning", {}).get("yes", "") else ""
        barriersToLearning_no = str(extension_value.get("barriersToLearning", {}).get("no", "")) if extension_value.get("barriersToLearning", {}) and extension_value.get("barriersToLearning", {}).get("no", "") else ""
        learningBarrierType_value = str(extension_value.get("learningBarrierType", {}).get("value", "")) if extension_value.get("learningBarrierType", {}) and extension_value.get("learningBarrierType", {}).get("value", "") else ""
        ifOther = str(extension_value.get("ifOther", "")) if extension_value and extension_value.get("ifOther", "") else ""

        mobilityImpairment_yes = str(extension_value.get("mobilityImpairment", {}).get("yes", "")) if extension_value.get("mobilityImpairment", {}) and extension_value.get("mobilityImpairment", {}).get("yes", "") else ""
        mobilityImpairment_no = str(extension_value.get("mobilityImpairment", {}).get("no", "")) if extension_value.get("mobilityImpairment", {}) and extension_value.get("mobilityImpairment", {}).get("no", "") else ""
        mobilityImpairment_value = str(extension_value.get("mobilityImpairmentType", {}).get("value", "")) if extension_value.get("mobilityImpairmentType", {}) and extension_value.get("mobilityImpairmentType", {}).get("value", "") else ""

        fallRiskAssessment_all_surgical_patients = str(extension_value.get("fallRiskAssessment", {}).get("fall_risk:_all_surgical_patients", "")) if extension_value.get("fallRiskAssessment", {}) and extension_value.get("fallRiskAssessment", {}).get("fall_risk:_all_surgical_patients", "") else ""
        fallRiskAssessment_fall_with_harm_risk = str(extension_value.get("fallRiskAssessment", {}).get("fall_with_harm_risk", "")) if extension_value.get("fallRiskAssessment", {}) and extension_value.get("fallRiskAssessment", {}).get("fall_with_harm_risk", "") else ""

        yellowWristband_needs_assistance = str(extension_value.get("yellowWristband", {}).get("needs_assistance/close_monitoring_with_standing/walking/toileting/transferring", "")) if extension_value.get("yellowWristband", {}) and extension_value.get("yellowWristband", {}).get("needs_assistance/close_monitoring_with_standing/walking/toileting/transferring", "") else ""
        yellowWristband_current_falls = str(extension_value.get("yellowWristband", {}).get("current_falls_history_(within_last_6_months)", "")) if extension_value.get("yellowWristband", {}) and extension_value.get("yellowWristband", {}).get("current_falls_history_(within_last_6_months)", "") else ""
        yellowWristband_85_years = str(extension_value.get("yellowWristband", {}).get("85_years_or_older", "")) if extension_value.get("yellowWristband", {}) and extension_value.get("yellowWristband", {}).get("85_years_or_older", "") else ""
        yellowWristband_bone_condition = str(extension_value.get("yellowWristband", {}).get("has_bone_condition_(osteoporosis/cancer/previous_fractures)_or_long_steroid_use", "")) if extension_value.get("yellowWristband", {}) and extension_value.get("yellowWristband", {}).get("has_bone_condition_(osteoporosis/cancer/previous_fractures)_or_long_steroid_use", "") else ""
        yellowWristband_has_bleeding_disorder = str(extension_value.get("yellowWristband", {}).get("has_bleeding_disorder", "")) if extension_value.get("yellowWristband", {}) and extension_value.get("yellowWristband", {}).get("has_bleeding_disorder", "") else ""

        reportedNurologicProblem_yes = str(extension_value.get("reportedNurologicProblem", {}).get("yes", "")) if extension_value.get("reportedNurologicProblem", {}) and extension_value.get("reportedNurologicProblem", {}).get("yes", "") else ""
        reportedNurologicProblem_no = str(extension_value.get("reportedNurologicProblem", {}).get("no", "")) if extension_value.get("reportedNurologicProblem", {}) and extension_value.get("reportedNurologicProblem", {}).get("no", "") else ""

        seizures_yes = str(extension_value.get("seizures", {}).get("yes", "")) if extension_value.get("seizures", {}) and extension_value.get("seizures", {}).get("yes", "") else ""
        seizures_no = str(extension_value.get("seizures", {}).get("no", "")) if extension_value.get("seizures", {}) and extension_value.get("seizures", {}).get("no", "") else ""

        lastEpisode = str(extension_value.get("lastEpisode", "")) if extension_value and extension_value.get("lastEpisode", "") else ""

        onMeds_yes = str(extension_value.get("onMeds", {}).get("yes", "")) if extension_value.get("onMeds", {}) and extension_value.get("onMeds", {}).get("yes", "") else ""
        onMeds_no = str(extension_value.get("onMeds", {}).get("no", "")) if extension_value.get("onMeds", {}) and extension_value.get("onMeds", {}).get("no", "") else ""

        seizureHeadachesMigraines_yes = str(extension_value.get("seizureHeadachesMigraines", {}).get("yes", "")) if extension_value.get("seizureHeadachesMigraines", {}) and extension_value.get("seizureHeadachesMigraines", {}).get("yes", "") else ""
        seizureHeadachesMigraines_no = str(extension_value.get("seizureHeadachesMigraines", {}).get("no", "")) if extension_value.get("seizureHeadachesMigraines", {}) and extension_value.get("seizureHeadachesMigraines", {}).get("no", "") else ""

        frequency = str(extension_value.get("frequency", "")) if extension_value and extension_value.get("frequency", "") else ""

        lastEpisode1 = str(extension_value.get("lastEpisode1", "")) if extension_value and extension_value.get("lastEpisode1", "") else ""
        onMeds1_yes = str(extension_value.get("onMeds1", {}).get("yes", "")) if extension_value.get("onMeds1", {}) and extension_value.get("onMeds1", {}).get("yes", "") else ""
        onMeds1_no = str(extension_value.get("onMeds1", {}).get("no", "")) if extension_value.get("onMeds1", {}) and extension_value.get("onMeds1", {}).get("no", "") else ""

        strokeCVATIA_yes = str(extension_value.get("strokeCVATIA", {}).get("yes", "")) if extension_value.get("strokeCVATIA", {}) and extension_value.get("strokeCVATIA", {}).get("yes", "") else ""
        strokeCVATIA_no = str(extension_value.get("strokeCVATIA", {}).get("no", "")) if extension_value.get("strokeCVATIA", {}) and extension_value.get("strokeCVATIA", {}).get("no", "") else ""

        lastEpisode2 = str(extension_value.get("lastEpisode2", "")) if extension_value and extension_value.get("lastEpisode2", "") else ""

        weakness = str(extension_value.get("weakness", "")) if extension_value and extension_value.get("weakness", "") else ""

        vpShuntPlace_yes = str(extension_value.get("vpShuntPlace", {}).get("yes", "")) if extension_value.get("vpShuntPlace", {}) and extension_value.get("vpShuntPlace", {}).get("yes", "") else ""
        vpShuntPlace_no = str(extension_value.get("vpShuntPlace", {}).get("no", "")) if extension_value.get("vpShuntPlace", {}) and extension_value.get("vpShuntPlace", {}).get("no", "") else ""

        VpShunt = str(extension_value.get("VpShunt", "")) if extension_value and extension_value.get("VpShunt", "") else ""

        cronicDizziness_yes = str(extension_value.get("cronicDizziness", {}).get("yes", "")) if extension_value.get("cronicDizziness", {}) and extension_value.get("cronicDizziness", {}).get("yes", "") else ""
        cronicDizziness_no = str(extension_value.get("cronicDizziness", {}).get("no", "")) if extension_value.get("cronicDizziness", {}) and extension_value.get("cronicDizziness", {}).get("no", "") else ""

        lastEpisode3 = str(extension_value.get("lastEpisode3", "")) if extension_value and extension_value.get("lastEpisode3", "") else ""

        onMeds2_yes = str(extension_value.get("onMeds2", {}).get("yes", "")) if extension_value.get("onMeds2", {}) and extension_value.get("onMeds2", {}).get("yes", "") else ""
        onMeds2_no = str(extension_value.get("onMeds2", {}).get("no", "")) if extension_value.get("onMeds2", {}) and extension_value.get("onMeds2", {}).get("no", "") else ""

        comment = str(extension_value.get("comment", "")) if extension_value and extension_value.get("comment", "") else ""

        cardiovascularProblem_yes = str(extension_value.get("cardiovascularProblem", {}).get("yes", "")) if extension_value.get("cardiovascularProblem", {}) and extension_value.get("cardiovascularProblem", {}).get("yes", "") else ""
        cardiovascularProblem_no = str(extension_value.get("cardiovascularProblem", {}).get("no", "")) if extension_value.get("cardiovascularProblem", {}) and extension_value.get("cardiovascularProblem", {}).get("no", "") else ""

        hypertension_yes = str(extension_value.get("hypertension", {}).get("yes", "")) if extension_value.get("hypertension", {}) and extension_value.get("hypertension", {}).get("yes", "") else ""
        hypertension_no = str(extension_value.get("hypertension", {}).get("no", "")) if extension_value.get("hypertension", {}) and extension_value.get("hypertension", {}).get("no", "") else ""

        mi_yes = str(extension_value.get("mi", {}).get("yes", "")) if extension_value.get("mi", {}) and extension_value.get("mi", {}).get("yes", "") else ""
        mi_no = str(extension_value.get("mi", {}).get("no", "")) if extension_value.get("mi", {}) and extension_value.get("mi", {}).get("no", "") else ""

        dateOfEvent = str(extension_value.get("dateOfEvent", "")) if extension_value and extension_value.get("dateOfEvent", "") else ""

        angina_yes = str(extension_value.get("angina", {}).get("yes", "")) if extension_value.get("angina", {}) and extension_value.get("angina", {}).get("yes", "") else ""
        angina_no = str(extension_value.get("angina", {}).get("no", "")) if extension_value.get("angina", {}) and extension_value.get("angina", {}).get("no", "") else ""

        lastEpisode4 = str(extension_value.get("lastEpisode4", "")) if extension_value and extension_value.get("lastEpisode4", "") else ""

        chestPain_yes = str(extension_value.get("chestPain", {}).get("yes", "")) if extension_value.get("chestPain", {}) and extension_value.get("chestPain", {}).get("yes", "") else ""
        chestPain_no = str(extension_value.get("chestPain", {}).get("no", "")) if extension_value.get("chestPain", {}) and extension_value.get("chestPain", {}).get("no", "") else ""

        describe_ = str(extension_value.get("describe", "")) if extension_value and extension_value.get("describe", "") else ""

        arterioscleroticHeartDisease_yes = str(extension_value.get("arterioscleroticHeartDisease", {}).get("yes", "")) if extension_value.get("arterioscleroticHeartDisease", {}) and extension_value.get("arterioscleroticHeartDisease", {}).get("yes", "") else ""
        arterioscleroticHeartDisease_no = str(extension_value.get("arterioscleroticHeartDisease", {}).get("no", "")) if extension_value.get("arterioscleroticHeartDisease", {}) and extension_value.get("arterioscleroticHeartDisease", {}).get("no", "") else ""

        onset = str(extension_value.get("onset", "")) if extension_value and extension_value.get("onset", "") else ""

        congestiveHeartFailure_yes = str(extension_value.get("congestiveHeartFailure", {}).get("yes", "")) if extension_value.get("congestiveHeartFailure", {}) and extension_value.get("congestiveHeartFailure", {}).get("yes", "") else ""
        congestiveHeartFailure_no = str(extension_value.get("congestiveHeartFailure", {}).get("no", "")) if extension_value.get("congestiveHeartFailure", {}) and extension_value.get("congestiveHeartFailure", {}).get("no", "") else ""

        onset1 = str(extension_value.get("onset1", "")) if extension_value and extension_value.get("onset1", "") else ""

        swellingOfAnklesFeet_yes = str(extension_value.get("swellingOfAnklesFeet", {}).get("yes", "")) if extension_value.get("swellingOfAnklesFeet", {}) and extension_value.get("swellingOfAnklesFeet", {}).get("yes", "") else ""
        swellingOfAnklesFeet_no = str(extension_value.get("swellingOfAnklesFeet", {}).get("no", "")) if extension_value.get("swellingOfAnklesFeet", {}) and extension_value.get("swellingOfAnklesFeet", {}).get("no", "") else ""

        pitting_yes = str(extension_value.get("pitting", {}).get("yes", "")) if extension_value.get("pitting", {}) and extension_value.get("pitting", {}).get("yes", "") else ""
        pitting_no = str(extension_value.get("pitting", {}).get("no", "")) if extension_value.get("pitting", {}) and extension_value.get("pitting", {}).get("no", "") else ""

        arrhythmia_yes = str(extension_value.get("arrhythmia", {}).get("yes", "")) if extension_value.get("arrhythmia", {}) and extension_value.get("arrhythmia", {}).get("yes", "") else ""
        arrhythmia_no = str(extension_value.get("arrhythmia", {}).get("no", "")) if extension_value.get("arrhythmia", {}) and extension_value.get("arrhythmia", {}).get("no", "") else ""

        typeArrhythmia = str(extension_value.get("typeArrhythmia", "")) if extension_value and extension_value.get("typeArrhythmia", "") else ""

        murmurs_yes = str(extension_value.get("murmurs", {}).get("yes", "")) if extension_value.get("murmurs", {}) and extension_value.get("murmurs", {}).get("yes", "") else ""
        murmurs_no = str(extension_value.get("murmurs", {}).get("no", "")) if extension_value.get("murmurs", {}) and extension_value.get("murmurs", {}).get("no", "") else ""

        peripheralVascularDisease_yes = str(extension_value.get("peripheralVascularDisease", {}).get("yes", "")) if extension_value.get("peripheralVascularDisease", {}) and extension_value.get("peripheralVascularDisease", {}).get("yes", "") else ""
        peripheralVascularDisease_no = str(extension_value.get("peripheralVascularDisease", {}).get("no", "")) if extension_value.get("peripheralVascularDisease", {}) and extension_value.get("peripheralVascularDisease", {}).get("no", "") else ""

        otherPeripheral = str(extension_value.get("otherPeripheral", "")) if extension_value and extension_value.get("otherPeripheral", "") else ""

        pacemaker_yes = str(extension_value.get("pacemaker", {}).get("yes", "")) if extension_value.get("pacemaker", {}) and extension_value.get("pacemaker", {}).get("yes", "") else ""
        pacemaker_no = str(extension_value.get("pacemaker", {}).get("no", "")) if extension_value.get("pacemaker", {}) and extension_value.get("pacemaker", {}).get("no", "") else ""

        pacemakerDate = str(extension_value.get("pacemakerDate", "")) if extension_value and extension_value.get("pacemakerDate", "") else ""

        aicd_yes = str(extension_value.get("aicd", {}).get("yes", "")) if extension_value.get("aicd", {}) and extension_value.get("aicd", {}).get("yes", "") else ""
        aicd_no = str(extension_value.get("aicd", {}).get("no", "")) if extension_value.get("aicd", {}) and extension_value.get("aicd", {}).get("no", "") else ""

        aicdDate = str(extension_value.get("aicdDate", "")) if extension_value and extension_value.get("aicdDate", "") else ""

        cardiackStentBypass_yes = str(extension_value.get("cardiackStentBypass", {}).get("yes", "")) if extension_value.get("cardiackStentBypass", {}) and extension_value.get("cardiackStentBypass", {}).get("yes", "") else ""
        cardiackStentBypass_no = str(extension_value.get("cardiackStentBypass", {}).get("no", "")) if extension_value.get("cardiackStentBypass", {}) and extension_value.get("cardiackStentBypass", {}).get("no", "") else ""

        cardiack = str(extension_value.get("cardiackDate", "")) if extension_value and extension_value.get("cardiackDate", "") else ""

        pastBloodTransfusions_yes = str(extension_value.get("pastBloodTransfusions", {}).get("yes", "")) if extension_value.get("pastBloodTransfusions", {}) and extension_value.get("pastBloodTransfusions", {}).get("yes", "") else ""
        pastBloodTransfusions_no = str(extension_value.get("pastBloodTransfusions", {}).get("no", "")) if extension_value.get("pastBloodTransfusions", {}) and extension_value.get("pastBloodTransfusions", {}).get("no", "") else ""

        transfusionsReson = str(extension_value.get("transfusionsReson", "")) if extension_value and extension_value.get("transfusionsReson", "") else ""
        transfusionsDate = str(extension_value.get("transfusionsDate", "")) if extension_value and extension_value.get("transfusionsDate", "") else ""

        adverseReaction1_yes = str(extension_value.get("adverseReaction1", {}).get("yes", "")) if extension_value.get("adverseReaction1", {}) and extension_value.get("adverseReaction1", {}).get("yes", "") else ""
        adverseReaction1_no = str(extension_value.get("adverseReaction1", {}).get("no", "")) if extension_value.get("adverseReaction1", {}) and extension_value.get("adverseReaction1", {}).get("no", "") else ""

        describe3 = str(extension_value.get("describe3", "")) if extension_value and extension_value.get("describe3", "") else ""

        active_yes = str(extension_value.get("active", {}).get("yes", "")) if extension_value.get("active", {}) and extension_value.get("active", {}).get("yes", "") else ""
        active_no = str(extension_value.get("active", {}).get("no", "")) if extension_value.get("active", {}) and extension_value.get("active", {}).get("no", "") else ""
        active1 = str(extension_value.get("active1", "")) if extension_value and extension_value.get("active1", "") else ""

        withoutStoping_yes = str(extension_value.get("withoutStoping", {}).get("yes", "")) if extension_value.get("withoutStoping", {}) and extension_value.get("withoutStoping", {}).get("yes", "") else ""
        withoutStoping_no = str(extension_value.get("withoutStoping", {}).get("no", "")) if extension_value.get("withoutStoping", {}) and extension_value.get("withoutStoping", {}).get("no", "") else ""

        flights = str(extension_value.get("flights", "")) if extension_value and extension_value.get("flights", "") else ""

        atolerateLyingFlatPeriodOfTime_yes = str(extension_value.get("atolerateLyingFlatPeriodOfTime", {}).get("yes", "")) if extension_value.get("atolerateLyingFlatPeriodOfTime", {}) and extension_value.get("atolerateLyingFlatPeriodOfTime", {}).get("yes", "") else ""
        atolerateLyingFlatPeriodOfTime_no = str(extension_value.get("atolerateLyingFlatPeriodOfTime", {}).get("no", "")) if extension_value.get("atolerateLyingFlatPeriodOfTime", {}) and extension_value.get("atolerateLyingFlatPeriodOfTime", {}).get("no", "") else ""

        tolerate = str(extension_value.get("tolerate", "")) if extension_value and extension_value.get("tolerate", "") else ""

        onMeds3_yes = str(extension_value.get("onMeds3", {}).get("yes", "")) if extension_value.get("onMeds3", {}) and extension_value.get("onMeds3", {}).get("yes", "") else ""
        onMeds3_no = str(extension_value.get("onMeds3", {}).get("no", "")) if extension_value.get("onMeds3", {}) and extension_value.get("onMeds3", {}).get("no", "") else ""
        comment1 = str(extension_value.get("comment1", "")) if extension_value and extension_value.get("comment1", "") else ""

        respiratoryProblem_yes = str(extension_value.get("respiratoryProblem", {}).get("yes", "")) if extension_value.get("respiratoryProblem", {}) and extension_value.get("respiratoryProblem", {}).get("yes", "") else ""
        respiratoryProblem_no = str(extension_value.get("respiratoryProblem", {}).get("no", "")) if extension_value.get("respiratoryProblem", {}) and extension_value.get("respiratoryProblem", {}).get("no", "") else ""

        sleepApnea_yes = str(extension_value.get("sleepApnea", {}).get("yes", "")) if extension_value.get("sleepApnea", {}) and extension_value.get("sleepApnea", {}).get("yes", "") else ""
        sleepApnea_no = str(extension_value.get("sleepApnea", {}).get("no", "")) if extension_value.get("sleepApnea", {}) and extension_value.get("sleepApnea", {}).get("no", "") else ""
        daignosed = str(extension_value.get("daignosed", "")) if extension_value and extension_value.get("daignosed", "") else ""

        sleepStudyDone_yes = str(extension_value.get("sleepStudyDone", {}).get("yes", "")) if extension_value.get("sleepStudyDone", {}) and extension_value.get("sleepStudyDone", {}).get("yes", "") else ""
        sleepStudyDone_no = str(extension_value.get("sleepStudyDone", {}).get("no", "")) if extension_value.get("sleepStudyDone", {}) and extension_value.get("sleepStudyDone", {}).get("no", "") else ""
        sleepStudyDone_uses_cpap = str(extension_value.get("sleepStudyDone", {}).get("uses_cpap", "")) if extension_value.get("sleepStudyDone", {}) and extension_value.get("sleepStudyDone", {}).get("uses_cpap", "") else ""

        asthma_yes = str(extension_value.get("asthma", {}).get("yes", "")) if extension_value.get("asthma", {}) and extension_value.get("asthma", {}).get("yes", "") else ""
        asthma_no = str(extension_value.get("asthma", {}).get("no", "")) if extension_value.get("asthma", {}) and extension_value.get("asthma", {}).get("no", "") else ""

        lastEpisode5 = str(extension_value.get("lastEpisode5", "")) if extension_value and extension_value.get("lastEpisode5", "") else ""

        lastHospitalization = str(extension_value.get("lastHospitalization", "")) if extension_value and extension_value.get("lastHospitalization", "") else ""

        intubation_yes = str(extension_value.get("intubation", {}).get("yes", "")) if extension_value.get("intubation", {}) and extension_value.get("intubation", {}).get("yes", "") else ""
        intubation_no = str(extension_value.get("intubation", {}).get("no", "")) if extension_value.get("intubation", {}) and extension_value.get("intubation", {}).get("no", "") else ""

        intubationYes = str(extension_value.get("intubationYes", "")) if extension_value and extension_value.get("intubationYes", "") else ""

        chronicCoughBronchitis_yes = str(extension_value.get("chronicCoughBronchitis", {}).get("yes", "")) if extension_value.get("chronicCoughBronchitis", {}) and extension_value.get("chronicCoughBronchitis", {}).get("yes", "") else ""
        chronicCoughBronchitis_no = str(extension_value.get("chronicCoughBronchitis", {}).get("no", "")) if extension_value.get("chronicCoughBronchitis", {}) and extension_value.get("chronicCoughBronchitis", {}).get("no", "") else ""

        lastEpisode6 = str(extension_value.get("lastEpisode6", "")) if extension_value and extension_value.get("lastEpisode6", "") else ""
        lastHospitalization1 = str(extension_value.get("lastHospitalization1", "")) if extension_value and extension_value.get("lastHospitalization1", "") else ""

        intubation1_yes = str(extension_value.get("intubation1", {}).get("yes", "")) if extension_value.get("intubation1", {}) and extension_value.get("intubation1", {}).get("yes", "") else ""
        intubation1_no = str(extension_value.get("intubation1", {}).get("no", "")) if extension_value.get("intubation1", {}) and extension_value.get("intubation1", {}).get("no", "") else ""
        intubationYes1 = str(extension_value.get("intubationYes1", "")) if extension_value and extension_value.get("intubationYes1", "") else ""

        emphysema_yes = str(extension_value.get("emphysema", {}).get("yes", "")) if extension_value.get("emphysema", {}) and extension_value.get("emphysema", {}).get("yes", "") else ""
        emphysema_no = str(extension_value.get("emphysema", {}).get("no", "")) if extension_value.get("emphysema", {}) and extension_value.get("emphysema", {}).get("no", "") else ""
        lastEpisode7 = str(extension_value.get("lastEpisode7", "")) if extension_value and extension_value.get("lastEpisode7", "") else ""
        lastHospitalization2 = str(extension_value.get("lastHospitalization2", "")) if extension_value and extension_value.get("lastHospitalization2", "") else ""
        intubation2_yes = str(extension_value.get("intubation2", {}).get("yes", "")) if extension_value.get("intubation2", {}) and extension_value.get("intubation2", {}).get("yes", "") else ""
        intubation2_no = str(extension_value.get("intubation2", {}).get("no", "")) if extension_value.get("intubation2", {}) and extension_value.get("intubation2", {}).get("no", "") else ""
        intubationYes2 = str(extension_value.get("intubationYes2", "")) if extension_value and extension_value.get("intubationYes2", "") else ""

        pneumonia_yes = str(extension_value.get("pneumonia", {}).get("yes", "")) if extension_value.get("pneumonia", {}) and extension_value.get("pneumonia", {}).get("yes", "") else ""
        pneumonia_no = str(extension_value.get("pneumonia", {}).get("no", "")) if extension_value.get("pneumonia", {}) and extension_value.get("pneumonia", {}).get("no", "") else ""
        lastEpisode8 = str(extension_value.get("lastEpisode8", "")) if extension_value and extension_value.get("lastEpisode8", "") else ""
        lastHospitalization3 = str(extension_value.get("lastHospitalization3", "")) if extension_value and extension_value.get("lastHospitalization3", "") else ""
        intubation3_yes = str(extension_value.get("intubation3", {}).get("yes", "")) if extension_value.get("intubation3", {}) and extension_value.get("intubation3", {}).get("yes", "") else ""
        intubation3_no = str(extension_value.get("intubation3", {}).get("no", "")) if extension_value.get("intubation3", {}) and extension_value.get("intubation3", {}).get("no", "") else ""
        intubationYes3 = str(extension_value.get("intubationYes3", "")) if extension_value and extension_value.get("intubationYes3", "") else ""

        tuberculosis_yes = str(extension_value.get("tuberculosis", {}).get("yes", "")) if extension_value.get("tuberculosis", {}) and extension_value.get("tuberculosis", {}).get("yes", "") else ""
        tuberculosis_no = str(extension_value.get("tuberculosis", {}).get("no", "")) if extension_value.get("tuberculosis", {}) and extension_value.get("tuberculosis", {}).get("no", "") else ""
        lastEpisode9 = str(extension_value.get("lastEpisode9", "")) if extension_value and extension_value.get("lastEpisode9", "") else ""
        lastHospitalization4 = str(extension_value.get("lastHospitalization4", "")) if extension_value and extension_value.get("lastHospitalization4", "") else ""
        intubation4_yes = str(extension_value.get("intubation4", {}).get("yes", "")) if extension_value.get("intubation4", {}) and extension_value.get("intubation4", {}).get("yes", "") else ""
        intubation4_no = str(extension_value.get("intubation4", {}).get("no", "")) if extension_value.get("intubation4", {}) and extension_value.get("intubation4", {}).get("no", "") else ""
        intubationYes4 = str(extension_value.get("intubationYes4", "")) if extension_value and extension_value.get("intubationYes4", "") else ""

        recentUpperRespiratoryInfection_yes = str(extension_value.get("tuberculosis", {}).get("yes", "")) if extension_value.get("tuberculosis", {}) and extension_value.get("tuberculosis", {}).get("yes", "") else ""
        recentUpperRespiratoryInfection_no = str(extension_value.get("tuberculosis", {}).get("no", "")) if extension_value.get("tuberculosis", {}) and extension_value.get("tuberculosis", {}).get("no", "") else ""
        lastEpisode01 = str(extension_value.get("lastEpisode01", "")) if extension_value and extension_value.get("lastEpisode01", "") else ""
        lastHospitalization05 = str(extension_value.get("lastHospitalization05", "")) if extension_value and extension_value.get("lastHospitalization05", "") else ""
        intubation5_yes = str(extension_value.get("intubation5", {}).get("yes", "")) if extension_value.get("intubation5", {}) and extension_value.get("intubation5", {}).get("yes", "") else ""
        intubation5_no = str(extension_value.get("intubation5", {}).get("no", "")) if extension_value.get("intubation5", {}) and extension_value.get("intubation5", {}).get("no", "") else ""
        intubationYes5 = str(extension_value.get("intubationYes5", "")) if extension_value and extension_value.get("intubationYes5", "") else ""

        lastEpisodeOther = str(extension_value.get("lastEpisodeOther", "")) if extension_value and extension_value.get("lastEpisodeOther", "") else ""
        lastEpisode02 = str(extension_value.get("lastEpisode02", "")) if extension_value and extension_value.get("lastEpisode02", "") else ""
        lastHospitalization7 = str(extension_value.get("lastHospitalization7", "")) if extension_value and extension_value.get("lastHospitalization7", "") else ""
        intubation7_yes = str(extension_value.get("intubation7", {}).get("yes", "")) if extension_value.get("intubation7", {}) and extension_value.get("intubation7", {}).get("yes", "") else ""
        intubation7_no = str(extension_value.get("intubation7", {}).get("no", "")) if extension_value.get("intubation7", {}) and extension_value.get("intubation7", {}).get("no", "") else ""
        intubationYes7 = str(extension_value.get("intubationYes7", "")) if extension_value and extension_value.get("intubationYes7", "") else ""

        onMeds4_yes = str(extension_value.get("onMeds4", {}).get("yes", "")) if extension_value.get("onMeds4", {}) and extension_value.get("onMeds4", {}).get("yes", "") else ""
        onMeds4_no = str(extension_value.get("onMeds4", {}).get("no", "")) if extension_value.get("onMeds4", {}) and extension_value.get("onMeds4", {}).get("no", "") else ""
        comment2 = str(extension_value.get("comment2", "")) if extension_value and extension_value.get("comment2", "") else ""

        reportedGastroinestinalProblem_yes = str(extension_value.get("reportedGastroinestinalProblem", {}).get("yes", "")) if extension_value.get("reportedGastroinestinalProblem", {}) and extension_value.get("reportedGastroinestinalProblem", {}).get("yes", "") else ""
        reportedGastroinestinalProblem_no = str(extension_value.get("reportedGastroinestinalProblem", {}).get("no", "")) if extension_value.get("reportedGastroinestinalProblem", {}) and extension_value.get("reportedGastroinestinalProblem", {}).get("no", "") else ""
        curentDiet = str(extension_value.get("curentDiet", "")) if extension_value and extension_value.get("curentDiet", "") else ""

        difficultySwallowing_yes = str(extension_value.get("difficultySwallowing", {}).get("yes", "")) if extension_value.get("difficultySwallowing", {}) and extension_value.get("difficultySwallowing", {}).get("yes", "") else ""
        difficultySwallowing_no = str(extension_value.get("difficultySwallowing", {}).get("no", "")) if extension_value.get("difficultySwallowing", {}) and extension_value.get("difficultySwallowing", {}).get("no", "") else ""

        pepticUlcerDisease_yes = str(extension_value.get("pepticUlcerDisease", {}).get("yes", "")) if extension_value.get("pepticUlcerDisease", {}) and extension_value.get("pepticUlcerDisease", {}).get("yes", "") else ""
        pepticUlcerDisease_no = str(extension_value.get("pepticUlcerDisease", {}).get("no", "")) if extension_value.get("pepticUlcerDisease", {}) and extension_value.get("pepticUlcerDisease", {}).get("no", "") else ""

        gerdHeartBurn_yes = str(extension_value.get("gerdHeartBurn", {}).get("yes", "")) if extension_value.get("gerdHeartBurn", {}) and extension_value.get("gerdHeartBurn", {}).get("yes", "") else ""
        gerdHeartBurn_no = str(extension_value.get("gerdHeartBurn", {}).get("no", "")) if extension_value.get("gerdHeartBurn", {}) and extension_value.get("gerdHeartBurn", {}).get("no", "") else ""

        hiatalHernia_yes = str(extension_value.get("hiatalHernia", {}).get("yes", "")) if extension_value.get("hiatalHernia", {}) and extension_value.get("hiatalHernia", {}).get("yes", "") else ""
        hiatalHernia_no = str(extension_value.get("hiatalHernia", {}).get("no", "")) if extension_value.get("hiatalHernia", {}) and extension_value.get("hiatalHernia", {}).get("no", "") else ""

        hepatitis_yes = str(extension_value.get("hepatitis", {}).get("yes", "")) if extension_value.get("hepatitis", {}) and extension_value.get("hepatitis", {}).get("yes", "") else ""
        hepatitis_no = str(extension_value.get("hepatitis", {}).get("no", "")) if extension_value.get("hepatitis", {}) and extension_value.get("hepatitis", {}).get("no", "") else ""
        hepatitisType = str(extension_value.get("hepatitisType", "")) if extension_value and extension_value.get("hepatitisType", "") else ""
        hepatitisDiagnosed = str(extension_value.get("hepatitisDiagnosed", "")) if extension_value and extension_value.get("hepatitisDiagnosed", "") else ""
        hepatitisTreated = str(extension_value.get("hepatitisTreated", "")) if extension_value and extension_value.get("hepatitisTreated", "") else ""

        irritableBowelSyndromeCrohn_yes = str(extension_value.get("irritableBowelSyndromeCrohn", {}).get("yes", "")) if extension_value.get("irritableBowelSyndromeCrohn", {}) and extension_value.get("irritableBowelSyndromeCrohn", {}).get("yes", "") else ""
        irritableBowelSyndromeCrohn_no = str(extension_value.get("irritableBowelSyndromeCrohn", {}).get("no", "")) if extension_value.get("irritableBowelSyndromeCrohn", {}) and extension_value.get("irritableBowelSyndromeCrohn", {}).get("no", "") else ""
        bowelActivity = str(extension_value.get("bowelActivity", "")) if extension_value and extension_value.get("bowelActivity", "") else ""

        unintentionalweightLoss_yes = str(extension_value.get("unintentionalweightLoss", {}).get("yes", "")) if extension_value.get("unintentionalweightLoss", {}) and extension_value.get("unintentionalweightLoss", {}).get("yes", "") else ""
        unintentionalweightLoss_no = str(extension_value.get("unintentionalweightLoss", {}).get("no", "")) if extension_value.get("unintentionalweightLoss", {}) and extension_value.get("unintentionalweightLoss", {}).get("no", "") else ""


        onMeds5_yes = str(extension_value.get("onMeds5", {}).get("yes", "")) if extension_value.get("onMeds5", {}) and extension_value.get("onMeds5", {}).get("yes", "") else ""
        onMeds5_no = str(extension_value.get("onMeds5", {}).get("no", "")) if extension_value.get("onMeds5", {}) and extension_value.get("onMeds5", {}).get("no", "") else ""
        comment3 = str(extension_value.get("comment3", "")) if extension_value and extension_value.get("comment3", "") else ""

        reportRenalProblem_yes = str(extension_value.get("reportRenalProblem", {}).get("yes", "")) if extension_value.get("reportRenalProblem", {}) and extension_value.get("reportRenalProblem", {}).get("yes", "") else ""
        reportRenalProblem_no = str(extension_value.get("reportRenalProblem", {}).get("no", "")) if extension_value.get("reportRenalProblem", {}) and extension_value.get("reportRenalProblem", {}).get("no", "") else ""

        kidneyDisease_yes = str(extension_value.get("kidneyDisease", {}).get("yes", "")) if extension_value.get("kidneyDisease", {}) and extension_value.get("kidneyDisease", {}).get("yes", "") else ""
        kidneyDisease_no = str(extension_value.get("kidneyDisease", {}).get("no", "")) if extension_value.get("kidneyDisease", {}) and extension_value.get("kidneyDisease", {}).get("no", "") else ""

        urinaryInfections_yes = str(extension_value.get("urinaryInfections", {}).get("yes", "")) if extension_value.get("urinaryInfections", {}) and extension_value.get("urinaryInfections", {}).get("yes", "") else ""
        urinaryInfections_no = str(extension_value.get("urinaryInfections", {}).get("no", "")) if extension_value.get("urinaryInfections", {}) and extension_value.get("urinaryInfections", {}).get("no", "") else ""

        enlargedProstate_yes = str(extension_value.get("enlargedProstate", {}).get("yes", "")) if extension_value.get("enlargedProstate", {}) and extension_value.get("enlargedProstate", {}).get("yes", "") else ""
        enlargedProstate_no = str(extension_value.get("enlargedProstate", {}).get("no", "")) if extension_value.get("enlargedProstate", {}) and extension_value.get("enlargedProstate", {}).get("no", "") else ""

        difficultyUrinating_yes = str(extension_value.get("difficultyUrinating", {}).get("yes", "")) if extension_value.get("difficultyUrinating", {}) and extension_value.get("difficultyUrinating", {}).get("yes", "") else ""
        difficultyUrinating_no = str(extension_value.get("difficultyUrinating", {}).get("no", "")) if extension_value.get("difficultyUrinating", {}) and extension_value.get("difficultyUrinating", {}).get("no", "") else ""

        bloodUrine_yes = str(extension_value.get("bloodUrine", {}).get("yes", "")) if extension_value.get("bloodUrine", {}) and extension_value.get("bloodUrine", {}).get("yes", "") else ""
        bloodUrine_no = str(extension_value.get("bloodUrine", {}).get("no", "")) if extension_value.get("bloodUrine", {}) and extension_value.get("bloodUrine", {}).get("no", "") else ""

        onDialysis_yes = str(extension_value.get("onDialysis", {}).get("yes", "")) if extension_value.get("onDialysis", {}) and extension_value.get("onDialysis", {}).get("yes", "") else ""
        onDialysis_no = str(extension_value.get("onDialysis", {}).get("no", "")) if extension_value.get("onDialysis", {}) and extension_value.get("onDialysis", {}).get("no", "") else ""

        dialysisDays = str(extension_value.get("dialysisDays", "")) if extension_value and extension_value.get("dialysisDays", "") else ""
        dialysisType = str(extension_value.get("dialysisType", {}).get("value", "")) if extension_value.get("dialysisType", {}) and extension_value.get("dialysisType", {}).get("value", "") else ""

        shuntFistualAccess_yes = str(extension_value.get("shuntFistualAccess", {}).get("yes", "")) if extension_value.get("shuntFistualAccess", {}) and extension_value.get("shuntFistualAccess", {}).get("yes", "") else ""
        shuntFistualAccess_no = str(extension_value.get("shuntFistualAccess", {}).get("no", "")) if extension_value.get("shuntFistualAccess", {}) and extension_value.get("shuntFistualAccess", {}).get("no", "") else ""


        selfCatherization_yes = str(extension_value.get("selfCatherization", {}).get("yes", "")) if extension_value.get("selfCatherization", {}) and extension_value.get("selfCatherization", {}).get("yes", "") else ""
        selfCatherization_no = str(extension_value.get("selfCatherization", {}).get("no", "")) if extension_value.get("selfCatherization", {}) and extension_value.get("selfCatherization", {}).get("no", "") else ""

        otherCatherization = str(extension_value.get("otherCatherization", "")) if extension_value and extension_value.get("otherCatherization", "") else ""

        onMeds6_yes = str(extension_value.get("onMeds6", {}).get("yes", "")) if extension_value.get("onMeds6", {}) and extension_value.get("onMeds6", {}).get("yes", "") else ""
        onMeds6_no = str(extension_value.get("onMeds6", {}).get("no", "")) if extension_value.get("onMeds6", {}) and extension_value.get("onMeds6", {}).get("no", "") else ""

        comments = str(extension_value.get("comments", "")) if extension_value and extension_value.get("comments", "") else ""

        reportedMusculoskeletalProblem_yes = str(extension_value.get("reportedMusculoskeletalProblem", {}).get("yes", "")) if extension_value.get("reportedMusculoskeletalProblem", {}) and extension_value.get("reportedMusculoskeletalProblem", {}).get("yes", "") else ""
        reportedMusculoskeletalProblem_no = str(extension_value.get("reportedMusculoskeletalProblem", {}).get("no", "")) if extension_value.get("reportedMusculoskeletalProblem", {}) and extension_value.get("reportedMusculoskeletalProblem", {}).get("no", "") else ""

        arthritis_yes = str(extension_value.get("arthritis", {}).get("yes", "")) if extension_value.get("arthritis", {}) and extension_value.get("arthritis", {}).get("yes", "") else ""
        arthritis_no = str(extension_value.get("arthritis", {}).get("no", "")) if extension_value.get("arthritis", {}) and extension_value.get("arthritis", {}).get("no", "") else ""

        gout_yes = str(extension_value.get("gout", {}).get("yes", "")) if extension_value.get("gout", {}) and extension_value.get("gout", {}).get("yes", "") else ""
        gout_no = str(extension_value.get("gout", {}).get("no", "")) if extension_value.get("gout", {}) and extension_value.get("gout", {}).get("no", "") else ""

        osteoperosis_yes = str(extension_value.get("osteoperosis", {}).get("yes", "")) if extension_value.get("osteoperosis", {}) and extension_value.get("osteoperosis", {}).get("yes", "") else ""
        osteoperosis_no = str(extension_value.get("osteoperosis", {}).get("no", "")) if extension_value.get("osteoperosis", {}) and extension_value.get("osteoperosis", {}).get("no", "") else ""

        muscleWeakness_yes = str(extension_value.get("muscleWeakness", {}).get("yes", "")) if extension_value.get("muscleWeakness", {}) and extension_value.get("muscleWeakness", {}).get("yes", "") else ""
        muscleWeakness_no = str(extension_value.get("muscleWeakness", {}).get("no", "")) if extension_value.get("muscleWeakness", {}) and extension_value.get("muscleWeakness", {}).get("no", "") else ""

        fractures = str(extension_value.get("fractures", "")) if extension_value and extension_value.get("fractures", "") else ""
        others = str(extension_value.get("others", "")) if extension_value and extension_value.get("others", "") else ""

        onMeds7_yes = str(extension_value.get("onMeds7", {}).get("yes", "")) if extension_value.get("onMeds7", {}) and extension_value.get("onMeds7", {}).get("yes", "") else ""
        onMeds7_no = str(extension_value.get("onMeds7", {}).get("no", "")) if extension_value.get("onMeds7", {}) and extension_value.get("onMeds7", {}).get("no", "") else ""

        musculoskeletalComment = str(extension_value.get("musculoskeletalComment", "")) if extension_value and extension_value.get("musculoskeletalComment", "") else ""

        reportedHematologyProblem_yes = str(extension_value.get("reportedHematologyProblem", {}).get("yes", "")) if extension_value.get("reportedHematologyProblem", {}) and extension_value.get("reportedHematologyProblem", {}).get("yes", "") else ""
        reportedHematologyProblem_no = str(extension_value.get("reportedHematologyProblem", {}).get("no", "")) if extension_value.get("reportedHematologyProblem", {}) and extension_value.get("reportedHematologyProblem", {}).get("no", "") else ""

        bloodTransfusions_yes = str(extension_value.get("bloodTransfusions", {}).get("yes", "")) if extension_value.get("bloodTransfusions", {}) and extension_value.get("bloodTransfusions", {}).get("yes", "") else ""
        bloodTransfusions_no = str(extension_value.get("bloodTransfusions", {}).get("no", "")) if extension_value.get("bloodTransfusions", {}) and extension_value.get("bloodTransfusions", {}).get("no", "") else ""

        bloodTransfusionsReason = str(extension_value.get("bloodTransfusionsReason", "")) if extension_value and extension_value.get("bloodTransfusionsReason", "") else ""
        bloodTransfusionsDate = str(extension_value.get("bloodTransfusionsDate", "")) if extension_value and extension_value.get("bloodTransfusionsDate", "") else ""

        adverseReactions_yes = str(extension_value.get("adverseReactions", {}).get("yes", "")) if extension_value.get("adverseReactions", {}) and extension_value.get("adverseReactions", {}).get("yes", "") else ""
        adverseReactions_no = str(extension_value.get("adverseReactions", {}).get("no", "")) if extension_value.get("adverseReactions", {}) and extension_value.get("adverseReactions", {}).get("no", "") else ""

        adverseReactionsDescribe = str(extension_value.get("adverseReactionsDescribe", "")) if extension_value and extension_value.get("adverseReactionsDescribe", "") else ""

        bleedingProblem_yes = str(extension_value.get("bleedingProblem", {}).get("yes", "")) if extension_value.get("bleedingProblem", {}) and extension_value.get("bleedingProblem", {}).get("yes", "") else ""
        bleedingProblem_no = str(extension_value.get("bleedingProblem", {}).get("no", "")) if extension_value.get("bleedingProblem", {}) and extension_value.get("bleedingProblem", {}).get("no", "") else ""

        bruiseEasily_yes = str(extension_value.get("bruiseEasily", {}).get("yes", "")) if extension_value.get("bruiseEasily", {}) and extension_value.get("bruiseEasily", {}).get("yes", "") else ""
        bruiseEasily_no = str(extension_value.get("bruiseEasily", {}).get("no", "")) if extension_value.get("bruiseEasily", {}) and extension_value.get("bruiseEasily", {}).get("no", "") else ""

        sickleCellDisease_yes = str(extension_value.get("sickleCellDisease", {}).get("yes", "")) if extension_value.get("sickleCellDisease", {}) and extension_value.get("sickleCellDisease", {}).get("yes", "") else ""
        sickleCellDisease_no = str(extension_value.get("sickleCellDisease", {}).get("no", "")) if extension_value.get("sickleCellDisease", {}) and extension_value.get("sickleCellDisease", {}).get("no", "") else ""

        hivAIDS_yes = str(extension_value.get("hivAIDS", {}).get("yes", "")) if extension_value.get("hivAIDS", {}) and extension_value.get("hivAIDS", {}).get("yes", "") else ""
        hivAIDS_no = str(extension_value.get("hivAIDS", {}).get("no", "")) if extension_value.get("hivAIDS", {}) and extension_value.get("hivAIDS", {}).get("no", "") else ""

        leukemia_yes = str(extension_value.get("leukemia", {}).get("yes", "")) if extension_value.get("leukemia", {}) and extension_value.get("leukemia", {}).get("yes", "") else ""
        leukemia_no = str(extension_value.get("leukemia", {}).get("no", "")) if extension_value.get("leukemia", {}) and extension_value.get("leukemia", {}).get("no", "") else ""

        leukemiaYes = str(extension_value.get("leukemiaYes", "")) if extension_value and extension_value.get("leukemiaYes", "") else ""

        treatmentType_chemotherapy = str(extension_value.get("treatmentType", {}).get("_chemotherapy", "")) if extension_value.get("treatmentType", {}) and extension_value.get("treatmentType", {}).get("_chemotherapy", "") else ""
        treatmentType_bone_marrow_transplant = str(extension_value.get("treatmentType", {}).get("bone_marrow_transplant", "")) if extension_value.get("treatmentType", {}) and extension_value.get("treatmentType", {}).get("bone_marrow_transplant", "") else ""

        cancer_yes = str(extension_value.get("cancer", {}).get("yes", "")) if extension_value.get("cancer", {}) and extension_value.get("cancer", {}).get("yes", "") else ""
        cancer_no = str(extension_value.get("cancer", {}).get("no", "")) if extension_value.get("cancer", {}) and extension_value.get("cancer", {}).get("no", "") else ""

        treatmentTypeCancer__chemotherapy = str(extension_value.get("treatmentTypeCancer", {}).get("_chemotherapy", "")) if extension_value.get("treatmentTypeCancer", {}) and extension_value.get("treatmentTypeCancer", {}).get("_chemotherapy", "") else ""
        treatmentTypeCancer_radiation_therapy = str(extension_value.get("treatmentTypeCancer", {}).get("radiation_therapy", "")) if extension_value.get("treatmentTypeCancer", {}) and extension_value.get("treatmentTypeCancer", {}).get("radiation_therapy", "") else ""

        onMedsHematology_yes = str(extension_value.get("onMedsHematology", {}).get("yes", "")) if extension_value.get("onMedsHematology", {}) and extension_value.get("onMedsHematology", {}).get("yes", "") else ""
        onMedsHematology_no = str(extension_value.get("onMedsHematology", {}).get("no", "")) if extension_value.get("onMedsHematology", {}) and extension_value.get("onMedsHematology", {}).get("no", "") else ""
        hematologyComment = str(extension_value.get("hematologyComment", "")) if extension_value and extension_value.get("hematologyComment", "") else ""

        endocrineProblem_yes = str(extension_value.get("endocrineProblem", {}).get("yes", "")) if extension_value.get("endocrineProblem", {}) and extension_value.get("endocrineProblem", {}).get("yes", "") else ""
        endocrineProblem_no = str(extension_value.get("endocrineProblem", {}).get("no", "")) if extension_value.get("endocrineProblem", {}) and extension_value.get("endocrineProblem", {}).get("no", "") else ""

        diabetes_yes = str(extension_value.get("diabetes", {}).get("yes", "")) if extension_value.get("diabetes", {}) and extension_value.get("diabetes", {}).get("yes", "") else ""
        diabetes_no = str(extension_value.get("diabetes", {}).get("no", "")) if extension_value.get("diabetes", {}) and extension_value.get("diabetes", {}).get("no", "") else ""

        type_type_i = str(extension_value.get("type", {}).get("type_i", "")) if extension_value.get("type", {}) and extension_value.get("type", {}).get("type_i", "") else ""
        type_type_ii = str(extension_value.get("type", {}).get("type_ii", "")) if extension_value.get("type", {}) and extension_value.get("type", {}).get("type_ii", "") else ""

        diabetesDate = str(extension_value.get("diabetesDate", "")) if extension_value and extension_value.get("diabetesDate", "") else ""

        BloodGlucose_yes = str(extension_value.get("BloodGlucose", {}).get("yes", "")) if extension_value.get("BloodGlucose", {}) and extension_value.get("BloodGlucose", {}).get("yes", "") else ""
        BloodGlucose_no = str(extension_value.get("BloodGlucose", {}).get("no", "")) if extension_value.get("BloodGlucose", {}) and extension_value.get("BloodGlucose", {}).get("no", "") else ""

        bloodGlucoseMonitoring = str(extension_value.get("bloodGlucoseMonitoring", "")) if extension_value and extension_value.get("bloodGlucoseMonitoring", "") else ""
        lowRnge = str(extension_value.get("lowRnge", "")) if extension_value and extension_value.get("lowRnge", "") else ""
        highRange = str(extension_value.get("highRange", "")) if extension_value and extension_value.get("highRange", "") else ""
        hospitalizationDiabetes = str(extension_value.get("hospitalizationDiabetes", "")) if extension_value and extension_value.get("hospitalizationDiabetes", "") else ""
        reason = str(extension_value.get("reason", "")) if extension_value and extension_value.get("reason", "") else ""

        thyroidDisease_yes = str(extension_value.get("thyroidDisease", {}).get("yes", "")) if extension_value.get("thyroidDisease", {}) and extension_value.get("thyroidDisease", {}).get("yes", "") else ""
        thyroidDisease_no = str(extension_value.get("thyroidDisease", {}).get("no", "")) if extension_value.get("thyroidDisease", {}) and extension_value.get("thyroidDisease", {}).get("no", "") else ""

        HistorySteroidUse_yes = str(extension_value.get("HistorySteroidUse", {}).get("yes", "")) if extension_value.get("HistorySteroidUse", {}) and extension_value.get("HistorySteroidUse", {}).get("yes", "") else ""
        HistorySteroidUse_no = str(extension_value.get("HistorySteroidUse", {}).get("no", "")) if extension_value.get("HistorySteroidUse", {}) and extension_value.get("HistorySteroidUse", {}).get("no", "") else ""
        steroidHistory = str(extension_value.get("steroidHistory", "")) if extension_value and extension_value.get("steroidHistory", "") else ""

        onMeds8_yes = str(extension_value.get("onMeds8", {}).get("yes", "")) if extension_value.get("onMeds8", {}) and extension_value.get("onMeds8", {}).get("yes", "") else ""
        onMeds8_no = str(extension_value.get("onMeds8", {}).get("no", "")) if extension_value.get("onMeds8", {}) and extension_value.get("onMeds8", {}).get("no", "") else ""
        EndocrineComment = str(extension_value.get("EndocrineComment", "")) if extension_value and extension_value.get("EndocrineComment", "") else ""

        reportedSkinProblem_yes = str(extension_value.get("reportedSkinProblem", {}).get("yes", "")) if extension_value.get("reportedSkinProblem", {}) and extension_value.get("reportedSkinProblem", {}).get("yes", "") else ""
        reportedSkinProblem_no = str(extension_value.get("reportedSkinProblem", {}).get("no", "")) if extension_value.get("reportedSkinProblem", {}) and extension_value.get("reportedSkinProblem", {}).get("no", "") else ""

        intact_yes = str(extension_value.get("intact", {}).get("yes", "")) if extension_value.get("intact", {}) and extension_value.get("intact", {}).get("yes", "") else ""
        intact_no = str(extension_value.get("intact", {}).get("no", "")) if extension_value.get("intact", {}) and extension_value.get("intact", {}).get("no", "") else ""

        psoriasis_yes = str(extension_value.get("psoriasis", {}).get("yes", "")) if extension_value.get("psoriasis", {}) and extension_value.get("psoriasis", {}).get("yes", "") else ""
        psoriasis_no = str(extension_value.get("psoriasis", {}).get("no", "")) if extension_value.get("psoriasis", {}) and extension_value.get("psoriasis", {}).get("no", "") else ""

        eczdma_yes = str(extension_value.get("eczdma", {}).get("yes", "")) if extension_value.get("eczdma", {}) and extension_value.get("eczdma", {}).get("yes", "") else ""
        eczdma_no = str(extension_value.get("eczdma", {}).get("no", "")) if extension_value.get("eczdma", {}) and extension_value.get("eczdma", {}).get("no", "") else ""

        brusesCuts_yes = str(extension_value.get("brusesCuts", {}).get("yes", "")) if extension_value.get("brusesCuts", {}) and extension_value.get("brusesCuts", {}).get("yes", "") else ""
        brusesCuts_no = str(extension_value.get("brusesCuts", {}).get("no", "")) if extension_value.get("brusesCuts", {}) and extension_value.get("brusesCuts", {}).get("no", "") else ""

        rashesDescribe = str(extension_value.get("rashesDescribe", "")) if extension_value and extension_value.get("rashesDescribe", "") else ""
        rashesDescribeOther = str(extension_value.get("rashesDescribeOther", "")) if extension_value and extension_value.get("rashesDescribeOther", "") else ""
        skinComment = str(extension_value.get("skinComment", "")) if extension_value and extension_value.get("skinComment", "") else ""

        visualImpairmentGyn_yes = str(extension_value.get("visualImpairmentGyn", {}).get("yes", "")) if extension_value.get("visualImpairmentGyn", {}) and extension_value.get("visualImpairmentGyn", {}).get("yes", "") else ""
        visualImpairmentGyn_no = str(extension_value.get("visualImpairmentGyn", {}).get("no", "")) if extension_value.get("visualImpairmentGyn", {}) and extension_value.get("visualImpairmentGyn", {}).get("no", "") else ""
        visualImpairmentGyn_na = str(extension_value.get("visualImpairmentGyn", {}).get("n/a", "")) if extension_value.get("visualImpairmentGyn", {}) and extension_value.get("visualImpairmentGyn", {}).get("n/a", "") else ""
        fynComment = str(extension_value.get("fynComment", "")) if extension_value and extension_value.get("fynComment", "") else ""
        menstrualPeriod = str(extension_value.get("menstrualPeriod", "")) if extension_value and extension_value.get("menstrualPeriod", "") else ""
        SapSmear = str(extension_value.get("SapSmear", "")) if extension_value and extension_value.get("SapSmear", "") else ""
        dateOfLastPelvicExam = str(extension_value.get("dateOfLastPelvicExam", "")) if extension_value and extension_value.get("dateOfLastPelvicExam", "") else ""

        pregnantNow_yes = str(extension_value.get("pregnantNow", {}).get("yes", "")) if extension_value.get("pregnantNow", {}) and extension_value.get("pregnantNow", {}).get("yes", "") else ""
        pregnantNow_no = str(extension_value.get("pregnantNow", {}).get("no", "")) if extension_value.get("pregnantNow", {}) and extension_value.get("pregnantNow", {}).get("no", "") else ""

        possibilityPregnantDescribe = str(extension_value.get("possibilityPregnantDescribe", "")) if extension_value and extension_value.get("possibilityPregnantDescribe", "") else ""
        lactating_yes = str(extension_value.get("lactating", {}).get("yes", "")) if extension_value.get("lactating", {}) and extension_value.get("lactating", {}).get("yes", "") else ""
        lactating_no = str(extension_value.get("lactating", {}).get("no", "")) if extension_value.get("lactating", {}) and extension_value.get("lactating", {}).get("no", "") else ""

        peds_na = str(extension_value.get("peds", {}).get("n/a", "")) if extension_value.get("peds", {}) and extension_value.get("peds", {}).get("n/a", "") else ""
        deliveryType = str(extension_value.get("deliveryType", {}).get("value", "")) if extension_value.get("deliveryType", {}) and extension_value.get("deliveryType", {}).get("value", "") else ""

        preemie = str(extension_value.get("preemie", "")) if extension_value and extension_value.get("preemie", "") else ""
        birthWeight = str(extension_value.get("birthWeight", "")) if extension_value and extension_value.get("birthWeight", "") else ""

        immunizationDate_yes = str(extension_value.get("immunizationDate", {}).get("yes", "")) if extension_value.get("immunizationDate", {}) and extension_value.get("immunizationDate", {}).get("yes", "") else ""
        immunizationDate_no = str(extension_value.get("immunizationDate", {}).get("no", "")) if extension_value.get("immunizationDate", {}) and extension_value.get("immunizationDate", {}).get("no", "") else ""
        immunizationDetails = str(extension_value.get("immunizationDetails", "")) if extension_value and extension_value.get("immunizationDetails", "") else ""

        respiratoryInfection_yes = str(extension_value.get("respiratoryInfection", {}).get("yes", "")) if extension_value.get("respiratoryInfection", {}) and extension_value.get("respiratoryInfection", {}).get("yes", "") else ""
        respiratoryInfection_no = str(extension_value.get("respiratoryInfection", {}).get("no", "")) if extension_value.get("respiratoryInfection", {}) and extension_value.get("respiratoryInfection", {}).get("no", "") else ""
        respiratoryInfectionDetails = str(extension_value.get("respiratoryInfectionDetails", "")) if extension_value and extension_value.get("respiratoryInfectionDetails", "") else ""

        communicableDisease_measles = str(extension_value.get("communicableDisease", {}).get("measles", "")) if extension_value.get("communicableDisease", {}) and extension_value.get("communicableDisease", {}).get("measles", "") else ""
        communicableDisease_chicken_pox = str(extension_value.get("communicableDisease", {}).get("chicken_pox", "")) if extension_value.get("communicableDisease", {}) and extension_value.get("communicableDisease", {}).get("chicken_pox", "") else ""
        communicableDisease_tb = str(extension_value.get("communicableDisease", {}).get("tb", "")) if extension_value.get("communicableDisease", {}) and extension_value.get("communicableDisease", {}).get("tb", "") else ""
        communicableDisease_meningitis = str(extension_value.get("communicableDisease", {}).get("meningitis", "")) if extension_value.get("communicableDisease", {}) and extension_value.get("communicableDisease", {}).get("meningitis", "") else ""
        communicableDisease_na = str(extension_value.get("communicableDisease", {}).get("na", "")) if extension_value.get("communicableDisease", {}) and extension_value.get("communicableDisease", {}).get("na", "") else ""
        communicableDiseaseOthers = str(extension_value.get("communicableDiseaseOthers", "")) if extension_value and extension_value.get("communicableDiseaseOthers", "") else ""
        pedsComment = str(extension_value.get("pedsComment", "")) if extension_value and extension_value.get("pedsComment", "") else ""

        culturalSurgery_yes = str(extension_value.get("culturalSurgery", {}).get("yes", "")) if extension_value.get("culturalSurgery", {}) and extension_value.get("culturalSurgery", {}).get("yes", "") else ""
        culturalSurgery_no = str(extension_value.get("culturalSurgery", {}).get("no", "")) if extension_value.get("culturalSurgery", {}) and extension_value.get("culturalSurgery", {}).get("no", "") else ""
        dayOfSurgery = str(extension_value.get("dayOfSurgery", "")) if extension_value and extension_value.get("dayOfSurgery", "") else ""
        livesAlone_yes = str(extension_value.get("livesAlone", {}).get("yes", "")) if extension_value.get("livesAlone", {}) and extension_value.get("livesAlone", {}).get("yes", "") else ""
        livesAlone_no = str(extension_value.get("livesAlone", {}).get("no", "")) if extension_value.get("livesAlone", {}) and extension_value.get("livesAlone", {}).get("no", "") else ""
        typeOfHouse = str(extension_value.get("typeOfHouse", {}).get("value", "")) if extension_value.get("typeOfHouse", {}) and extension_value.get("typeOfHouse", {}).get("value", "") else ""
        stairs = str(extension_value.get("stairs", "")) if extension_value and extension_value.get("stairs", "") else ""

        alcohol_yes = str(extension_value.get("alcohol", {}).get("yes", "")) if extension_value.get("alcohol", {}) and extension_value.get("alcohol", {}).get("yes", "") else ""
        alcohol_no = str(extension_value.get("alcohol", {}).get("no", "")) if extension_value.get("alcohol", {}) and extension_value.get("alcohol", {}).get("no", "") else ""
        freq = str(extension_value.get("freq", {}).get("value", "")) if extension_value.get("freq", {}) and extension_value.get("freq", {}).get("value", "") else ""

        alcoholOther = str(extension_value.get("alcoholOther", "")) if extension_value and extension_value.get("alcoholOther", "") else ""
        alcoholAmount = str(extension_value.get("alcoholAmount", "")) if extension_value and extension_value.get("alcoholAmount", "") else ""

        drugs_yes = str(extension_value.get("drugs", {}).get("yes", "")) if extension_value.get("drugs", {}) and extension_value.get("drugs", {}).get("yes", "") else ""
        drugs_no = str(extension_value.get("drugs", {}).get("no", "")) if extension_value.get("drugs", {}) and extension_value.get("drugs", {}).get("no", "") else ""
        drugsFreq = str(extension_value.get("drugsFreq", {}).get("value", "")) if extension_value.get("drugsFreq", {}) and extension_value.get("drugsFreq", {}).get("value", "") else ""

        drugsOther = str(extension_value.get("drugsOther", "")) if extension_value and extension_value.get("drugsOther", "") else ""
        drugslAmount = str(extension_value.get("drugslAmount", "")) if extension_value and extension_value.get("drugslAmount", "") else ""
        lastDoseAmount = str(extension_value.get("lastDoseAmount", "")) if extension_value and extension_value.get("lastDoseAmount", "") else ""

        caffeine_yes = str(extension_value.get("caffeine", {}).get("yes", "")) if extension_value.get("caffeine", {}) and extension_value.get("caffeine", {}).get("yes", "") else ""
        caffeine_no = str(extension_value.get("caffeine", {}).get("no", "")) if extension_value.get("caffeine", {}) and extension_value.get("caffeine", {}).get("no", "") else ""
        caffeineFreq = str(extension_value.get("caffeineFreq", {}).get("value", "")) if extension_value.get("caffeineFreq", {}) and extension_value.get("caffeineFreq", {}).get("value", "") else ""

        caffeineOther = str(extension_value.get("caffeineOther", "")) if extension_value and extension_value.get("caffeineOther", "") else ""
        caffeineAmount = str(extension_value.get("caffeineAmount", "")) if extension_value and extension_value.get("caffeineAmount", "") else ""

        cigarettesVaping_yes = str(extension_value.get("cigarettesVaping", {}).get("yes", "")) if extension_value.get("cigarettesVaping", {}) and extension_value.get("cigarettesVaping", {}).get("yes", "") else ""
        cigarettesVaping_no = str(extension_value.get("cigarettesVaping", {}).get("no", "")) if extension_value.get("cigarettesVaping", {}) and extension_value.get("cigarettesVaping", {}).get("no", "") else ""

        packsPerDay = str(extension_value.get("packsPerDay", "")) if extension_value and extension_value.get("packsPerDay", "") else ""
        lastCigarette = str(extension_value.get("lastCigarette", "")) if extension_value and extension_value.get("lastCigarette", "") else ""

        cigars_yes = str(extension_value.get("cigars", {}).get("yes", "")) if extension_value.get("cigars", {}) and extension_value.get("cigars", {}).get("yes", "") else ""
        cigars_no = str(extension_value.get("cigars", {}).get("no", "")) if extension_value.get("cigars", {}) and extension_value.get("cigars", {}).get("no", "") else ""

        cigarsPerDay = str(extension_value.get("cigarsPerDay", "")) if extension_value and extension_value.get("cigarsPerDay", "") else ""
        lastCigar = str(extension_value.get("lastCigar", "")) if extension_value and extension_value.get("lastCigar", "") else ""

        readinessToQuit = str(extension_value.get("readinessToQuit", {}).get("value", "")) if extension_value.get("readinessToQuit", {}) and extension_value.get("readinessToQuit", {}).get("value", "") else ""
        psychosocialComment = str(extension_value.get("psychosocialComment", "")) if extension_value and extension_value.get("psychosocialComment", "") else ""

        reportedPsychiatricProblem_yes = str(extension_value.get("reportedPsychiatricProblem", {}).get("yes", "")) if extension_value.get("reportedPsychiatricProblem", {}) and extension_value.get("reportedPsychiatricProblem", {}).get("yes", "") else ""
        reportedPsychiatricProblem_no = str(extension_value.get("reportedPsychiatricProblem", {}).get("no", "")) if extension_value.get("reportedPsychiatricProblem", {}) and extension_value.get("reportedPsychiatricProblem", {}).get("no", "") else ""

        anxiety_yes = str(extension_value.get("anxiety", {}).get("yes", "")) if extension_value.get("anxiety", {}) and extension_value.get("anxiety", {}).get("yes", "") else ""
        anxiety_no = str(extension_value.get("anxiety", {}).get("no", "")) if extension_value.get("anxiety", {}) and extension_value.get("anxiety", {}).get("no", "") else ""

        hallucinations_yes = str(extension_value.get("hallucinations", {}).get("yes", "")) if extension_value.get("hallucinations", {}) and extension_value.get("hallucinations", {}).get("yes", "") else ""
        hallucinations_no = str(extension_value.get("hallucinations", {}).get("no", "")) if extension_value.get("hallucinations", {}) and extension_value.get("hallucinations", {}).get("no", "") else ""

        depressions_yes = str(extension_value.get("depressions", {}).get("yes", "")) if extension_value.get("depressions", {}) and extension_value.get("depressions", {}).get("yes", "") else ""
        depressions_no = str(extension_value.get("depressions", {}).get("no", "")) if extension_value.get("depressions", {}) and extension_value.get("depressions", {}).get("no", "") else ""

        postTraumaticReaction_yes = str(extension_value.get("postTraumaticReaction", {}).get("yes", "")) if extension_value.get("postTraumaticReaction", {}) and extension_value.get("postTraumaticReaction", {}).get("yes", "") else ""
        postTraumaticReaction_no = str(extension_value.get("postTraumaticReaction", {}).get("no", "")) if extension_value.get("postTraumaticReaction", {}) and extension_value.get("postTraumaticReaction", {}).get("no", "") else ""

        suicidalIdeations_yes = str(extension_value.get("suicidalIdeations", {}).get("yes", "")) if extension_value.get("suicidalIdeations", {}) and extension_value.get("suicidalIdeations", {}).get("yes", "") else ""
        suicidalIdeations_no = str(extension_value.get("suicidalIdeations", {}).get("no", "")) if extension_value.get("suicidalIdeations", {}) and extension_value.get("suicidalIdeations", {}).get("no", "") else ""
        suicidalOther = str(extension_value.get("suicidalOther", "")) if extension_value and extension_value.get("suicidalOther", "") else ""

        onMeds9_yes = str(extension_value.get("onMeds9", {}).get("yes", "")) if extension_value.get("onMeds9", {}) and extension_value.get("onMeds9", {}).get("yes", "") else ""
        onMeds9_no = str(extension_value.get("onMeds9", {}).get("no", "")) if extension_value.get("onMeds9", {}) and extension_value.get("onMeds9", {}).get("no", "") else ""

        followedByMD_yes = str(extension_value.get("followedByMD", {}).get("yes", "")) if extension_value.get("followedByMD", {}) and extension_value.get("followedByMD", {}).get("yes", "") else ""
        followedByMD_no = str(extension_value.get("followedByMD", {}).get("no", "")) if extension_value.get("followedByMD", {}) and extension_value.get("followedByMD", {}).get("no", "") else ""
        psychiatricComment = str(extension_value.get("psychiatricComment", "")) if extension_value and extension_value.get("psychiatricComment", "") else ""

        afraidOfAnyyone_yes = str(extension_value.get("afraidOfAnyyone", {}).get("yes", "")) if extension_value.get("afraidOfAnyyone", {}) and extension_value.get("afraidOfAnyyone", {}).get("yes", "") else ""
        afraidOfAnyyone_no = str(extension_value.get("afraidOfAnyyone", {}).get("no", "")) if extension_value.get("afraidOfAnyyone", {}) and extension_value.get("afraidOfAnyyone", {}).get("no", "") else ""
        afraidOfAnyyone_unable_to_access = str(extension_value.get("afraidOfAnyyone", {}).get("unable_to_access", "")) if extension_value.get("afraidOfAnyyone", {}) and extension_value.get("afraidOfAnyyone", {}).get("unable_to_access", "") else ""

        afraid = str(extension_value.get("afraid", "")) if extension_value and extension_value.get("afraid", "") else ""
        unableAfraid = str(extension_value.get("unableAfraid", "")) if extension_value and extension_value.get("unableAfraid", "") else ""

        isAnyoneHurtingYou_yes = str(extension_value.get("isAnyoneHurtingYou", {}).get("yes", "")) if extension_value.get("isAnyoneHurtingYou", {}) and extension_value.get("isAnyoneHurtingYou", {}).get("yes", "") else ""
        isAnyoneHurtingYou_no = str(extension_value.get("isAnyoneHurtingYou", {}).get("no", "")) if extension_value.get("isAnyoneHurtingYou", {}) and extension_value.get("isAnyoneHurtingYou", {}).get("no", "") else ""
        isAnyoneHurtingYou_unable_to_access = str(extension_value.get("isAnyoneHurtingYou", {}).get("unable_to_access", "")) if extension_value.get("isAnyoneHurtingYou", {}) and extension_value.get("isAnyoneHurtingYou", {}).get("unable_to_access", "") else ""

        hurting = str(extension_value.get("hurting", "")) if extension_value and extension_value.get("hurting", "") else ""
        unableHurting = str(extension_value.get("unableHurting", "")) if extension_value and extension_value.get("unableHurting", "") else ""

        talkSomeone_yes = str(extension_value.get("talkSomeone", {}).get("yes", "")) if extension_value.get("talkSomeone", {}) and extension_value.get("talkSomeone", {}).get("yes", "") else ""
        talkSomeone_no = str(extension_value.get("talkSomeone", {}).get("no", "")) if extension_value.get("talkSomeone", {}) and extension_value.get("talkSomeone", {}).get("no", "") else ""
        talkSomeone_unable_to_access = str(extension_value.get("talkSomeone", {}).get("unable_to_access", "")) if extension_value.get("talkSomeone", {}) and extension_value.get("talkSomeone", {}).get("unable_to_access", "") else ""

        unableTalk = str(extension_value.get("unableTalk", "")) if extension_value and extension_value.get("unableTalk", "") else ""
        abuseScreenComments = str(extension_value.get("abuseScreenComments", "")) if extension_value and extension_value.get("abuseScreenComments", "") else ""

        placeYouDoNotFeelSafe_yes = str(extension_value.get("placeYouDoNotFeelSafe", {}).get("yes", "")) if extension_value.get("placeYouDoNotFeelSafe", {}) and extension_value.get("placeYouDoNotFeelSafe", {}).get("yes", "") else ""
        placeYouDoNotFeelSafe_no = str(extension_value.get("placeYouDoNotFeelSafe", {}).get("no", "")) if extension_value.get("placeYouDoNotFeelSafe", {}) and extension_value.get("placeYouDoNotFeelSafe", {}).get("no", "") else ""
        placeYouDoNotFeelSafe_unable_to_access = str(extension_value.get("placeYouDoNotFeelSafe", {}).get("unable_to_access", "")) if extension_value.get("placeYouDoNotFeelSafe", {}) and extension_value.get("placeYouDoNotFeelSafe", {}).get("unable_to_access", "") else ""

        feelSafeYes = str(extension_value.get("feelSafeYes", "")) if extension_value and extension_value.get("feelSafeYes", "") else ""
        unableFeelSafe = str(extension_value.get("unableFeelSafe", "")) if extension_value and extension_value.get("unableFeelSafe", "") else ""

        isAnyoneHurtingYou1_yes = str(extension_value.get("isAnyoneHurtingYou1", {}).get("yes", "")) if extension_value.get("isAnyoneHurtingYou1", {}) and extension_value.get("isAnyoneHurtingYou1", {}).get("yes", "") else ""
        isAnyoneHurtingYou1_no = str(extension_value.get("isAnyoneHurtingYou1", {}).get("no", "")) if extension_value.get("isAnyoneHurtingYou1", {}) and extension_value.get("isAnyoneHurtingYou1", {}).get("no", "") else ""
        isAnyoneHurtingYou1_unable_to_access = str(extension_value.get("isAnyoneHurtingYou1", {}).get("unable_to_access", "")) if extension_value.get("isAnyoneHurtingYou1", {}) and extension_value.get("isAnyoneHurtingYou1", {}).get("unable_to_access", "") else ""

        hurtingYes = str(extension_value.get("hurtingYes", "")) if extension_value and extension_value.get("hurtingYes", "") else ""
        unableToHurting = str(extension_value.get("unableToHurting", "")) if extension_value and extension_value.get("unableToHurting", "") else ""

        afraidOfAnyone_yes = str(extension_value.get("afraidOfAnyone", {}).get("yes", "")) if extension_value.get("afraidOfAnyone", {}) and extension_value.get("afraidOfAnyone", {}).get("yes", "") else ""
        afraidOfAnyone_no = str(extension_value.get("afraidOfAnyone", {}).get("no", "")) if extension_value.get("afraidOfAnyone", {}) and extension_value.get("afraidOfAnyone", {}).get("no", "") else ""
        afraidOfAnyone_unable_to_access = str(extension_value.get("afraidOfAnyone", {}).get("unable_to_access", "")) if extension_value.get("afraidOfAnyone", {}) and extension_value.get("afraidOfAnyone", {}).get("unable_to_access", "") else ""

        personAfraid = str(extension_value.get("personAfraid", {}).get("value", "")) if extension_value.get("personAfraid", {}) and extension_value.get("personAfraid", {}).get("value", "") else ""
        otherPersonAfraid = str(extension_value.get("otherPersonAfraid", "")) if extension_value and extension_value.get("otherPersonAfraid", "") else ""
        unableToAssess = str(extension_value.get("unableToAssess", "")) if extension_value and extension_value.get("unableToAssess", "") else ""
        abuseScreenComments1 = str(extension_value.get("abuseScreenComments1", "")) if extension_value and extension_value.get("abuseScreenComments1", "") else ""

        nameOfPhysician = str(extension_value.get("nameOfPhysician", "")) if extension_value and extension_value.get("nameOfPhysician", "") else ""
        nameOfPhysicianDate = str(extension_value.get("nameOfPhysicianDate", "")) if extension_value and extension_value.get("nameOfPhysicianDate", "") else ""
        nameOfPhysicianTime = str(extension_value.get("nameOfPhysicianTime", "")) if extension_value and extension_value.get("nameOfPhysicianTime", "") else ""
        nameOfSocialWorker = str(extension_value.get("nameOfSocialWorker", "")) if extension_value and extension_value.get("nameOfSocialWorker", "") else ""
        nameOfSocialWorkerDate = str(extension_value.get("nameOfSocialWorkerDate", "")) if extension_value and extension_value.get("nameOfSocialWorkerDate", "") else ""
        nameOfSocialWorkerTime = str(extension_value.get("nameOfSocialWorkerTime", "")) if extension_value and extension_value.get("nameOfSocialWorkerTime", "") else ""

        rnName = str(extension_value.get("rnName", "")) if extension_value and extension_value.get("rnName", "") else ""
        rnNameDate = str(extension_value.get("rnNameDate", "")) if extension_value and extension_value.get("rnNameDate", "") else ""
        rnNameTime = str(extension_value.get("rnNameTime", "")) if extension_value and extension_value.get("rnNameTime", "") else ""

        form_response_id = str(document.get("identifier", [{}])[0].get("value", {}).get("value", "")) if document.get("identifier", [{}])[0] and document.get("identifier", [{}])[0].get("value", {}) and document.get("identifier", [{}])[0].get("value", {}).get("value", "") else ""
        status = str(document.get("status", {}).get("value", "")) if document.get("status", {}) and document.get("status", {}).get("value", "") else ""  # Update the default value to an empty string
        patientId = str(document.get("subject", "")) if document.get("subject", "") else ""
        markToBeDeleted = document.get("markToBeDeleted", "") if document.get("markToBeDeleted", "") else ""
        lastUpdatedBy = str(document.get("lastUpdatedBy", {}).get("reference", "")) if document.get("lastUpdatedBy", {}) and document.get("lastUpdatedBy", {}).get("reference", "") else ""
        mongo_date_created = document.get("dateCreated", "")
        mongo_last_updated = document.get("lastUpdated", "")

        mysql_valuesA = (
                mongo_id, service_request_id, created_by_user, patientName, patientDob, datOfSurgery, surgeryName, surgeonName, primaryPhon, alternatePhon, patientPreferredLanguage, otherPreferredLanguage, interpreterRequired_family, interpreterRequired_language_line_id_number, patientDesignated, informationObtained, languageLineId, advancedDirectives_yes, advancedDirectives_no, copyOnChart_yes, copyOnChart_no, allergies_no_known_allergy, allergies_yes, allergiesTo, allergies_reactions, surgicalHistory_denied, surgicalHistory_yes, surgicalHistoryYes, anesthesiaHistory_yes, anesthesiaHistory_no, bloodRelatives_yes, bloodRelatives_no, adverseReaction_yes, adverseReaction_no, reactions1, currentlyPain_yes, currentlyPain_no, painScale, painQuality, scaleType_Numerical, scaleType_face_, scaleType_flacc__, scaleType_pain_ad, painDuration_value, painLocation, visualImpairment_yes, visualImpairment_no, visualImpairmentText, auditoryImpairment_yes, auditoryImpairment_no, auditoryImpairmentText, congnitiveImpairment_yes, congnitiveImpairment_no, congnitiveImpairmentText, barriersToLearning_yes, barriersToLearning_no, learningBarrierType_value, ifOther, mobilityImpairment_yes, mobilityImpairment_no, mobilityImpairment_value, fallRiskAssessment_all_surgical_patients, fallRiskAssessment_fall_with_harm_risk, yellowWristband_needs_assistance, yellowWristband_current_falls, yellowWristband_85_years, yellowWristband_bone_condition, yellowWristband_has_bleeding_disorder, reportedNurologicProblem_yes, reportedNurologicProblem_no, seizures_yes, seizures_no, lastEpisode, onMeds_yes, onMeds_no, seizureHeadachesMigraines_yes, seizureHeadachesMigraines_no, frequency, lastEpisode1, onMeds1_no, onMeds1_yes, strokeCVATIA_yes, strokeCVATIA_no, lastEpisode2, weakness, vpShuntPlace_yes, vpShuntPlace_no, VpShunt, cronicDizziness_yes, cronicDizziness_no, lastEpisode3, onMeds2_yes, onMeds2_no, comment, cardiovascularProblem_yes, cardiovascularProblem_no, hypertension_yes, hypertension_no, mi_yes, mi_no, dateOfEvent, angina_yes, angina_no, lastEpisode4, chestPain_yes, chestPain_no, describe_, arterioscleroticHeartDisease_yes, arterioscleroticHeartDisease_no, onset, congestiveHeartFailure_yes, congestiveHeartFailure_no, onset1, swellingOfAnklesFeet_yes, swellingOfAnklesFeet_no, pitting_yes, pitting_no, arrhythmia_yes, arrhythmia_no, typeArrhythmia, murmurs_yes, murmurs_no, peripheralVascularDisease_yes, peripheralVascularDisease_no, otherPeripheral, pacemaker_yes, pacemaker_no, pacemakerDate, aicd_yes, aicd_no, aicdDate, cardiackStentBypass_yes, cardiackStentBypass_no, cardiack, pastBloodTransfusions_yes, pastBloodTransfusions_no, transfusionsReson, transfusionsDate, adverseReaction1_yes, adverseReaction1_no, describe3, active_yes, active_no, active1, withoutStoping_yes, withoutStoping_no, flights, atolerateLyingFlatPeriodOfTime_yes, atolerateLyingFlatPeriodOfTime_no, tolerate, onMeds3_yes, onMeds3_no, comment1,  form_response_id, status, patientId, lastUpdatedBy, markToBeDeleted, mongo_date_created, mongo_last_updated
          )

        mysql_valuesB = (
                mongo_id, service_request_id, respiratoryProblem_yes, respiratoryProblem_no, sleepApnea_yes, sleepApnea_no, daignosed, sleepStudyDone_yes, sleepStudyDone_no, sleepStudyDone_uses_cpap, asthma_yes, asthma_no, lastEpisode5, lastHospitalization, intubation_yes, intubation_no, intubationYes, chronicCoughBronchitis_yes, chronicCoughBronchitis_no, lastEpisode6, lastHospitalization1, intubation1_yes, intubation1_no, intubationYes1, emphysema_yes, emphysema_no, lastEpisode7, lastHospitalization2, intubation2_yes, intubation2_no, intubationYes2, pneumonia_yes, pneumonia_no, lastEpisode8, lastHospitalization3, intubation3_yes, intubation3_no, intubationYes3, tuberculosis_yes, tuberculosis_no, lastEpisode9, lastHospitalization4, intubation4_yes, intubation4_no, intubationYes4, recentUpperRespiratoryInfection_yes, recentUpperRespiratoryInfection_no, lastEpisode01, lastHospitalization05, intubation5_yes, intubation5_no, intubationYes5, lastEpisodeOther, lastEpisode02, lastHospitalization7, intubation7_yes, intubation7_no, intubationYes7, onMeds4_yes, onMeds4_no, comment2,reportedGastroinestinalProblem_yes, reportedGastroinestinalProblem_no, curentDiet, difficultySwallowing_yes, difficultySwallowing_no, pepticUlcerDisease_yes, pepticUlcerDisease_no, gerdHeartBurn_yes, gerdHeartBurn_no, hiatalHernia_yes, hiatalHernia_no, hepatitis_yes, hepatitis_no, hepatitisType, hepatitisDiagnosed, hepatitisTreated, irritableBowelSyndromeCrohn_yes, irritableBowelSyndromeCrohn_no, bowelActivity, unintentionalweightLoss_yes, unintentionalweightLoss_no, onMeds5_yes, onMeds5_no, comment3, reportRenalProblem_yes, reportRenalProblem_no, kidneyDisease_yes, kidneyDisease_no, urinaryInfections_yes, urinaryInfections_no, enlargedProstate_yes, enlargedProstate_no, difficultyUrinating_yes, difficultyUrinating_no, bloodUrine_yes, bloodUrine_no, onDialysis_yes, onDialysis_no, dialysisDays, dialysisType, shuntFistualAccess_yes, shuntFistualAccess_no, selfCatherization_yes, selfCatherization_no, otherCatherization, onMeds6_yes, onMeds6_no, comments, reportedMusculoskeletalProblem_yes, reportedMusculoskeletalProblem_no, arthritis_yes, arthritis_no, gout_yes, gout_no, osteoperosis_yes, osteoperosis_no, muscleWeakness_yes, muscleWeakness_no, fractures, others, onMeds7_yes, onMeds7_no, musculoskeletalComment, reportedHematologyProblem_yes, reportedHematologyProblem_no, bloodTransfusions_yes, bloodTransfusions_no, bloodTransfusionsReason, bloodTransfusionsDate, adverseReactions_yes, adverseReactions_no, adverseReactionsDescribe, bleedingProblem_yes, bleedingProblem_no, bruiseEasily_yes, bruiseEasily_no, sickleCellDisease_yes, sickleCellDisease_no, hivAIDS_yes, hivAIDS_no, leukemia_yes, leukemia_no, leukemiaYes, treatmentType_chemotherapy, treatmentType_bone_marrow_transplant, cancer_yes, cancer_no, treatmentTypeCancer__chemotherapy, treatmentTypeCancer_radiation_therapy, onMedsHematology_yes, onMedsHematology_no, hematologyComment, endocrineProblem_yes, endocrineProblem_no, diabetes_yes, diabetes_no, type_type_i, type_type_ii, diabetesDate, BloodGlucose_yes, BloodGlucose_no, bloodGlucoseMonitoring, lowRnge, highRange, hospitalizationDiabetes, reason, thyroidDisease_yes, thyroidDisease_no, HistorySteroidUse_yes, HistorySteroidUse_no, steroidHistory, onMeds8_yes, onMeds8_no, EndocrineComment
          )

        mysql_valuesC = (
                mongo_id, service_request_id,reportedSkinProblem_no, reportedSkinProblem_yes, intact_yes, intact_no, psoriasis_yes, psoriasis_no, eczdma_yes, eczdma_no, brusesCuts_yes, brusesCuts_no, rashesDescribe, rashesDescribeOther, skinComment, visualImpairmentGyn_yes, visualImpairmentGyn_no, visualImpairmentGyn_na, fynComment, menstrualPeriod, SapSmear, dateOfLastPelvicExam, pregnantNow_yes, pregnantNow_no, possibilityPregnantDescribe, lactating_yes, lactating_no, peds_na, deliveryType, preemie, birthWeight, immunizationDate_yes, immunizationDate_no, immunizationDetails, respiratoryInfection_no, respiratoryInfection_yes, respiratoryInfectionDetails, communicableDisease_measles, communicableDisease_chicken_pox, communicableDisease_tb, communicableDisease_meningitis, communicableDisease_na, communicableDiseaseOthers, pedsComment, culturalSurgery_yes, culturalSurgery_no, dayOfSurgery, livesAlone_yes, livesAlone_no, typeOfHouse, stairs, alcohol_yes, alcohol_no, freq, alcoholOther, alcoholAmount, drugs_yes, drugs_no, drugsFreq, drugsOther, drugslAmount, lastDoseAmount, caffeine_yes, caffeine_no, caffeineFreq, caffeineOther, caffeineAmount, cigarettesVaping_yes, cigarettesVaping_no, packsPerDay, lastCigarette, cigars_yes, cigars_no, cigarsPerDay, lastCigar, readinessToQuit, psychosocialComment, reportedPsychiatricProblem_yes, reportedPsychiatricProblem_no, anxiety_yes, anxiety_no, hallucinations_yes, hallucinations_no, depressions_yes, depressions_no, postTraumaticReaction_yes, postTraumaticReaction_no, suicidalIdeations_yes, suicidalIdeations_no, suicidalOther, onMeds9_yes, onMeds9_no, followedByMD_yes, followedByMD_no, psychiatricComment, afraidOfAnyyone_yes, afraidOfAnyyone_no, afraidOfAnyyone_unable_to_access, afraid, unableAfraid, isAnyoneHurtingYou_yes, isAnyoneHurtingYou_no, isAnyoneHurtingYou_unable_to_access, hurting, unableHurting, talkSomeone_yes, talkSomeone_no, talkSomeone_unable_to_access, unableTalk, abuseScreenComments, placeYouDoNotFeelSafe_yes, placeYouDoNotFeelSafe_no, placeYouDoNotFeelSafe_unable_to_access, feelSafeYes, unableFeelSafe, isAnyoneHurtingYou1_yes, isAnyoneHurtingYou1_no, isAnyoneHurtingYou1_unable_to_access, hurtingYes, unableToHurting, afraidOfAnyone_yes, afraidOfAnyone_no, afraidOfAnyone_unable_to_access, personAfraid, otherPersonAfraid, unableToAssess, abuseScreenComments1, nameOfPhysician, nameOfPhysicianDate, nameOfPhysicianTime, nameOfSocialWorker, nameOfSocialWorkerDate, nameOfSocialWorkerTime
          )

        batchA.append(mysql_valuesA)
        batchB.append(mysql_valuesB)
        batchC.append(mysql_valuesC)
        # Insert batch into MySQL
        try:
            if len(batchA) >= batch_size:
                config.mysql_cursor.executemany(
                    "INSERT INTO completeNursingAssessmentPartA (mongo_id, service_request_id, created_by_user, patientName, patientDob, datOfSurgery, surgeryName, surgeonName, primaryPhon, alternatePhon, patientPreferredLanguage, otherPreferredLanguage, interpreterRequired_family, interpreterRequired_language_line_id_number, patientDesignated, informationObtained, languageLineId, advancedDirectives_yes, advancedDirectives_no, copyOnChart_yes, copyOnChart_no, allergies_no_known_allergy, allergies_yes, allergiesTo, allergies_reactions, surgicalHistory_denied, surgicalHistory_yes, surgicalHistoryYes, anesthesiaHistory_yes, anesthesiaHistory_no, bloodRelatives_yes, bloodRelatives_no, adverseReaction_yes, adverseReaction_no, reactions1, currentlyPain_yes, currentlyPain_no, painScale, painQuality, scaleType_Numerical, scaleType_face_, scaleType_flacc__, scaleType_pain_ad, painDuration_value, painLocation, visualImpairment_yes, visualImpairment_no, visualImpairmentText, auditoryImpairment_yes, auditoryImpairment_no, auditoryImpairmentText, congnitiveImpairment_yes, congnitiveImpairment_no, congnitiveImpairmentText, barriersToLearning_yes, barriersToLearning_no, learningBarrierType_value, ifOther, mobilityImpairment_yes, mobilityImpairment_no, mobilityImpairment_value, fallRiskAssessment_all_surgical_patients, fallRiskAssessment_fall_with_harm_risk, yellowWristband_needs_assistance, yellowWristband_current_falls, yellowWristband_85_years, yellowWristband_bone_condition, yellowWristband_has_bleeding_disorder, reportedNurologicProblem_yes, reportedNurologicProblem_no, seizures_yes, seizures_no, lastEpisode, onMeds_yes, onMeds_no, seizureHeadachesMigraines_yes, seizureHeadachesMigraines_no, frequency, lastEpisode1, onMeds1_no, onMeds1_yes, strokeCVATIA_yes, strokeCVATIA_no, lastEpisode2, weakness, vpShuntPlace_yes, vpShuntPlace_no, VpShunt, cronicDizziness_yes, cronicDizziness_no, lastEpisode3, onMeds2_yes, onMeds2_no, comment, cardiovascularProblem_yes, cardiovascularProblem_no, hypertension_yes, hypertension_no, mi_yes, mi_no, dateOfEvent, angina_yes, angina_no, lastEpisode4, chestPain_yes, chestPain_no, describe_, arterioscleroticHeartDisease_yes, arterioscleroticHeartDisease_no, onset, congestiveHeartFailure_yes, congestiveHeartFailure_no, onset1, swellingOfAnklesFeet_yes, swellingOfAnklesFeet_no, pitting_yes, pitting_no, arrhythmia_yes, arrhythmia_no, typeArrhythmia, murmurs_yes, murmurs_no, peripheralVascularDisease_yes, peripheralVascularDisease_no, otherPeripheral, pacemaker_yes, pacemaker_no, pacemakerDate, aicd_yes, aicd_no, aicdDate, cardiackStentBypass_yes, cardiackStentBypass_no, cardiack, pastBloodTransfusions_yes, pastBloodTransfusions_no, transfusionsReson, transfusionsDate, adverseReaction1_yes, adverseReaction1_no, describe3, active_yes, active_no, active1, withoutStoping_yes, withoutStoping_no, flights, atolerateLyingFlatPeriodOfTime_yes, atolerateLyingFlatPeriodOfTime_no, tolerate, onMeds3_yes, onMeds3_no, comment1, form_response_id, status, patientId, lastUpdatedBy, markToBeDeleted, mongo_date_created, mongo_last_updated) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    batchA
                )
                print("Documents inserted for completeNursingAssessmentPartA",batchA)


                config.mysql_cursor.executemany(
                    "INSERT INTO completeNursingAssessmentPartB (mongo_id, service_request_id,  respiratoryProblem_yes, respiratoryProblem_no, sleepApnea_yes, sleepApnea_no, daignosed, sleepStudyDone_yes, sleepStudyDone_no, sleepStudyDone_uses_cpap, asthma_yes, asthma_no, lastEpisode5, lastHospitalization, intubation_yes, intubation_no, intubationYes, chronicCoughBronchitis_yes, chronicCoughBronchitis_no, lastEpisode6, lastHospitalization1, intubation1_yes, intubation1_no, intubationYes1, emphysema_yes, emphysema_no, lastEpisode7, lastHospitalization2, intubation2_yes, intubation2_no, intubationYes2, pneumonia_yes, pneumonia_no, lastEpisode8, lastHospitalization3, intubation3_yes, intubation3_no, intubationYes3, tuberculosis_yes, tuberculosis_no, lastEpisode9, lastHospitalization4, intubation4_yes, intubation4_no, intubationYes4, recentUpperRespiratoryInfection_yes, recentUpperRespiratoryInfection_no, lastEpisode01, lastHospitalization05, intubation5_yes, intubation5_no, intubationYes5, lastEpisodeOther, lastEpisode02, lastHospitalization7, intubation7_yes, intubation7_no, intubationYes7, onMeds4_yes, onMeds4_no, comment2,reportedGastroinestinalProblem_yes, reportedGastroinestinalProblem_no, curentDiet, difficultySwallowing_yes, difficultySwallowing_no, pepticUlcerDisease_yes, pepticUlcerDisease_no, gerdHeartBurn_yes, gerdHeartBurn_no, hiatalHernia_yes, hiatalHernia_no, hepatitis_yes, hepatitis_no, hepatitisType, hepatitisDiagnosed, hepatitisTreated, irritableBowelSyndromeCrohn_yes, irritableBowelSyndromeCrohn_no, bowelActivity, unintentionalweightLoss_yes, unintentionalweightLoss_no, onMeds5_yes, onMeds5_no, comment3, reportRenalProblem_yes, reportRenalProblem_no, kidneyDisease_yes, kidneyDisease_no, urinaryInfections_yes, urinaryInfections_no, enlargedProstate_yes, enlargedProstate_no, difficultyUrinating_yes, difficultyUrinating_no, bloodUrine_yes, bloodUrine_no, onDialysis_yes, onDialysis_no, dialysisDays, dialysisType, shuntFistualAccess_yes, shuntFistualAccess_no, selfCatherization_yes, selfCatherization_no, otherCatherization, onMeds6_yes, onMeds6_no, comments, reportedMusculoskeletalProblem_yes, reportedMusculoskeletalProblem_no, arthritis_yes, arthritis_no, gout_yes, gout_no, osteoperosis_yes, osteoperosis_no, muscleWeakness_yes, muscleWeakness_no, fractures, others, onMeds7_yes, onMeds7_no, musculoskeletalComment, reportedHematologyProblem_yes, reportedHematologyProblem_no, bloodTransfusions_yes, bloodTransfusions_no, bloodTransfusionsReason, bloodTransfusionsDate, adverseReactions_yes, adverseReactions_no, adverseReactionsDescribe, bleedingProblem_yes, bleedingProblem_no, bruiseEasily_yes, bruiseEasily_no, sickleCellDisease_yes, sickleCellDisease_no, hivAIDS_yes, hivAIDS_no, leukemia_yes, leukemia_no, leukemiaYes, treatmentType_chemotherapy, treatmentType_bone_marrow_transplant, cancer_yes, cancer_no, treatmentTypeCancer__chemotherapy, treatmentTypeCancer_radiation_therapy, onMedsHematology_yes, onMedsHematology_no, hematologyComment, endocrineProblem_yes, endocrineProblem_no, diabetes_yes, diabetes_no, type_type_i, type_type_ii, diabetesDate, BloodGlucose_yes, BloodGlucose_no, bloodGlucoseMonitoring, lowRnge, highRange, hospitalizationDiabetes, reason, thyroidDisease_yes, thyroidDisease_no, HistorySteroidUse_yes, HistorySteroidUse_no, steroidHistory, onMeds8_yes, onMeds8_no, EndocrineComment) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    batchB
                )
                print("Documents inserted for completeNursingAssessmentPartB",batchB)


                config.mysql_cursor.executemany(
                    "INSERT INTO completeNursingAssessmentPartC (mongo_id, service_request_id, reportedSkinProblem_no, reportedSkinProblem_yes, intact_yes, intact_no, psoriasis_yes, psoriasis_no, eczdma_yes, eczdma_no, brusesCuts_yes, brusesCuts_no, rashesDescribe, rashesDescribeOther, skinComment, visualImpairmentGyn_yes, visualImpairmentGyn_no, visualImpairmentGyn_na, fynComment, menstrualPeriod, SapSmear, dateOfLastPelvicExam, pregnantNow_yes, pregnantNow_no, possibilityPregnantDescribe, lactating_yes, lactating_no, peds_na, deliveryType, preemie, birthWeight, immunizationDate_yes, immunizationDate_no, immunizationDetails, respiratoryInfection_no, respiratoryInfection_yes, respiratoryInfectionDetails, communicableDisease_measles, communicableDisease_chicken_pox, communicableDisease_tb, communicableDisease_meningitis, communicableDisease_na, communicableDiseaseOthers, pedsComment, culturalSurgery_yes, culturalSurgery_no, dayOfSurgery, livesAlone_yes, livesAlone_no, typeOfHouse, stairs, alcohol_yes, alcohol_no, freq, alcoholOther, alcoholAmount, drugs_yes, drugs_no, drugsFreq, drugsOther, drugslAmount, lastDoseAmount, caffeine_yes, caffeine_no, caffeineFreq, caffeineOther, caffeineAmount, cigarettesVaping_yes, cigarettesVaping_no, packsPerDay, lastCigarette, cigars_yes, cigars_no, cigarsPerDay, lastCigar, readinessToQuit, psychosocialComment, reportedPsychiatricProblem_yes, reportedPsychiatricProblem_no, anxiety_yes, anxiety_no, hallucinations_yes, hallucinations_no, depressions_yes, depressions_no, postTraumaticReaction_yes, postTraumaticReaction_no, suicidalIdeations_yes, suicidalIdeations_no, suicidalOther, onMeds9_yes, onMeds9_no, followedByMD_yes, followedByMD_no, psychiatricComment, afraidOfAnyyone_yes, afraidOfAnyyone_no, afraidOfAnyyone_unable_to_access, afraid, unableAfraid, isAnyoneHurtingYou_yes, isAnyoneHurtingYou_no, isAnyoneHurtingYou_unable_to_access, hurting, unableHurting, talkSomeone_yes, talkSomeone_no, talkSomeone_unable_to_access, unableTalk, abuseScreenComments, placeYouDoNotFeelSafe_yes, placeYouDoNotFeelSafe_no, placeYouDoNotFeelSafe_unable_to_access, feelSafeYes, unableFeelSafe, isAnyoneHurtingYou1_yes, isAnyoneHurtingYou1_no, isAnyoneHurtingYou1_unable_to_access, hurtingYes, unableToHurting, afraidOfAnyone_yes, afraidOfAnyone_no, afraidOfAnyone_unable_to_access, personAfraid, otherPersonAfraid, unableToAssess, abuseScreenComments1, nameOfPhysician, nameOfPhysicianDate, nameOfPhysicianTime, nameOfSocialWorker, nameOfSocialWorkerDate, nameOfSocialWorkerTime) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    batchC
                )
                print("Documents inserted for completeNursingAssessmentPartC",batchC)



                config.mysql_connection.commit()
                processed_count += len(batchA)
                batchA = []
                batchB = []
                batchC = []

        except Exception as e:
            print("An error occurred while inserting documents:", str(e))
            # Insert the remaining records in the last batch
    if batchA:
        try:
            config.mysql_cursor.executemany(
                "INSERT INTO completeNursingAssessmentPartA (mongo_id, service_request_id, created_by_user, patientName, patientDob, datOfSurgery, surgeryName, surgeonName, primaryPhon, alternatePhon, patientPreferredLanguage, otherPreferredLanguage, interpreterRequired_family, interpreterRequired_language_line_id_number, patientDesignated, informationObtained, languageLineId, advancedDirectives_yes, advancedDirectives_no, copyOnChart_yes, copyOnChart_no, allergies_no_known_allergy, allergies_yes, allergiesTo, allergies_reactions, surgicalHistory_denied, surgicalHistory_yes, surgicalHistoryYes, anesthesiaHistory_yes, anesthesiaHistory_no, bloodRelatives_yes, bloodRelatives_no, adverseReaction_yes, adverseReaction_no, reactions1, currentlyPain_yes, currentlyPain_no, painScale, painQuality, scaleType_Numerical, scaleType_face_, scaleType_flacc__, scaleType_pain_ad, painDuration_value, painLocation, visualImpairment_yes, visualImpairment_no, visualImpairmentText, auditoryImpairment_yes, auditoryImpairment_no, auditoryImpairmentText, congnitiveImpairment_yes, congnitiveImpairment_no, congnitiveImpairmentText, barriersToLearning_yes, barriersToLearning_no, learningBarrierType_value, ifOther, mobilityImpairment_yes, mobilityImpairment_no, mobilityImpairment_value, fallRiskAssessment_all_surgical_patients, fallRiskAssessment_fall_with_harm_risk, yellowWristband_needs_assistance, yellowWristband_current_falls, yellowWristband_85_years, yellowWristband_bone_condition, yellowWristband_has_bleeding_disorder, reportedNurologicProblem_yes, reportedNurologicProblem_no, seizures_yes, seizures_no, lastEpisode, onMeds_yes, onMeds_no, seizureHeadachesMigraines_yes, seizureHeadachesMigraines_no, frequency, lastEpisode1, onMeds1_no, onMeds1_yes, strokeCVATIA_yes, strokeCVATIA_no, lastEpisode2, weakness, vpShuntPlace_yes, vpShuntPlace_no, VpShunt, cronicDizziness_yes, cronicDizziness_no, lastEpisode3, onMeds2_yes, onMeds2_no, comment, cardiovascularProblem_yes, cardiovascularProblem_no, hypertension_yes, hypertension_no, mi_yes, mi_no, dateOfEvent, angina_yes, angina_no, lastEpisode4, chestPain_yes, chestPain_no, describe_, arterioscleroticHeartDisease_yes, arterioscleroticHeartDisease_no, onset, congestiveHeartFailure_yes, congestiveHeartFailure_no, onset1, swellingOfAnklesFeet_yes, swellingOfAnklesFeet_no, pitting_yes, pitting_no, arrhythmia_yes, arrhythmia_no, typeArrhythmia, murmurs_yes, murmurs_no, peripheralVascularDisease_yes, peripheralVascularDisease_no, otherPeripheral, pacemaker_yes, pacemaker_no, pacemakerDate, aicd_yes, aicd_no, aicdDate, cardiackStentBypass_yes, cardiackStentBypass_no, cardiack, pastBloodTransfusions_yes, pastBloodTransfusions_no, transfusionsReson, transfusionsDate, adverseReaction1_yes, adverseReaction1_no, describe3, active_yes, active_no, active1, withoutStoping_yes, withoutStoping_no, flights, atolerateLyingFlatPeriodOfTime_yes, atolerateLyingFlatPeriodOfTime_no, tolerate, onMeds3_yes, onMeds3_no, comment1, form_response_id, status, patientId, lastUpdatedBy, markToBeDeleted, mongo_date_created, mongo_last_updated) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    batchA
                )
            print("Documents inserted for completeNursingAssessmentPartA",batchA)


            config.mysql_cursor.executemany(
                "INSERT INTO completeNursingAssessmentPartB (mongo_id, service_request_id,  respiratoryProblem_yes, respiratoryProblem_no, sleepApnea_yes, sleepApnea_no, daignosed, sleepStudyDone_yes, sleepStudyDone_no, sleepStudyDone_uses_cpap, asthma_yes, asthma_no, lastEpisode5, lastHospitalization, intubation_yes, intubation_no, intubationYes, chronicCoughBronchitis_yes, chronicCoughBronchitis_no, lastEpisode6, lastHospitalization1, intubation1_yes, intubation1_no, intubationYes1, emphysema_yes, emphysema_no, lastEpisode7, lastHospitalization2, intubation2_yes, intubation2_no, intubationYes2, pneumonia_yes, pneumonia_no, lastEpisode8, lastHospitalization3, intubation3_yes, intubation3_no, intubationYes3, tuberculosis_yes, tuberculosis_no, lastEpisode9, lastHospitalization4, intubation4_yes, intubation4_no, intubationYes4, recentUpperRespiratoryInfection_yes, recentUpperRespiratoryInfection_no, lastEpisode01, lastHospitalization05, intubation5_yes, intubation5_no, intubationYes5, lastEpisodeOther, lastEpisode02, lastHospitalization7, intubation7_yes, intubation7_no, intubationYes7, onMeds4_yes, onMeds4_no, comment2,reportedGastroinestinalProblem_yes, reportedGastroinestinalProblem_no, curentDiet, difficultySwallowing_yes, difficultySwallowing_no, pepticUlcerDisease_yes, pepticUlcerDisease_no, gerdHeartBurn_yes, gerdHeartBurn_no, hiatalHernia_yes, hiatalHernia_no, hepatitis_yes, hepatitis_no, hepatitisType, hepatitisDiagnosed, hepatitisTreated, irritableBowelSyndromeCrohn_yes, irritableBowelSyndromeCrohn_no, bowelActivity, unintentionalweightLoss_yes, unintentionalweightLoss_no, onMeds5_yes, onMeds5_no, comment3, reportRenalProblem_yes, reportRenalProblem_no, kidneyDisease_yes, kidneyDisease_no, urinaryInfections_yes, urinaryInfections_no, enlargedProstate_yes, enlargedProstate_no, difficultyUrinating_yes, difficultyUrinating_no, bloodUrine_yes, bloodUrine_no, onDialysis_yes, onDialysis_no, dialysisDays, dialysisType, shuntFistualAccess_yes, shuntFistualAccess_no, selfCatherization_yes, selfCatherization_no, otherCatherization, onMeds6_yes, onMeds6_no, comments, reportedMusculoskeletalProblem_yes, reportedMusculoskeletalProblem_no, arthritis_yes, arthritis_no, gout_yes, gout_no, osteoperosis_yes, osteoperosis_no, muscleWeakness_yes, muscleWeakness_no, fractures, others, onMeds7_yes, onMeds7_no, musculoskeletalComment, reportedHematologyProblem_yes, reportedHematologyProblem_no, bloodTransfusions_yes, bloodTransfusions_no, bloodTransfusionsReason, bloodTransfusionsDate, adverseReactions_yes, adverseReactions_no, adverseReactionsDescribe, bleedingProblem_yes, bleedingProblem_no, bruiseEasily_yes, bruiseEasily_no, sickleCellDisease_yes, sickleCellDisease_no, hivAIDS_yes, hivAIDS_no, leukemia_yes, leukemia_no, leukemiaYes, treatmentType_chemotherapy, treatmentType_bone_marrow_transplant, cancer_yes, cancer_no, treatmentTypeCancer__chemotherapy, treatmentTypeCancer_radiation_therapy, onMedsHematology_yes, onMedsHematology_no, hematologyComment, endocrineProblem_yes, endocrineProblem_no, diabetes_yes, diabetes_no, type_type_i, type_type_ii, diabetesDate, BloodGlucose_yes, BloodGlucose_no, bloodGlucoseMonitoring, lowRnge, highRange, hospitalizationDiabetes, reason, thyroidDisease_yes, thyroidDisease_no, HistorySteroidUse_yes, HistorySteroidUse_no, steroidHistory, onMeds8_yes, onMeds8_no, EndocrineComment) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                batchB
            )
            print("Documents inserted for completeNursingAssessmentPartB",batchB)


            config.mysql_cursor.executemany(
                "INSERT INTO completeNursingAssessmentPartC (mongo_id, service_request_id, reportedSkinProblem_no, reportedSkinProblem_yes, intact_yes, intact_no, psoriasis_yes, psoriasis_no, eczdma_yes, eczdma_no, brusesCuts_yes, brusesCuts_no, rashesDescribe, rashesDescribeOther, skinComment, visualImpairmentGyn_yes, visualImpairmentGyn_no, visualImpairmentGyn_na, fynComment, menstrualPeriod, SapSmear, dateOfLastPelvicExam, pregnantNow_yes, pregnantNow_no, possibilityPregnantDescribe, lactating_yes, lactating_no, peds_na, deliveryType, preemie, birthWeight, immunizationDate_yes, immunizationDate_no, immunizationDetails, respiratoryInfection_no, respiratoryInfection_yes, respiratoryInfectionDetails, communicableDisease_measles, communicableDisease_chicken_pox, communicableDisease_tb, communicableDisease_meningitis, communicableDisease_na, communicableDiseaseOthers, pedsComment, culturalSurgery_yes, culturalSurgery_no, dayOfSurgery, livesAlone_yes, livesAlone_no, typeOfHouse, stairs, alcohol_yes, alcohol_no, freq, alcoholOther, alcoholAmount, drugs_yes, drugs_no, drugsFreq, drugsOther, drugslAmount, lastDoseAmount, caffeine_yes, caffeine_no, caffeineFreq, caffeineOther, caffeineAmount, cigarettesVaping_yes, cigarettesVaping_no, packsPerDay, lastCigarette, cigars_yes, cigars_no, cigarsPerDay, lastCigar, readinessToQuit, psychosocialComment, reportedPsychiatricProblem_yes, reportedPsychiatricProblem_no, anxiety_yes, anxiety_no, hallucinations_yes, hallucinations_no, depressions_yes, depressions_no, postTraumaticReaction_yes, postTraumaticReaction_no, suicidalIdeations_yes, suicidalIdeations_no, suicidalOther, onMeds9_yes, onMeds9_no, followedByMD_yes, followedByMD_no, psychiatricComment, afraidOfAnyyone_yes, afraidOfAnyyone_no, afraidOfAnyyone_unable_to_access, afraid, unableAfraid, isAnyoneHurtingYou_yes, isAnyoneHurtingYou_no, isAnyoneHurtingYou_unable_to_access, hurting, unableHurting, talkSomeone_yes, talkSomeone_no, talkSomeone_unable_to_access, unableTalk, abuseScreenComments, placeYouDoNotFeelSafe_yes, placeYouDoNotFeelSafe_no, placeYouDoNotFeelSafe_unable_to_access, feelSafeYes, unableFeelSafe, isAnyoneHurtingYou1_yes, isAnyoneHurtingYou1_no, isAnyoneHurtingYou1_unable_to_access, hurtingYes, unableToHurting, afraidOfAnyone_yes, afraidOfAnyone_no, afraidOfAnyone_unable_to_access, personAfraid, otherPersonAfraid, unableToAssess, abuseScreenComments1, nameOfPhysician, nameOfPhysicianDate, nameOfPhysicianTime, nameOfSocialWorker, nameOfSocialWorkerDate, nameOfSocialWorkerTime) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                batchC
            )
            print("Documents inserted for completeNursingAssessmentPartC",batchC)


            config.mysql_connection.commit()
            processed_count += len(batchA)

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

