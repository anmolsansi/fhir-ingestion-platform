def patient_to_row(patient: dict) -> dict:
    name = (patient.get("name") or [{}])[0]
    family = name.get("family")
    given_list = name.get("given") or []
    given = given_list[0] if given_list else None

    return {
        "id": patient["id"],
        "family": family,
        "given": given,
        "gender": patient.get("gender"),
        "birth_date": patient.get("birthDate"),
        "raw": patient,
    }


def observation_to_row(obs: dict) -> dict:
    # subject.reference is usually "Patient/{id}"
    subject_ref = ((obs.get("subject") or {}).get("reference") or "")
    patient_id = subject_ref.split("/")[-1] if "/" in subject_ref else subject_ref

    code = (((obs.get("code") or {}).get("coding") or [{}])[0])
    code_value = code.get("code")
    code_display = code.get("display")

    value_quantity = None
    value_unit = None
    value_string = None

    if "valueQuantity" in obs and obs["valueQuantity"]:
        value_quantity = obs["valueQuantity"].get("value")
        value_unit = obs["valueQuantity"].get("unit")
    elif "valueString" in obs:
        value_string = obs.get("valueString")

    return {
        "id": obs["id"],
        "patient_id": patient_id,
        "status": obs.get("status"),
        "code": code_value,
        "code_display": code_display,
        "effective_datetime": obs.get("effectiveDateTime"),
        "value_quantity": value_quantity,
        "value_unit": value_unit,
        "value_string": value_string,
        "raw": obs,
    }


def _subject_patient_id(resource: dict) -> str:
    subject_ref = ((resource.get("subject") or {}).get("reference") or "")
    return subject_ref.split("/")[-1] if "/" in subject_ref else subject_ref


def encounter_to_row(encounter: dict) -> dict:
    patient_id = _subject_patient_id(encounter)
    period = encounter.get("period") or {}
    return {
        "id": encounter["id"],
        "patient_id": patient_id,
        "status": encounter.get("status"),
        "encounter_class": encounter.get("class", {}).get("code") if isinstance(encounter.get("class"), dict) else None,
        "start": period.get("start"),
        "end": period.get("end"),
        "raw": encounter,
    }


def condition_to_row(condition: dict) -> dict:
    patient_id = _subject_patient_id(condition)
    code = (condition.get("code") or {}).get("coding") or [{}]
    code_value = code[0].get("code") if code else None
    onset = condition.get("onsetDateTime") or condition.get("onsetString")
    return {
        "id": condition["id"],
        "patient_id": patient_id,
        "clinical_status": (condition.get("clinicalStatus") or {}).get("text"),
        "verification_status": (condition.get("verificationStatus") or {}).get("text"),
        "code": code_value,
        "onset": onset,
        "recorded_date": condition.get("recordedDate"),
        "raw": condition,
    }


def medication_request_to_row(med: dict) -> dict:
    patient_id = _subject_patient_id(med)
    med_code = (med.get("medicationCodeableConcept") or {}).get("coding") or [{}]
    med_value = med_code[0].get("code") if med_code else None
    return {
        "id": med["id"],
        "patient_id": patient_id,
        "status": med.get("status"),
        "intent": med.get("intent"),
        "medication_code": med_value,
        "authored_on": med.get("authoredOn"),
        "raw": med,
    }
