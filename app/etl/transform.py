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
