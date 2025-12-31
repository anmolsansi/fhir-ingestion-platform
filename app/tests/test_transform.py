from app.etl.transform import observation_to_row, patient_to_row


def test_patient_to_row_minimal():
    raw = {"resourceType": "Patient", "id": "p1", "name": [{"family": "Doe", "given": ["John"]}]}
    row = patient_to_row(raw)
    assert row["id"] == "p1"
    assert row["family"] == "Doe"
    assert row["given"] == "John"


def test_observation_subject_patient_reference():
    raw = {
        "resourceType": "Observation",
        "id": "o1",
        "subject": {"reference": "Patient/p123"},
        "status": "final",
        "code": {"coding": [{"code": "8480-6", "display": "Systolic blood pressure"}]},
        "valueQuantity": {"value": 120, "unit": "mmHg"},
    }
    row = observation_to_row(raw)
    assert row["patient_id"] == "p123"
    assert row["value_quantity"] == 120
