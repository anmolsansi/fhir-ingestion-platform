from app.etl.transform import patient_to_row


def test_patient_to_row_minimal():
    raw = {"resourceType": "Patient", "id": "p1", "name": [{"family": "Doe", "given": ["John"]}]}
    row = patient_to_row(raw)
    assert row["id"] == "p1"
    assert row["family"] == "Doe"
    assert row["given"] == "John"
