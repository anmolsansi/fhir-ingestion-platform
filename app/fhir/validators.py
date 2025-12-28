from fhir.resources.observation import Observation as ObservationModel
from fhir.resources.patient import Patient as PatientModel


def validate_patient(raw: dict) -> dict:
    obj = PatientModel.model_validate(raw)
    return obj.model_dump()


def validate_observation(raw: dict) -> dict:
    obj = ObservationModel.model_validate(raw)
    return obj.model_dump()
