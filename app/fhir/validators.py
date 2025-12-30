from fhir.resources.condition import Condition as ConditionModel
from fhir.resources.encounter import Encounter as EncounterModel
from fhir.resources.medicationrequest import MedicationRequest as MedicationRequestModel
from fhir.resources.observation import Observation as ObservationModel
from fhir.resources.patient import Patient as PatientModel


def validate_patient(raw: dict) -> dict:
    obj = PatientModel.model_validate(raw)
    return obj.model_dump()


def validate_observation(raw: dict) -> dict:
    obj = ObservationModel.model_validate(raw)
    return obj.model_dump()


def validate_encounter(raw: dict) -> dict:
    obj = EncounterModel.model_validate(raw)
    return obj.model_dump()


def validate_condition(raw: dict) -> dict:
    obj = ConditionModel.model_validate(raw)
    return obj.model_dump()


def validate_medication_request(raw: dict) -> dict:
    obj = MedicationRequestModel.model_validate(raw)
    return obj.model_dump()
