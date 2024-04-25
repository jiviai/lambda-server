import json

from doctor_patient_evaluation.llm.llm import MedHistoryTakerAgent, PatientAgent

patient_agent = PatientAgent()
med_history_taker_agent = MedHistoryTakerAgent()

def fetch_med_history(
    case_study
):
    doc_patient_conv = ""
    symptom = str(patient_agent.run("what is your primary complain?",case_study))
    doc_patient_conv = f"Assistant: what is your primary complain? \n Patient: {symptom}"
    questions = json.loads(med_history_taker_agent.run(symptom))['questions']
    for question in questions:
        symptom = str(patient_agent.run(question,case_study))
        print(f"Patient:{symptom}")
        doc_patient_conv += f"\n Assistant: {question} \n Patient: {symptom}"
    return doc_patient_conv