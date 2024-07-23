import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

from langchain.prompts import PromptTemplate
from langchain.llms import OpenAI
from langchain.chat_models import ChatOpenAI
from langchain.schema import BaseOutputParser
from langchain.prompts import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    MessagesPlaceholder,
    SystemMessagePromptTemplate,
)

class PatientAgent():
    def __init__(self):
        self.prompt_agent_template=PromptTemplate.from_template('''You are doing a role play of patient.Your job is to answer the questions asked by the doctor.
    1. You should strictly provide answers from the patient case study presented in context. 
    2. Answer in detail about your episode if there is a related question
    2. Any information asked which is not presented in the context should be answered as no or you are not aware as per relevance
    3. Do not provide answer to any question which you don't konw. Answer:"I don't know"
    Patient_case_context:
    ----------------------
    {case_study}
    ---------------------
    Question:{ques}
    Answer:
    ''')
        self.llm=ChatOpenAI(model_name="gpt-3.5-turbo",temperature=0)
        self.llm_chain = self.prompt_agent_template | self.llm
        logger.info("Patient Agent Loaded")

    def run(self,question,case_study):
        return self.llm_chain.invoke({"ques": question,"case_study":case_study}).content
    
class SummaryAgent():
    def __init__(self):
        self.prompt_template=PromptTemplate.from_template('''You are a medical assistant to the history taking session. Create a summary profile for the 
        patient basis conversation below:
        ----------------------
        history taking conversation
        ----------------------
        {doc_patent_conv}
    ''')
        self.llm=ChatOpenAI(model_name="gpt-3.5-turbo",temperature=0)
        self.llm_chain = self.prompt_template | self.llm
        logger.info("Summary Agent Loaded")

    def run(self,doc_patent_conv):
        return self.llm_chain.invoke({"doc_patent_conv": doc_patent_conv}).content
    
class DdxAgent():
    def __init__(self):
        self.prompt_template=PromptTemplate.from_template('''You are an extremely reknowned doctor and cannot miss a diagnois.Given a patient summary you need to give the ddx
        ----------------------
        Patient summary
        ----------------------
        {patient_summary}
        --------------------
        Formatting instructions
        Output should be a json with a list of "ddx" not more than 4
    ''')
        self.llm=ChatOpenAI(model_name="gpt-3.5-turbo",temperature=0)
        self.llm_chain = self.prompt_template | self.llm
        logger.info("Differential Diagnosis Agent Loaded")

    def run(self,patient_summary):
        return self.llm_chain.invoke({"patient_summary": patient_summary}).content
    
class MedHistoryTakerAgent():
    def __init__(self):
        self.prompt_agent_template='''You are an acting assistant to a doctor.Your job has few key roles:
        1. Gather all necessary information about the chief complaint keeping in mind the SOCRATES model
        2. Donot combine all SOCRATES questions into 1 question
        3. Gather related information about associated symptoms related to chief complaint
        4. Gather Specific past medical history that may be related to the chief complaint
        5. Gather Comorbidities history(mandatory)
        Formatting instructions
        Output should be a json with a list of "questions"
        ----------------------
    '''
        self.llm=ChatOpenAI(model_name="gpt-3.5-turbo",temperature=0)
        self.prompt_template = ChatPromptTemplate(
                messages=[
                    SystemMessagePromptTemplate.from_template(self.prompt_agent_template),
                    #MessagesPlaceholder(variable_name="chat_history"),
                    HumanMessagePromptTemplate.from_template("{complain}"),
                ]
            )
        self.llm_chain = self.prompt_template | self.llm
        logger.info("Medical History Agent Loaded")

    def run(self,complain):
        return self.llm_chain.invoke({"complain": complain}).content
    
class DdxMatchAgent():
    
    def __init__(self):
        self.prompt_agent_template=PromptTemplate.from_template('''You are a medical professional designed to compare two diagnosis results:
        1. You should compare diagnosis_1 with diagnosis_2
        2. You should answer "Match", "Strong Match", "Weak Match", "Not Match"
        3. You should provide a reason for the matching of the results
        4. Do not provide misinformation.
        
        diagnosis_1: {diagnosis_1}
        diagnosis_2: {diagnosis_2}
        Answer:
        ''')
        self.llm=ChatOpenAI(model_name="gpt-3.5-turbo",temperature=0)
        self.llm_chain = self.prompt_agent_template | self.llm
        logger.info("DDX Match Agent Loaded")
        
    # def __init__(self):
    #     self.prompt_agent_template=PromptTemplate.from_template('''You are a medical professional designed to compare two diagnosis results:
    # 1. You are provided a two diagnosis results that you need to compare.
    # 2. You are expected to answer "Yes", "Partial", "No"
    # 2. JSON Format: Always present the extracted information in a JSON format.
    # 3. You need to provide confidence score for the matching of the results.
    # 4. match_value: match score for the comparison , it can only be (No for no match, Partial for partial match and Yes for full match)
    # 5. match_reason: reason for matching of the results
    # 6. Never add JSON markdown in your response.
    # 7. Keep language simple and easy to understand.

    # diagnosis 1 : 
    # diagnosis 2 : 
    
    # ''')
    #     self.parser = PydanticOutputParser(pydantic_object=DDXMatchResponseLLM)
    #     self.llm=ChatOpenAI(model_name="gpt-3.5-turbo",temperature=0)
    #     logger.info("DDX Matching Agent Loaded")

    def run(self,actual_diagnosis,differential_diagnosis_result):
        return self.llm_chain.invoke({"diagnosis_1": actual_diagnosis,"diagnosis_2":differential_diagnosis_result}).content
        # conversation = LLMChain(llm=self.llm, prompt=self.prompt_agent_template, verbose=True, output_parser=self.parser)
        # out = conversation({"question": f"{actual_diagnosis} {differential_diagnosis_result}"})
        # resp = out["text"].dict()
        # return resp
