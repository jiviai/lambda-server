# Lambda Server

Lambda Server is a Python-based repository designed to manage different Lambda functions. Each Lambda function is contained within its own module with a `controller.py` file and is executed through the `lambda_handler` function.

## Repository Structure

This repository contains the following Lambda function modules:

- **chat_agent**: Contains functionalities related to the chat agent.
- **doctor_patient_evaluation**: Contains functionalities for doctor-patient evaluations.
- **evaluation_workflow**: Contains functionalities for evaluation workflows.
- **related_queries**: Contains functionalities for handling related queries.
- **search_content**: Contains functionalities for searching content.

## Installation

To run this repository locally, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/lambda-server.git
   cd lambda-server
   ```

2. Create a virtual environment and activate it:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running Locally

You can use the provided `Makefile` to run the application locally. The `Makefile` contains commands for running each module independently.

### Makefile

Create a `Makefile` in the root of your repository with the following content:

```makefile
.PHONY: all chat_agent doctor_patient_evaluation evaluation_workflow related_queries search_content

all: chat_agent doctor_patient_evaluation evaluation_workflow related_queries search_content

chat_agent:
	python3 chat_agent/controller.py

doctor_patient_evaluation:
	python3 doctor_patient_evaluation/controller.py

evaluation_workflow:
	python3 evaluation_workflow/controller.py

related_queries:
	python3 related_queries/controller.py

search_content:
	python3 search_content/controller.py
```

To run a specific module, use the following command:
```bash
make <module_name>
```

For example, to run the `search_content` module:
```bash
make search_content
```

## Example Lambda Handler

Each module's `controller.py` contains a `lambda_handler` function which is the entry point for the Lambda function. Below is an example outline for a typical `lambda_handler` function:

```python
def lambda_handler(event, context):
    # Your code here
    pass
```

## Contributing

Please ensure any pull requests include an explanation of what changes were made and why the change was necessary.

## Maintainer

This repository is maintained by:
- **Rishabh**
  - Email: [rishabh@jivi.ai](mailto:rishabh@jivi.ai)

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

---