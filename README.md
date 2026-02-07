# Supply-Chain-Attack-Detection

## Prerequisites
- Python 3.14.1
- Free [Libraries.io API Key](https://libraries.io/)

## Installation
### Clone git repository
```bash
git clone https://github.com/rahulkfernandes/Supply-Chain-Attack-Detection.git
```

### Install Dependencies
1. Create and activate a virtual environment (optional but recommended) You can use either venv or conda.

venv:
```bash
# Create and activate a virtual environment (optional but recommended)
python -m venv <env_name>
source <env_name>/bin/activate  # On Windows, use: <env_name>\Scripts\activate
```
OR

conda
```bash
conda create -n <env_name> python=3.14.1
conda activate <env_name>
```

2. Install python dependecies
```bash
# Install the required packages
pip install -r requirements.txt
```

### Setting Up Environment Variables
1. Create your local environment file:
```bash
cp .env.example .env
```
2. Update the .env file in root directory with your Libraries.io API key
```bash
LIBRARIES_IO_KEY = "<API-KEY>"
```

## Usage

To run data collection pipeline:
```
python -m scripts.run_data_collection --num_pkgs <NUM_PACKAGES>
```

## Contact

**Rahul Keneth Fernandes**  
Email: rahulkfernandes@gmail.com  
Github: [@rahulkfernandes](https://github.com/rahulkfernandes)