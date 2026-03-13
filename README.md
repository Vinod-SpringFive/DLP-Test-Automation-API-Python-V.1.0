# DLP Test Automation — API Edition (Python V1.0)

A Flask web app for automating DLP interface testing against Salesforce using direct REST API calls — no browser required.

---

## Setup & Run

### 1. Clone the repository

```bash
git clone https://github.com/Vinod-SpringFive/DLP-Test-Automation-API-Python-V.1.0.git
cd DLP-Test-Automation-API-Python-V.1.0
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Add your private key

Place your RSA private key file in the project root and name it `private.key`.

> Never commit this file to git.

### 5. Create a `.env` file

```env
SALESFORCE_CLIENT_ID=your_connected_app_consumer_key
SALESFORCE_USERNAME=your_salesforce_username@company.com
SALESFORCE_PRIVATE_KEY_PATH=private.key
SALESFORCE_LOGIN_URL=https://test.salesforce.com
SALESFORCE_ORG_URL=https://your-instance.lightning.force.com
APP_USER=admin
APP_PASS=your_password
```

### 6. Run the app

```bash
python dlp_api.py
```

Open `http://localhost:8000` in your browser and log in with the `APP_USER` and `APP_PASS` from your `.env`.
