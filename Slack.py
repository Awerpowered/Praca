import gspread
from oauth2client.service_account import ServiceAccountCredentials
import random
import json
import os # Potrzebujemy tej biblioteki

# --- POCZĄTEK ZMIAN ---

# 1. Pobierz zawartość klucza ze zmiennej środowiskowej, którą ustawi GitHub
creds_json_string = os.environ.get('GCP_SA_KEY')

# Sprawdzenie, czy sekret został poprawnie załadowany
if not creds_json_string:
    raise ValueError("Nie znaleziono sekretu GCP_SA_KEY. Upewnij się, że jest ustawiony w GitHub Secrets.")

# 2. Zamień string JSON na słownik Pythona
creds_dict = json.loads(creds_json_string)

# 3. Użyj słownika do autoryzacji zamiast pliku
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# --- KONIEC ZMIAN ---


# Reszta kodu pozostaje bez zmian
headers = ["Created", "From", "Text", "Tweet_link", "add_link"]
source_sheet_name = "Twitter_LLM"
source_sheet = client.open(source_sheet_name).sheet1

data = source_sheet.get_all_records(expected_headers=headers)

# Wybierz 5 losowych wierszy (lub mniej, jeśli jest ich mniej)
if len(data) > 0:
    sample_size = min(len(data), 5)
    random_rows = random.sample(data, sample_size)

    # Utwórz nowy arkusz do publikacji (lub otwórz, jeśli istnieje)
    try:
        publish_sheet = client.open("Slack_publish").sheet1
    except gspread.SpreadsheetNotFound:
        # Jeśli arkusz nie istnieje, stwórz go i udostępnij dla konta serwisowego
        service_account_email = creds.service_account_email
        publish_sheet = client.create("Slack_publish").sheet1
        publish_sheet.share(service_account_email, perm_type='user', role='writer')

    # Opcjonalnie: wyczyść arkusz przed dodaniem nowych danych


    # Dodaj nagłówki
    if random_rows:
        headers = list(random_rows[0].keys())
        publish_sheet.append_row(headers)

        # Dodaj dane
        for row in random_rows:
            publish_sheet.append_row(list(row.values()))

    print(f"✔️ Dane ({len(random_rows)} wierszy) zostały zapisane do arkusza Slack_publish.")
else:
    print("✔️ Arkusz źródłowy jest pusty. Nie wykonano żadnych operacji.")