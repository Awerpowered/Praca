import gspread
from oauth2client.service_account import ServiceAccountCredentials
import random
import json

# Ustawienia do autoryzacji
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("../../Desktop/slack-463519-91afa873e54a.json", scope)
client = gspread.authorize(creds)
headers = ["Created", "From", "Text", "Tweet_link","add_link"]

# Nazwa arkusza źródłowego (ten, z którego pobieramy dane)
source_sheet_name = "Twitter_LLM"  # <- zmień na właściwą nazwę
source_sheet = client.open(source_sheet_name).sheet1

# Pobierz wszystkie dane jako listę słowników
data = source_sheet.get_all_records(expected_headers=headers)

# Możesz wypisać dane jako JSON
with open("sheet_data.json", "w") as f:
    json.dump(data, f, indent=2)

# Wybierz 5 losowych wierszy
random_rows = random.sample(data, 5)

# Utwórz nowy arkusz do publikacji (lub otwórz, jeśli istnieje)
try:
    publish_sheet = client.open("Slack_publish").sheet1
except gspread.SpreadsheetNotFound:
    publish_sheet = client.create("Slack_publish").sheet1


# Dodaj nagłówki
if random_rows:
    headers = list(random_rows[0].keys())
    publish_sheet.append_row(headers)

# Dodaj dane
for row in random_rows:
    publish_sheet.append_row(list(row.values()))

print("✔️ Dane zostały zapisane do arkusza Slack_publish.")
