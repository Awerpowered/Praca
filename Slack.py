import gspread
from google.oauth2.service_account import Credentials  # Używamy nowszej, zalecanej biblioteki
import openai
import json
import os

# --- KONFIGURACJA ---

# 1. Konfiguracja Google Sheets API z użyciem GitHub Secrets
#    Skrypt pobiera klucz ze zmiennej środowiskowej 'GCP_SA_KEY'.
try:
    creds_json_string = os.environ.get('GCP_SA_KEY')
    if not creds_json_string:
        raise ValueError(
            "Sekret GCP_SA_KEY nie został znaleziony. Upewnij się, że jest poprawnie skonfigurowany w GitHub Secrets.")

    # Konwertujemy string JSON na słownik Pythona
    creds_dict = json.loads(creds_json_string)

    # Autoryzacja przy użyciu słownika (zamiast pliku)
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    print("🔑 Pomyślnie autoryzowano przy użyciu klucza z sekretów GitHub.")

except (ValueError, json.JSONDecodeError) as e:
    print(f"BŁĄD: Problem z kluczem Google Service Account: {e}")
    exit()
except Exception as e:
    print(f"BŁĄD: Nieoczekiwany problem z autoryzacją Google: {e}")
    exit()

# 2. Konfiguracja OpenAI API
#    Ustaw swój klucz API jako zmienną środowiskową o nazwie 'OPENAI_API_KEY'.


# 3. Nazwy arkuszy i nagłówki
SOURCE_SHEET_NAME = "Twitter_LLM"
EXPECTED_HEADERS = ["Created", "From", "Text", "Tweet_link", "add_link"]
PUBLISH_SPREADSHEET_NAME = "Slack_publish"
RESULTS_WORKSHEET_NAME = "test"


# --- GŁÓWNA LOGIKA SKRYPTU ---

def get_best_tweets_from_ai(tweets_data):
    """
    Wysyła teksty tweetów do API OpenAI i prosi o wybranie 5 najlepszych.

    Args:
        tweets_data (list of dict): Lista tweetów, gdzie każdy tweet to słownik.

    Returns:
        list of str: Lista 5 wybranych tekstów tweetów lub pusta lista w przypadku błędu.
    """
    print("🤖 Łączenie z API OpenAI w celu analizy tweetów...")

    # Przygotowanie listy tekstów do wysłania (używamy nagłówka 'Text')
    all_texts = [tweet.get('Text', '') for tweet in tweets_data if tweet.get('Text')]

    if not all_texts:
        print("⚠️ Nie znaleziono żadnych tekstów w kolumnie 'Text'.")
        return []

    # Tworzymy jeden duży tekst, aby wysłać go w jednym zapytaniu
    separator = "\n---TWEET---\n"
    combined_texts = separator.join(all_texts)

    # Definicja promptów dla modelu LLM
    system_prompt = (
        "Jesteś ekspertem w dziedzinie sztucznej inteligencji. Twoim zadaniem jest analiza listy tweetów "
        "oddzielonych separatorem '---TWEET---'. Zwróć odpowiedź w formacie JSON zawierającą obiekt z jednym kluczem 'selected_tweets', "
        "którego wartością jest tablica (array) zawierająca DOKŁADNIE 5 stringów. Każdy string to PEŁNA i NIENARUSZONA treść tweeta, "
        "który jest najbardziej wartościowy i wnosi istotne informacje o nowościach, trendach lub ważnych wydarzeniach w świecie AI. "
        "Nie dodawaj żadnych wyjaśnień, numeracji ani dodatkowego tekstu poza odpowiedzią JSON."
    )

    user_prompt = f"Oto lista tweetów do analizy:\n{combined_texts}"

    try:
        response = openai.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"}
        )

        response_content = response.choices[0].message.content
        parsed_json = json.loads(response_content)

        selected_texts = parsed_json.get("selected_tweets", [])

        if isinstance(selected_texts, list):
            print(f"✔️ AI pomyślnie wybrało {len(selected_texts)} tweetów.")
            return selected_texts
        else:
            raise ValueError("Klucz 'selected_tweets' w odpowiedzi JSON nie zawiera listy.")

    except Exception as e:
        print(f"❌ Błąd podczas komunikacji z API OpenAI: {e}")
        return []


def main():
    """Główna funkcja wykonująca cały proces."""
    try:
        # Otwórz arkusz źródłowy i pobierz wszystkie dane
        source_sheet = client.open(SOURCE_SHEET_NAME).sheet1
        all_data = source_sheet.get_all_records(expected_headers=EXPECTED_HEADERS)
        print(f"📄 Pobrano {len(all_data)} wierszy z arkusza '{SOURCE_SHEET_NAME}'.")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"BŁĄD: Nie znaleziono arkusza o nazwie '{SOURCE_SHEET_NAME}'. Sprawdź, czy nazwa jest poprawna.")
        return
    except Exception as e:
        print(f"BŁĄD: Wystąpił nieoczekiwany problem podczas otwierania arkusza źródłowego: {e}")
        return

    if len(all_data) > 0:
        # Krok 1: Wybierz najlepsze tweety za pomocą AI
        selected_tweet_texts = get_best_tweets_from_ai(all_data)

        if not selected_tweet_texts:
            print("❌ Anulowano operację z powodu braku odpowiedzi od AI.")
            return

        # Krok 2: Znajdź pełne dane dla wybranych tweetów
        text_to_row_map = {row['Text']: row for row in all_data}

        final_rows_to_publish = []
        for text in selected_tweet_texts:
            if text in text_to_row_map:
                final_rows_to_publish.append(text_to_row_map[text])
            else:
                print(f"⚠️ Ostrzeżenie: Nie można znaleźć oryginalnego wiersza dla tweeta: '{text[:50]}...'")

        if not final_rows_to_publish:
            print("❌ Nie znaleziono żadnych pasujących wierszy. Przerwanie zapisu.")
            return

        # Krok 3: Zapisz wybrane tweety do nowego arkusza
        try:
            spreadsheet = client.open(PUBLISH_SPREADSHEET_NAME)
            try:
                publish_sheet = spreadsheet.worksheet(RESULTS_WORKSHEET_NAME)
                print(f"📝 Otworzono istniejący arkusz '{RESULTS_WORKSHEET_NAME}'.")
            except gspread.exceptions.WorksheetNotFound:
                publish_sheet = spreadsheet.add_worksheet(title=RESULTS_WORKSHEET_NAME, rows="100", cols="20")
                print(f"✨ Utworzono nowy arkusz '{RESULTS_WORKSHEET_NAME}'.")

            publish_sheet.clear()
            print("🧹 Wyczyszczono arkusz docelowy.")

            if final_rows_to_publish:
                # Przygotowanie danych do zapisu bez dodawania nagłówków
                # Kolejność jest zachowana dzięki `expected_headers`
                rows_as_lists = [list(row.values()) for row in final_rows_to_publish]

                publish_sheet.append_rows(rows_as_lists, value_input_option='USER_ENTERED')

                print(
                    f"✔️ Dane ({len(final_rows_to_publish)} wierszy) zostały pomyślnie zapisane do arkusza '{RESULTS_WORKSHEET_NAME}'.")

        except gspread.exceptions.SpreadsheetNotFound:
            print(f"BŁĄD: Arkusz o nazwie '{PUBLISH_SPREADSHEET_NAME}' nie istnieje i nie można go utworzyć.")
        except Exception as e:
            print(f"❌ Wystąpił nieoczekiwany błąd podczas zapisu do Google Sheets: {e}")

    else:
        print("✔️ Arkusz źródłowy '{SOURCE_SHEET_NAME}' jest pusty. Nie wykonano żadnych operacji.")


if __name__ == "__main__":
    main()
