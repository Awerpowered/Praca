import os
import re
import json
import time
import gspread
import pandas as pd
from openai import OpenAI

# --- GŁÓWNA KONFIGURACJA ---
NAZWA_ARKUSZA_GOOGLE = "Twitter_LLM"
NAZWA_ARKUSZA_WYNIKOWEGO = "test1"

# Nazwy kolumn w arkuszu źródłowym
NAZWA_KOLUMNY_Z_TEKSTEM = "Text"
NAZWA_KOLUMNY_Z_LINKIEM = "Tweet_link"

LICZBA_TWEETOW_DO_WYBORU = 5

# Konfiguracja zakładki przechowującej stan skryptu
NAZWA_ARKUSZA_STANU = "_script_state"
KOMORKA_STANU = "B1"


# --------------------

def autoryzuj_google_sheets():
    """
    Autoryzuje dostęp do Google Sheets przy użyciu danych logowania
    przechowywanych w zmiennej środowiskowej (sekret GitHub).
    """
    print("Attempting to authorize with Google Sheets...")
    try:
        google_creds_json_str = os.getenv("GCP_SA_KEY")
        if not google_creds_json_str:
            print("❌ ERROR: Environment variable GCP_SA_KEY not found.")
            return None

        google_creds_dict = json.loads(google_creds_json_str)
        gc = gspread.service_account_from_dict(google_creds_dict)
        print("✅ Successfully authorized with Google Sheets.")
        return gc
    except Exception as e:
        print(f"❌ ERROR during Google authorization: {e}")
        return None


def pobierz_stan(arkusz_glowny):
    """
    Pobiera numer ostatniego przetworzonego wiersza z zakładki _script_state.
    Jeśli zakładka nie istnieje, tworzy ją i zwraca 0.
    """
    try:
        zakladka_stanu = arkusz_glowny.worksheet(NAZWA_ARKUSZA_STANU)
        ostatni_wiersz = zakladka_stanu.acell(KOMORKA_STANU).value
        return int(ostatni_wiersz) if ostatni_wiersz and ostatni_wiersz.isdigit() else 0
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{NAZWA_ARKUSZA_STANU}' not found. Creating it...")
        zakladka_stanu = arkusz_glowny.add_worksheet(title=NAZWA_ARKUSZA_STANU, rows="10", cols="10")

        zakladka_stanu.update(range_name='A1', values=[['last_processed_row_index']])
        zakladka_stanu.update(range_name=KOMORKA_STANU, values=[['0']])

        print(f"✅ Created and initialized worksheet '{NAZWA_ARKUSZA_STANU}'.")
        return 0
    except Exception as e:
        print(f"Error getting state: {e}. Defaulting to 0.")
        return 0


def aktualizuj_stan(arkusz_glowny, nowy_indeks_wiersza):
    """Aktualizuje numer ostatniego przetworzonego wiersza w zakładce _script_state."""
    try:
        zakladka_stanu = arkusz_glowny.worksheet(NAZWA_ARKUSZA_STANU)
        zakladka_stanu.update(range_name=KOMORKA_STANU, values=[[str(nowy_indeks_wiersza)]])
        print(f"✅ State updated. Last processed row is now: {nowy_indeks_wiersza}")
    except Exception as e:
        print(f"❌ ERROR updating state: {e}")


def dopisz_dane_do_arkusza(gc, nazwa_arkusza_matki, nazwa_zakladki_wynikowej, dataframe_wynikow):
    """
    Zapisuje dane do arkusza wynikowego metodą "odczytaj-wszystko, połącz, wyczyść, zapisz-wszystko",
    aby zapewnić maksymalną niezawodność.
    """
    if dataframe_wynikow.empty:
        print("ℹ️ No new results to write. Skipping.")
        return True  # Zwróć sukces, bo nic nie trzeba było robić

    try:
        print(f"--- Starting robust write process for worksheet '{nazwa_zakladki_wynikowej}' ---")
        arkusz_google = gc.open(nazwa_arkusza_matki)
        try:
            zakladka = arkusz_google.worksheet(nazwa_zakladki_wynikowej)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{nazwa_zakladki_wynikowej}' not found, creating it...")
            zakladka = arkusz_google.add_worksheet(title=nazwa_zakladki_wynikowej, rows="100", cols="20")

        # 1. Odczytaj wszystkie istniejące dane
        print("Reading all existing data from the target sheet...")
        istniejace_dane = zakladka.get_all_values()
        print(f"Found {len(istniejace_dane)} existing rows.")

        # 2. Przygotuj nowe dane i połącz je z istniejącymi
        nowe_wiersze = dataframe_wynikow.values.tolist()

        if not istniejace_dane:  # Jeśli arkusz jest pusty, dodaj nagłówki
            print("Target sheet is empty. Adding headers.")
            finalna_lista_danych = [dataframe_wynikow.columns.tolist()] + nowe_wiersze
        else:
            finalna_lista_danych = istniejace_dane + nowe_wiersze

        # 3. Wyczyść cały arkusz
        print(f"Clearing the entire worksheet '{nazwa_zakladki_wynikowej}'...")
        zakladka.clear()
        time.sleep(2)  # Krótka pauza po czyszczeniu, aby API Google nadążyło

        # 4. Zapisz połączone dane z powrotem
        print(f"Writing {len(finalna_lista_danych)} total rows back to the sheet...")
        zakladka.update(range_name='A1', values=finalna_lista_danych, value_input_option='USER_ENTERED')

        print(f"✅ Robust write operation completed for '{nazwa_zakladki_wynikowej}'.")
        return True  # Zwróć sukces

    except Exception as e:
        print(f"❌ CRITICAL ERROR during robust write operation: {e}")
        return False  # Zwróć błąd


def analizuj_tweety_z_openai(lista_tweetow, liczba_do_wyboru):
    """Wysyła ponumerowane tweety do AI i prosi o zwrot numerów najlepszych z nich."""
    try:
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            print("❌ ERROR: Environment variable OPENAI_API_KEY not found.")
            return None

        client = OpenAI(api_key=openai_api_key)
        sformatowane_tweety = "\n\n".join([f"Tweet numer {i + 1}:\n{tweet}" for i, tweet in enumerate(lista_tweetow)])

        prompt_systemowy = "Jesteś ekspertem od marketingu i mediów społecznościowych. Twoim zadaniem jest analiza tweetów pod kątem ich wartości i potencjału."
        prompt_uzytkownika = (
            f"Przeanalizuj poniższą listę {len(lista_tweetow)} tweetów, z których każdy ma przypisany 'Tweet numer X'.\n\n"
            f"Twoim zadaniem jest wybrać {liczba_do_wyboru} z nich, które są najbardziej wartościowe.\n\n"
            "Zwróć TYLKO I WYŁĄCZNIE numery porządkowe tych wybranych tweetów, oddzielone przecinkami. "
            "Nie dodawaj żadnego innego tekstu. Oczekiwany format odpowiedzi to na przykład: 4, 8, 1, 9, 2"
            f"\n\n--- TWEETY DO ANALIZY ---\n{sformatowane_tweety}"
        )

        print(f"\n🤖 Sending {len(lista_tweetow)} tweets for analysis by OpenAI...")
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": prompt_systemowy}, {"role": "user", "content": prompt_uzytkownika}],
            temperature=0.1,
        )
        wynik_tekstowy = response.choices[0].message.content
        print(f"🤖 OpenAI response received: '{wynik_tekstowy}'")
        numery = re.findall(r'\d+', wynik_tekstowy)
        indeksy = [int(n) for n in numery]
        return indeksy if indeksy else None
    except Exception as e:
        print(f"❌ ERROR during OpenAI communication: {e}")
        return None


def main():
    """Główna funkcja skryptu."""
    print("🚀 Starting daily tweet analysis script.")
    gc = autoryzuj_google_sheets()
    if not gc:
        return

    try:
        arkusz_glowny = gc.open(NAZWA_ARKUSZA_GOOGLE)
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Could not open main Google Sheet '{NAZWA_ARKUSZA_GOOGLE}'. Error: {e}")
        return

    zakladka_danych = arkusz_glowny.sheet1
    ostatni_przetworzony_wiersz = pobierz_stan(arkusz_glowny)
    print(f"ℹ️ Last processed row index from state: {ostatni_przetworzony_wiersz}")

    print("Fetching all values to handle potentially bad headers...")
    wszystkie_wartosci = zakladka_danych.get_all_values()
    if not wszystkie_wartosci or len(wszystkie_wartosci) < 2:
        print("✅ Worksheet is empty or contains only a header. No records to process. Exiting.")
        return

    naglowki_oryginalne = wszystkie_wartosci[0]
    dane_wiersze = wszystkie_wartosci[1:]

    naglowki_naprawione = []
    uzyte_nazwy = {}
    for naglowek in naglowki_oryginalne:
        oryginalna_nazwa = naglowek if naglowek else 'pusta_kolumna'
        if oryginalna_nazwa in uzyte_nazwy:
            uzyte_nazwy[oryginalna_nazwa] += 1
            nowa_nazwa = f"{oryginalna_nazwa}_{uzyte_nazwy[oryginalna_nazwa]}"
        else:
            uzyte_nazwy[oryginalna_nazwa] = 0
            nowa_nazwa = oryginalna_nazwa
        naglowki_naprawione.append(nowa_nazwa)

    df = pd.DataFrame(dane_wiersze, columns=naglowki_naprawione)
    df.dropna(how='all', inplace=True)
    print(f"Successfully created DataFrame with {len(df)} rows.")

    aktualna_liczba_wierszy = len(df)

    if aktualna_liczba_wierszy <= ostatni_przetworzony_wiersz:
        print("✅ No new records to process. Exiting.")
        return

    print(f"Found {aktualna_liczba_wierszy - ostatni_przetworzony_wiersz} new records to process.")
    nowe_rekordy_df = df.iloc[ostatni_przetworzony_wiersz:aktualna_liczba_wierszy].reset_index(drop=True)

    for kolumna in [NAZWA_KOLUMNY_Z_TEKSTEM, NAZWA_KOLUMNY_Z_LINKIEM]:
        if kolumna not in nowe_rekordy_df.columns:
            print(f"❌ CRITICAL ERROR: Missing column '{kolumna}' in source data.")
            print(f"Available columns: {nowe_rekordy_df.columns.tolist()}")
            return

    tweety_do_analizy = nowe_rekordy_df[NAZWA_KOLUMNY_Z_TEKSTEM].tolist()
    wybrane_indeksy = analizuj_tweety_z_openai(tweety_do_analizy, LICZBA_TWEETOW_DO_WYBORU)

    if not wybrane_indeksy:
        print("🔴 AI analysis did not return valid results. State will not be updated.")
        return

    print(f"AI selected tweets with relative numbers: {wybrane_indeksy}")
    indeksy_df = [i - 1 for i in wybrane_indeksy if (i - 1) < len(nowe_rekordy_df)]

    finalne_rekordy_df = nowe_rekordy_df.iloc[indeksy_df]
    wyniki_df = finalne_rekordy_df[[NAZWA_KOLUMNY_Z_TEKSTEM, NAZWA_KOLUMNY_Z_LINKIEM]]

    # Zapisz dane i sprawdź, czy operacja się powiodła
    sukces_zapisu = dopisz_dane_do_arkusza(gc, NAZWA_ARKUSZA_GOOGLE, NAZWA_ARKUSZA_WYNIKOWEGO, wyniki_df)

    # Zaktualizuj stan tylko jeśli zapis się udał
    if sukces_zapisu:
        aktualizuj_stan(arkusz_glowny, aktualna_liczba_wierszy)
        print("\n🎉 All operations completed successfully!")
    else:
        print("\n🔴 Write operation failed. State was not updated. Please check the logs.")


if __name__ == "__main__":
    main()
