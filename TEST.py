import os
import json
import gspread

# --- Konfiguracja testu ---
NAZWA_PLIKU_DO_SPRAWDZENIA = "dupa"


# -------------------------

def autoryzuj_google_sheets():
    """Skopiowana funkcja autoryzacji."""
    print("--- Rozpoczynam autoryzację...")
    try:
        google_creds_json_str = os.getenv("GCP_SA_KEY")
        if not google_creds_json_str:
            print("❌ BŁĄD: Nie znaleziono zmiennej środowiskowej GCP_SA_KEY.")
            return None
        google_creds_dict = json.loads(google_creds_json_str)
        gc = gspread.service_account_from_dict(google_creds_dict)
        print("✅ Pomyślnie autoryzowano.")
        return gc
    except Exception as e:
        print(f"❌ KRYTYCZNY BŁĄD podczas autoryzacji: {e}")
        return None


def sprawdz_dostep():
    """Główna funkcja testowa."""
    gc = autoryzuj_google_sheets()
    if not gc:
        print("--- Test przerwany z powodu błędu autoryzacji.")
        return

    print(f"\n--- Próbuję otworzyć arkusz o nazwie: '{NAZWA_PLIKU_DO_SPRAWDZENIA}'...")
    try:
        arkusz = gc.open(NAZWA_PLIKU_DO_SPRAWDZENIA)
        print("\n" + "=" * 50)
        print(f"✅✅✅ SUKCES! Udało się otworzyć arkusz '{arkusz.title}'.")
        print(f"   ID Arkusza: {arkusz.id}")
        print(f"   Link: {arkusz.url}")
        print("=" * 50)
        print("\nWNIOSEK: Dostęp jest poprawny. Problem musi leżeć w logice głównego skryptu.")

    except gspread.exceptions.SpreadsheetNotFound:
        print("\n" + "=" * 50)
        print(f"❌❌❌ BŁĄD: Nie znaleziono arkusza o nazwie '{NAZWA_PLIKU_DO_SPRAWDZENIA}'.")
        print("=" * 50)
        print("\nWNIOSEK: Problem na 100% leży po stronie Google Drive. Sprawdź jeszcze raz:")
        print("1. DOKŁADNĄ nazwę pliku (czy nie ma literówek, spacji, itp.).")
        print("2. CZY NA PEWNO udostępniłeś plik WŁAŚCIWEMU kontu serwisowemu.")
        print("3. CZY NA PEWNO nadałeś uprawnienia 'Edytor'.")

    except Exception as e:
        print(f"\n❌ Niespodziewany błąd: {e}")


if __name__ == "__main__":
    sprawdz_dostep()