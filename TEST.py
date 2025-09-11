import os
import subprocess
from flask import Flask, request, render_template, send_from_directory, make_response
from ess import translate_markdown_file

UPLOAD_FOLDER = 'uploads'
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@app.route('/')
def index():
    languages = {
        "pl": "Polski", "en": "Angielski", "de": "Niemiecki",
        "es": "Hiszpański", "fr": "Francuski", "it": "Włoski", "uk": "Ukraiński"
    }
    return render_template('index.html', languages=languages)


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files: return "Nie znaleziono pliku"
    file = request.files['file']
    if file.filename == '': return "Nie wybrano pliku"

    if file:
        original_filename = file.filename

        if not original_filename.lower().endswith('.md'):
            return "Błąd: Ta funkcja jest zoptymalizowana i działa obecnie tylko dla plików w formacie Markdown (.md)."

        original_path = os.path.join(app.config['UPLOAD_FOLDER'], original_filename)
        file.save(original_path)

        target_language_code = request.form['language']
        target_language_name = request.form['language_name']
        output_format = request.form['format']

        try:
            translated_md_content = translate_markdown_file(original_path, target_language_name)
        except Exception as e:
            return f"Wystąpił błąd podczas procesu tłumaczenia: {e}"

        translated_filename_base = f"{os.path.splitext(original_filename)[0]}_translated_{target_language_code}"
        translated_md_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{translated_filename_base}.md")

        with open(translated_md_path, 'w', encoding='utf-8') as f:
            f.write(translated_md_content)

        if output_format == 'md':
            response = make_response(
                send_from_directory(app.config['UPLOAD_FOLDER'], f"{translated_filename_base}.md",
                                    as_attachment=True))
            response.set_cookie('fileDownloaded', 'true', max_age=20)
            return response

        if output_format == 'html':
            output_filename = f"{translated_filename_base}.html"
            output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)

            try:
                subprocess.run(
                    [
                        '/opt/homebrew/bin/pandoc',
                        translated_md_path,
                        '-o', output_path,
                        '--standalone',
                        '--css', 'custom_style.css',
                        '--self-contained',
                    ],
                    check=True
                )

                response = make_response(
                    send_from_directory(app.config['UPLOAD_FOLDER'], output_filename, as_attachment=True))
                response.set_cookie('fileDownloaded', 'true', max_age=20)
                return response
            except Exception as e:
                return f"Błąd podczas konwersji do HTML za pomocą Pandoc: {e}."

    return "Coś poszło nie tak"


if __name__ == '__main__':
    app.run(debug=True)
