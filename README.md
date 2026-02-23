# PlayUA Desktop Client (PySide6)

Нативний кросплатформовий desktop-клієнт для Backblaze B2 на `PySide6 (Qt)`.

## Можливості
- Авторизація через `Application Key ID` та `Application Key`
- Завантаження в bucket: один файл, багато файлів, або ціла папка
- Візуальний прогрес завантаження:
  - відсоток (progress bar)
  - скільки вже завантажено (`MB/GB`)
  - скільки залишилось (`MB/GB`)
- Перегляд списку файлів
- Public direct link: copy/open
- Private direct link (тимчасовий): copy/open через `b2_get_download_authorization`
- Збереження налаштувань локально

## Вимоги
- Python 3.9+


## Налаштування
Settings файл:
- macOS: `~/Library/Application Support/BackblazeB2Client/settings.json`
- Linux: `~/.config/BackblazeB2Client/settings.json`
- Windows: `%APPDATA%\\BackblazeB2Client\\settings.json`

## Збірка
```bash
python -m pip install -r requirements-build.txt
PYTHON_BIN="$(pwd)/.venv/bin/python" bash /Users/Erleke/Documents/dev/rozetka/backblaze/scripts/build.sh
```

Windows:
```powershell
python -m pip install -r requirements-build.txt
powershell -ExecutionPolicy Bypass -File .\\scripts\\build.ps1
```

Результат: `dist/`.
