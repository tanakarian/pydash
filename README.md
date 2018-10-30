# pydash
Python Client for Redash API

# Usage
## 開発環境、ローカルでいじるとき
```bash
git clone https://github.com/eure/pydash
# pipenvなければ
pip install pipenv

pipenv install
pipenv update

# .envに以下記載
PYTHONPATH=/path/to/eure/pydash
USER=
PASSWORD=

pipenv shell

# あとはお好きなように
```

## ライブラリとして使うとき
```bash
pip install -U git+https://github.com/tanakarian/pydash 
```
