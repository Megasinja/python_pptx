# Python研修プロトタイプ

左側に Python 実行環境、右側に資料配置用 Canvas を持つ最小 Web プロトタイプです。  
WebSocket で実行ログを逐次ストリーミングし、停止操作にも対応しています。

## 構成
- 左ペイン
  - Python コード入力（Monaco Editor / VS Code 風）
  - 実行 / 停止 / ログクリア
  - stdout / stderr のリアルタイム表示
  - File > ファイルを開く: `.py`（エディタ読込）/ `.json`（Canvas読込）
  - File > フォルダを開く: フォルダ内の `.py` と `.json` を自動読込
- 右ペイン
  - 画像ファイルを Canvas に配置（ドラッグ&ドロップ対応）
  - 画像/テキストの移動・リサイズ
  - ペン描画（ブラシサイズ変更）
  - レーザーポインタ表示
  - ズームイン / ズームアウト / 100% リセット
  - Canvas内容の JSON 保存 / JSON 復元

## セットアップ
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 起動
```bash
python -m uvicorn app.main:app --reload
```

ブラウザで `http://127.0.0.1:8000` を開いてください。

`app/static/index.html` をファイルとして直接開く（`file://...`）と WebSocket 接続できません。  
必ず `python -m uvicorn` で起動したサーバー経由で開いてください。

## 注意事項
- これは研修向けの最小プロトタイプです。
- 実行タイムアウトは 10 秒です（`app/main.py` の `EXEC_TIMEOUT_SECONDS`）。
- Monaco Editor は CDN から読み込んでいます。オフライン環境では自動で簡易エディタにフォールバックします。
- Canvas 拡張機能は Fabric.js を CDN から読み込みます。
- 本番利用では、コンテナ隔離・リソース制限・監査ログを追加してください。
