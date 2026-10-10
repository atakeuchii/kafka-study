# Day 0：Day 1 以降に向けた準備

## 1. 環境

| 項目 | 内容 |
|---|---|
| ブローカー | `apache/kafka:4.3.1`、Docker Compose で1台、KRaft モード（broker と controller を兼ねる） |
| デフォルトパーティション数 | 3 |
| データディレクトリ | `/var/lib/kafka/data`（コンテナ内） |
| プロジェクト | `kafka-study`（Leiningen） |
| クライアント | `org.apache.kafka/kafka-clients 4.3.1`、ログは `slf4j-simple`（warn 以上） |

起動と確認：

```bash
docker compose up -d
docker compose logs kafka | grep "Kafka Server started"
```

## 2. CLI

### エイリアス

```bash
alias kt='docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092'
alias kp='docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092'
alias kc='docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092'
alias kg='docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092'
```

### よく使うコマンド

| やりたいこと | コマンド |
|---|---|
| トピック作成 | `kt --create --topic <名前> --partitions 3 --replication-factor 1` |
| トピック一覧・詳細 | `kt --list` / `kt --describe --topic <名前>` |
| キー付きで送る | `kp --topic <名前> --property parse.key=true --property key.separator=:` |
| 先頭から読む（パーティション・offset表示） | `kc --topic <名前> --from-beginning --property print.key=true --property print.partition=true --property print.offset=true` |
| グループを指定して読む | `kc --topic <名前> --group <グループ>` |
| グループの状態（offset・lag・担当） | `kg --describe --group <グループ>` |
| セグメントの中身を見る | `docker exec -it kafka /opt/kafka/bin/kafka-dump-log.sh --files <.logのパス> --print-data-log` |

### `kg --describe` の見方

| 列 | 意味 |
|---|---|
| CURRENT-OFFSET | グループが commit 済みの位置（次に読む番号） |
| LOG-END-OFFSET | パーティションの末尾（次に書かれる番号） |
| LAG | 未読の件数 |
| CONSUMER-ID | そのパーティションを今担当している consumer |

## 3. Clojure の最小構成

`src/kafka_study/hello.clj` に以下を用意した。

- `make-producer`：文字列キー・文字列値の producer を作る
- `send!`：1件送る（戻り値は Future。deref で RecordMetadata）
- `make-consumer`：グループを指定し、`auto.offset.reset=earliest` の consumer を作る
- `poll-once`：最大2秒待って、`{:partition :offset :key :value}` のベクタを返す

注意点：

- `auto.offset.reset` の Java のデフォルトは `latest` なので、先頭から読みたいときは明示する。
- `KafkaConsumer` はスレッドセーフではない。1 consumer＝1スレッドで使う。
- consumer はデフォルトで自動 commit するので、同じグループで読み直しても既読分は返らない。

## 4. 押さえた基本モデル（Day 1 以降の前提）

- 同じキーは同じパーティションに入る。順序はパーティション内でのみ保証される。
- キーなしの場合は、バッチ1つ分ずつパーティションを切り替えて散らす。
- メッセージは読まれても消えない。retention でセグメント単位に消える（retention は下限の保証）。
- 各グループはパーティションごとの commit 済み offset だけを持つ。違う処理は別グループ、同じ処理の分担は同じグループにする。
- グループ内では1パーティションを1 consumer だけが担当する。consumer が減るとリバランスで引き継がれ、commit 前の分は重複しうる。
