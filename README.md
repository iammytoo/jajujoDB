# jajujoDB
ja(**ja**va)ju(**ju**業)jo(**jo**報理工)DBです。

早稲田大学情報理工学実験B用のToyDBです。
余裕があったらindexとかやりたいけど多分間に合いません。

とりあえず
select, where, join, upsert, createtableを実装しています。

join -> where -> selectの順でクエリを構築するといい感じです。
テーブル内のカラム名は他テーブルと被らない方がいいです。多分
()とか使わずに、space区切りで粛々と`予約語 名`の文法を貫いてほしいです。
