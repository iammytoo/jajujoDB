# jajujoDB
ja(**ja**va)ju(**ju**業)jo(**jo**報理工)DBです。

早稲田大学情報理工学実験B用のToyDBです。
余裕があったらindexとかやりたいけど多分間に合いません。

とりあえず
select, where, join, upsert, createtableを実装しています。

join -> where -> selectの順でクエリを構築するといい感じです。
テーブル内のカラム名は他テーブルと被らない方がいいです。多分
()とか使わずに、space区切りで粛々と`予約語 名`の文法を貫いてほしいです。

文法
`createtable`
```
createtable {table名} {column数} ({column名},{column型(string or int)}) ({column名},{column型(string or int)}) ({column名},{column型(string or int)})...
```

`upsert`
渡したカラムとデータの順序は関係あります。
```
upsert {table名} ({column名},{column名},{column名}...) ({データ},{データ},{データ}...)
```

参照系
```
from {table名}
join {結合するtable名} ({結合するcolumn名}, {結合されるcolumn名})
where ({対象column名}, {演算子(==,>,<,>=,<=)}, {比較value})
select ({対象column名},{対象column名},{対象column名}...)
```
