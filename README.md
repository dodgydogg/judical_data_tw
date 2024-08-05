# 司法判決資料庫

司法院雖然有公開自2013年起，所有判決的各種編碼資料，但上傳資料的格式複雜難用；而且每個案件情況不同（如被告人數、起訴案由數量），資料的長短也不一，如下：

```
0!臺東地方法院!106!原交訴!9!肇事遺棄!xxx!!!!!!!110!07!08!其他!!!!!!0!0!0!0!!TTDM,106,原交訴,9,20210708,5!
1!110!07!08!!0!0!0!0!0!0!0!0!0!!0!0!0!0!!0!0!0!0!0!0!0!0!!!0!0!0!0!!0!!0!0!0!0!0!0!0!0!0!0!0!0!0!0!0!0!!!!!!!!!!!!!!0!0!!!!
1!107!05!07!!0!0!0!0!0!0!0!0!0!!0!0!0!0!!0!0!0!0!0!0!0!0!!!0!0!0!0!!0!!0!0!0!0!0!0!0!0!0!0!0!0!0!0!0!0!!!!!!!!!!!!!!0!0!!!!
#
0!臺東地方法院!109!原金訴!34!違反洗錢防制法等!xxx!xxx!xxx!!!!!110!07!30!判決!1.00!!!!!0!0!0!0!!TTDM,109,原金訴,34,20210730,1!
1!110!07!30!公設辯護人辯護!0!0!0!0!0!0!0!0!0!!0!0!0!0!!0!0!0!0!0!0!0!0!!!1!0!0!1!8000!0!!0!0!0!0!0!0!0!0!0!0!0!0!0!0!0!0!!!!!!!!!!!!!!0!0!!!!
1.1!刑法!339!!!1!!!10306!有期徒刑!0003!!!1!0!0!1!0!0!
1.1.2!幫助犯!
#
```

所以我試圖把這些資料整理成SQL資料庫，讓資料保持應有的結構。不過因為不同層級法院、民事、刑事、行政訴訟等等，案件的資料編碼格式與欄位都不同，處裡起來工程浩大，所以目前僅完成了地方法院刑事訴訟案件。
有需要參照司法院的[說明](https://opendata.judicial.gov.tw/dataset/detail?datasetId=29886)，來修改資料庫架構跟程式碼。




## 用法

司法院提供的txt檔是big5編碼，建議先透過encoding.ipynb將資料轉成utf-8編碼。
link_db_json.ipynb可用來將個別案件資料連上判決書全文，目前雲端上傳的的地院刑事案件資料庫已經連結完判決全文（所以檔案很龐大）。

因為完成的db檔案超出github的上傳上限，所以請從我個人的[google雲端硬碟](https://drive.google.com/file/d/1wcARfqmAPXYOcuY7RqqO8l67VHGzw98b/view?usp=sharing)下載。
[地方法院](https://drive.google.com/file/d/1wcARfqmAPXYOcuY7RqqO8l67VHGzw98b/view?usp=sharing)
[高等法院](https://drive.google.com/file/d/12i_6473geYn_dCMXcOVZWg_Kqtz3PGMD/view?usp=sharing)
而如果要自己重build資料庫，請從[司法院資料開放平臺](https://opendata.judicial.gov.tw/)下載「司法院及所屬各級法院之終結案件資料」，並將資料放在raw_txt資料夾中。


## 參考

- [司法院裁判書開放API 規格說明（下載PDF）](https://www.google.com/url?sa=i&url=https%3A%2F%2Fopendata.judicial.gov.tw%2Fapi%2FNewses%2F37%2Ffile&psig=AOvVaw28ogVlgknhCODOmrHucYf0&ust=1720084777638000&source=images&cd=vfe&opi=89978449&ved=0CAcQr5oMahcKEwjgzLKixYqHAxUAAAAAHQAAAAAQBA)